# Architectural Scaling Review Log

## Session 1: O(N × K) Full Catalog Scans in `validate_resource_limits` (2026-03-08)

### Location
`src/adapter/src/coord/ddl.rs:1033-1473` — `validate_resource_limits()`

### Problem

Every catalog transaction (DDL operation) calls `validate_resource_limits()` which performs **10+ full scans of the entire `entry_by_id` map** to count objects by type. Each `user_*()` method (e.g., `user_tables()`, `user_sources()`, `user_sinks()`, etc.) iterates over **every catalog entry** and filters by type.

The scanning chain per DDL:
1. `user_connections()` → scans all entries, filters by `is_connection() && is_user()` (lines 1285-1301)
2. `user_tables().count()` → scans all entries (line 1338)
3. `user_sources()` → scans all entries + `.filter_map(source()).map(persist_shard_count).sum()` (lines 1345-1352)
4. `user_sinks().count()` → scans all entries (line 1362)
5. `user_materialized_views().count()` → scans all entries (line 1369)
6. `user_clusters().count()` → scans all clusters (line 1381)
7. `current_credit_consumption_rate()` → scans all cluster replicas (line 1402)
8. `databases().count()` → scans all databases (line 1414)
9. `user_secrets().count()` → scans all entries (line 1446)
10. `user_continual_tasks().count()` → scans all entries (line 1460)
11. `user_continual_tasks().count()` → scans all entries AGAIN (line 1467, **also a bug** — see below)
12. `user_roles().count()` → scans all roles (line 1453)

Each `user_*()` call goes through `self.entries()` → `self.state.entry_by_id.values()` (catalog.rs:1183-1184), filtering the entire imbl::OrdMap.

**Total cost per DDL: O(N × K)** where N = total catalog entries and K ≈ 10 scan passes.

### Bug Found

**Line 1466-1472**: The network policy limit validation incorrectly uses `user_continual_tasks().count()` as the current count instead of counting network policies:

```rust
self.validate_resource_limit(
    self.catalog().user_continual_tasks().count(),  // BUG: should count network policies
    new_network_policies,
    SystemVars::max_network_policies,
    "network_policy",
    MAX_NETWORK_POLICIES.name(),
)?;
```

This means the network policy limit is enforced against the number of continual tasks, not network policies. If a user has 0 continual tasks but 100 network policies, this check would pass even if the limit is 10.

### Scaling Impact

- With 1,000 catalog objects, every `CREATE TABLE` does ~10,000 comparisons just for limit checking
- With 10,000 objects (common in production with subsources), that's ~100,000 comparisons per DDL
- This runs on the **single-threaded coordinator**, blocking all other operations
- The work is entirely redundant — object counts change only on DDL, yet we recount from scratch every time

### Root Cause

The `CatalogState` maintains only a single `entry_by_id: OrdMap<CatalogItemId, CatalogEntry>` map. There are no maintained counters or secondary indexes by object type. Every type-specific query must do a full scan + filter.

### Suggested Fix

**Option A (simple)**: Maintain per-type counters in `CatalogState` (e.g., `user_table_count: usize`, `user_source_count: usize`, etc.) that are incremented/decremented in `apply_updates()`. The `validate_resource_limits` function can then do O(1) lookups instead of O(N) scans. This is low-risk because the counters are purely derived state.

**Option B (more general)**: Add secondary indexes to `CatalogState` like `user_entries_by_type: BTreeMap<CatalogItemType, BTreeSet<CatalogItemId>>`. This would also benefit other code that needs to enumerate objects by type.

**Bug fix**: Change line 1467 from `user_continual_tasks().count()` to the correct network policy count (needs a `user_network_policies()` method or equivalent).

### Files Involved
- `src/adapter/src/coord/ddl.rs` — `validate_resource_limits()` (lines 1033-1473), `catalog_transact_inner()` (line 437 calls it)
- `src/adapter/src/catalog.rs` — `entries()` (line 1183), all `user_*()` methods (lines 1187-1276)
- `src/adapter/src/catalog/state.rs` — `CatalogState` struct (lines 118-171), where counters/indexes would be added
- `src/adapter/src/catalog/apply.rs` — `apply_updates()`, where counters/indexes would be maintained

## Session 2: Uncached Recursive Timeline Context Resolution — O(N × D) on Hot Paths (2026-03-08)

### Location
`src/adapter/src/catalog/timeline.rs:70-109` — `ids_in_timeline()`
`src/adapter/src/catalog/timeline.rs:167-230` — `get_timeline_contexts()`
`src/adapter/src/coord/timeline.rs:299-338` — `advance_timelines()`
`src/adapter/src/coord/timeline.rs:363-468` — `timedomain_for()`

### Problem

Timeline context resolution performs **redundant recursive dependency walks** with **no caching or memoization**, and this happens on two critical paths:

**Path 1 — `ids_in_timeline()` (periodic, every ~1s for non-EpochMilliseconds timelines):**

Called from `advance_timelines()` (line 315), this function:
1. Iterates ALL catalog entries (line 72: `for entry in self.entries()`)
2. For EACH entry, calls `get_timeline_context(entry.id())` (line 74)
3. Which calls `validate_timeline_context(entry.global_ids())` (line 28)
4. Which calls `get_timeline_contexts(ids)` (line 127)
5. Which performs a **recursive BFS through ALL transitive dependencies** (lines 177-230):

```rust
// get_timeline_contexts — recursive dependency walk per item
let mut ids: Vec<_> = ids.into_iter().collect();
while let Some(id) = ids.pop() {
    if !seen.insert(id) { continue; }
    match entry.item() {
        CatalogItem::View(View { optimized_expr, .. }) => {
            ids.extend(optimized_expr.depends_on()...);  // recurse into deps
        }
        CatalogItem::MaterializedView(mv) => {
            ids.extend(mv.optimized_expr.depends_on()...);  // recurse into deps
        }
        CatalogItem::Index(index) => {
            ids.push(index.on);  // recurse into indexed object
        }
        // ...
    }
}
```

**Total cost: O(N × D)** where N = total catalog entries and D = average transitive dependency depth. With 10,000 entries and average depth 5, this is **50,000 operations every ~1 second** per non-EpochMilliseconds timeline.

**Path 2 — `timedomain_for()` (per-query):**

Called at the start of every transaction, this function:
1. Collects ALL item IDs from ALL schemas touched by the query (lines 407-418)
2. If any system schema is touched, ALL system schemas are included (lines 396-404)
3. Resolves to `CollectionIdBundle` via `sufficient_collections()` (line 434)
4. Then for EACH ID in the bundle, calls `validate_timeline_context(vec![*gid])` inside a `.retain()` loop (line 441-444):

```rust
ids.retain(|gid| {
    let id_timeline_context = catalog
        .validate_timeline_context(vec![*gid])  // recursive walk PER ID
        .expect("single id should never fail");
    // ... timeline comparison ...
});
```

Each `validate_timeline_context` call triggers an independent recursive dependency walk. If the bundle has S IDs and average dependency depth D, **total cost per query is O(S × D)**. A query touching system schemas easily has S = 500+ IDs.

### Why This Is Redundant

The dependency walks for different catalog entries traverse the **same subgraph repeatedly**. For example:
- View A depends on Table T1, Table T2
- View B depends on Table T1, Table T3
- View C depends on View A, View B

Resolving the timeline context for C walks: C → A → T1, T2 and C → B → T1, T3.
But resolving A independently ALSO walks: A → T1, T2.
And resolving B independently ALSO walks: B → T1, T3.

Table T1's timeline context is computed **3 separate times** in this example. In a real catalog with deep view hierarchies, the same base tables are visited thousands of times.

### Scaling Impact

- **`advance_timelines`**: Runs every ~1s via `advance_timelines_interval` (default 1000ms). For each non-EpochMilliseconds timeline, does O(N × D) work on the single-threaded coordinator. With CDC sources creating custom timelines, this is hit in production.
- **`timedomain_for`**: Runs on EVERY query that starts a transaction. With 100 QPS and S = 500 schema items, that's 50,000 × D lookups per second just for timeline validation.
- Both paths block the coordinator's main loop, preventing other queries from being processed.

### Root Cause

`TimelineContext` is a **pure function of catalog state** — it only changes when the catalog changes (DDL). Yet it's recomputed from scratch on every call via uncached recursive dependency walks. There is no memoization, no caching layer, and no incremental update mechanism.

### Suggested Fix

**Option A (simple, high-impact)**: Cache timeline context per `CatalogItemId` in `CatalogState`. Maintain a `timeline_context_by_id: BTreeMap<CatalogItemId, TimelineContext>` that's populated during `apply_updates()` when items are added/modified. All lookups become O(1). The cache is invalidated naturally when catalog state changes.

With this cache:
- `ids_in_timeline()` becomes O(N) with O(1) per-entry lookup instead of O(N × D)
- `timedomain_for()` retain loop becomes O(S) instead of O(S × D)

**Option B (complementary)**: Pre-compute and maintain `ids_by_timeline: BTreeMap<Timeline, CollectionIdBundle>` in `CatalogState`. This eliminates the full catalog scan in `ids_in_timeline()` entirely, making it O(1). Updated incrementally in `apply_updates()`.

**Option C (for timedomain_for)**: Cache the result of `timedomain_for()` per schema-set + timeline combination, invalidated on catalog change. This avoids recomputing the same timedomain for repeated queries with the same schema set.

### Files Involved
- `src/adapter/src/catalog/timeline.rs` — `ids_in_timeline()` (lines 70-109), `get_timeline_context()` (lines 26-30), `validate_timeline_context()` (lines 116-164), `get_timeline_contexts()` (lines 167-230)
- `src/adapter/src/coord/timeline.rs` — `advance_timelines()` (lines 299-338), `timedomain_for()` (lines 363-468)
- `src/adapter/src/catalog/state.rs` — `CatalogState` struct, where the cache would be added
- `src/adapter/src/catalog/apply.rs` — `apply_updates()`, where the cache would be maintained

## Session 3: O(C + T) All-Table Advancement on Every Group Commit (2026-03-08)

### Location
`src/adapter/src/coord/appends.rs:512-546` — table advancement in `group_commit()`
`src/storage-controller/src/persist_handles.rs:398-477` — `TxnsTableWorker::append()`

### Problem

Every group commit — triggered by **every write transaction** AND a **periodic 1-second timer** — scans the entire catalog to find all tables and includes them in the persist append, even if only 1 table (or zero tables) was actually written to.

**The hot path (appends.rs:512-546):**

```rust
// Step 1: Scan ALL catalog entries to find tables — O(C) where C = total catalog entries
for table in self.catalog().entries().filter(|entry| entry.is_table()) {
    appends.entry(table.id()).or_default();  // insert empty entry for every table
}

// Step 2: Consolidation loop iterates ALL tables (including empty ones) — O(T)
for (item_id, table_data) in appends.into_iter() {
    let mut all_rows = Vec::new();
    let mut all_data = Vec::new();
    for data in table_data { /* ... */ }
    differential_dataflow::consolidation::consolidate(&mut all_rows);  // consolidate empty vec
    all_data.push(TableData::Rows(all_rows));  // push empty Rows
    all_appends.push((item_id, all_data));
}

// Step 3: Resolve GlobalId for ALL tables via catalog lookup — O(T) lookups
let appends: Vec<_> = all_appends.into_iter()
    .map(|(id, updates)| {
        let gid = self.catalog().get_entry(&id).latest_global_id();  // catalog lookup per table
        (gid, updates)
    }).collect();
```

This Vec of ALL tables (with empty data for most) is then sent to the storage controller, which forwards it to the `TxnsTableWorker`. There, **every table is iterated again** (persist_handles.rs:433-461):

```rust
let mut txn = self.txns.begin();
for (id, updates) in updates {            // O(T) — iterates ALL tables
    let Some(data_id) = self.write_handles.get(&id) else { ... };  // BTreeMap lookup per table
    for update in updates {               // inner loop is empty for advancement-only tables
        // ...
    }
}
let txn_res = txn.commit_at(&mut self.txns, write_ts).await;
```

**Total work per group commit:**
1. **O(C)** — full catalog scan to find tables (C = all catalog entries, not just tables)
2. **O(T)** — consolidation of empty vecs for each table
3. **O(T)** — catalog GlobalId resolution for each table
4. **O(T)** — BTreeMap lookups in persist worker for each table
5. **O(T)** — Vec allocation and serialization of T entries sent across channel

Where C = total catalog entries and T = total number of tables.

### Why This Exists

The comment at line 512 says "Add table advancements for all tables." This is needed because in the txn-wal protocol, all tables must have their upper frontier advanced together with the txns shard. The current design achieves this by including ALL tables in every append operation.

### Scaling Impact

- **Frequency**: Group commit fires on every `INSERT`/`UPDATE`/`DELETE`, AND on the periodic timer (default 1 second). Under write workloads, this can fire hundreds of times per second.
- **With 1,000 tables**: Every group commit does ~4,000 unnecessary iterations (1K consolidation + 1K GlobalId lookups + 1K persist worker lookups + 1K Vec entries), even when writing to just 1 table.
- **With 10,000 tables**: ~40,000 unnecessary operations per group commit.
- The team already instruments this with `group_commit_table_advancement_seconds` (line 517-519), suggesting it's a known bottleneck.
- **Single-threaded coordinator**: The catalog scan and consolidation happen on the coordinator's main loop, blocking all other query processing.
- **Unnecessary data movement**: Empty table entries are serialized, sent through an unbounded channel to the persist worker, and iterated there — all for no actual data.

### Root Cause

The txn-wal protocol requires all registered tables to advance their upper together. The current implementation achieves this naively by including every table in every append, rather than having the txn-wal layer handle advancement internally. There is no separation between "tables that have data to write" and "tables that just need advancement."

### Suggested Fix

**Option A (simple, high impact)**: Maintain a `table_ids: BTreeSet<CatalogItemId>` in `CatalogState` that's kept up to date in `apply_updates()`. This eliminates the O(C) full catalog scan (step 1). Combined with a `table_global_ids: BTreeMap<CatalogItemId, GlobalId>` cache, it also eliminates the O(T) per-entry catalog lookups (step 3).

**Option B (deeper fix)**: Separate the "advance all tables" responsibility from the data append path. The txn-wal layer already knows which shards are registered — it could advance all of them as part of `commit_at()` without the coordinator having to enumerate them. The coordinator would only send tables that actually have data to write.

**Option C (complementary)**: Skip the consolidation loop (step 2) for tables with empty data. Currently, even tables with zero rows go through `consolidate(&mut all_rows)` and `all_data.push(TableData::Rows(all_rows))`, creating unnecessary allocations.

### Files Involved
- `src/adapter/src/coord/appends.rs` — `group_commit()` (lines 328-600), table advancement (lines 512-546), `trigger_group_commit()` (line 168)
- `src/adapter/src/coord.rs` — main loop timer at line 3602 triggers `GroupCommitInitiate` every 1s
- `src/storage-controller/src/persist_handles.rs` — `TxnsTableWorker::append()` (lines 398-477), iterates all tables
- `src/storage-controller/src/lib.rs` — `append_table()` (lines 2117-2149), forwards to persist worker
- `src/adapter/src/catalog.rs` — `entries()` used for the full scan

## Session 4: Compute Controller `maintain()` — O(C × R × D) Work Every Second (2026-03-08)

### Location
`src/compute-client/src/controller/instance.rs:2233-2242` — `Instance::maintain()`
`src/compute-client/src/controller.rs:254` — 1-second maintenance ticker

### Problem

The compute controller's `maintain()` method runs **every 1 second** (line 254: `time::interval(Duration::from_secs(1))`) and calls 8 sub-methods, most of which perform **full iterations over all collections and all replicas**, regardless of whether anything has changed.

**The maintain() call chain (instance.rs:2233-2242):**

```rust
pub fn maintain(&mut self) {
    self.rehydrate_failed_replicas();          // O(R)
    self.downgrade_warmup_capabilities();      // O(C × D) — Problem A
    self.forward_implied_capabilities();       // O(C × D) with BFS — Problem B
    self.schedule_collections();               // O(C × D) — Problem C
    self.cleanup_collections();                // O(C × R) — Problem D
    self.update_frontier_introspection();      // O(C + R × C) — Problem E
    self.refresh_state_metrics();              // O(C + R) — minor
    self.refresh_wallclock_lag();              // O(C + R × C) — Problem F
}
```

**Problem A — `downgrade_warmup_capabilities()` (lines 2116-2143):**
For EVERY collection, iterates all its dependencies via `dependency_write_frontiers()` (line 2130) which does a BTreeMap lookup per compute dependency and a `storage_collections.collection_frontiers()` call per storage dependency. Then iterates all frontier times to compute step-back. Does this even for collections whose frontiers haven't changed. **Cost: O(C × D)** per tick.

**Problem B — `forward_implied_capabilities()` (lines 2172-2205):**
For EVERY collection, calls `transitive_storage_dependency_write_frontiers()` (line 2077-2099) which performs a **full BFS through the compute dependency graph** to find all transitive storage dependencies:

```rust
fn transitive_storage_dependency_write_frontiers(...) {
    let mut storage_ids: BTreeSet<_> = collection.storage_dependency_ids().collect();
    let mut todo: Vec<_> = collection.compute_dependency_ids().collect();
    let mut done = BTreeSet::new();
    while let Some(id) = todo.pop() {         // BFS loop
        if done.contains(&id) { continue; }
        if let Some(dep) = self.collections.get(&id) {
            storage_ids.extend(dep.storage_dependency_ids());
            todo.extend(dep.compute_dependency_ids())  // Expands frontier
        }
        done.insert(id);
    }
    // ...
}
```

Each collection's BFS independently traverses the **same sub-DAGs** — no sharing of results between collections. With deep dependency chains (common with stacked views/indexes), the same nodes are visited thousands of times. **Cost: O(C × D_transitive)** per tick. Mitigated only when `self.replicas.is_empty()` (paused clusters).

**Problem C — `schedule_collections()` (lines 1479-1484):**
Calls `maybe_schedule_collection()` for EVERY collection every tick. Even already-scheduled collections must be looked up to check their `scheduled` flag (line 1415: early return). For unscheduled collections, reads frontiers of all dependencies. **Cost: O(C × D)** per tick.

**Problem D — `cleanup_collections()` (lines 745-762):**
For EVERY collection, checks if it's dropped AND read capabilities are empty AND write frontiers are empty on ALL replicas:

```rust
.filter(|(id, collection)| {
    collection.dropped
        && collection.shared.lock_read_capabilities(|c| c.is_empty())
        && self.replicas.values().all(|r| r.collection_frontiers_empty(*id))
})
```

Each `lock_read_capabilities` acquires a mutex. Each `collection_frontiers_empty` does a BTreeMap lookup per replica. **Cost: O(C × R)** per tick, with C mutex acquisitions.

**Problem E — `update_frontier_introspection()` (lines 415-429):**
Two full iterations:
1. All instance-level collections: O(C), each calling `read_frontier()` and `write_frontier()` (which acquire locks)
2. All replica-level collections: O(R × C_per_replica), each calling `write_frontier`

**Cost: O(C + R × C)** per tick.

**Problem F — `refresh_wallclock_lag()` (lines 489-557):**
Two full iterations with per-collection **storage controller queries**:
1. For EVERY collection, calls `self.storage_collections.collection_frontiers(*id)` (line 508) — a cross-controller lookup — plus histogram bucket computation with `histogram_labels.clone()` allocating a new BTreeMap per collection
2. For EVERY replica × collection, computes and records lag metrics

**Cost: O(C + R × C)** per tick, with C cross-controller calls.

### Scaling Impact

Combined per-tick cost: **O(C × (D + D_transitive + D + R) + 2 × R × C)**

With realistic production numbers:
- **C** = 2,000 collections (indexes + MVs + sinks + subscribes)
- **R** = 5 replicas
- **D** = 5 avg direct dependencies, **D_transitive** = 15 avg transitive deps

Per tick (every second):
- `downgrade_warmup_capabilities`: 2,000 × 5 = 10,000 frontier reads
- `forward_implied_capabilities`: 2,000 × 15 = 30,000 BFS node visits (only on paused clusters)
- `schedule_collections`: 2,000 × 5 = 10,000 operations (mostly early-returns for scheduled)
- `cleanup_collections`: 2,000 × 5 = 10,000 replica checks + 2,000 mutex locks
- `update_frontier_introspection`: 2,000 + 5 × 2,000 = 12,000 frontier reads
- `refresh_wallclock_lag`: 2,000 storage controller calls + 5 × 2,000 = 12,000 metric updates

**Total: ~84,000 operations per second**, most of which are redundant because frontiers and dependencies rarely change between ticks.

### Root Cause

The `maintain()` method uses a **poll-everything-every-tick** architecture instead of an **event-driven/incremental** approach. Every sub-method recomputes its results from scratch by iterating all collections, even though:

1. **Frontiers change infrequently** — most collections' frontiers change far less often than once per second
2. **Dependencies are static** — they only change on DDL (create/drop dataflow), yet transitive dependency walks are recomputed every tick
3. **Most collections aren't dropped** — `cleanup_collections` checks ALL collections to find the rare dropped ones
4. **Scheduling is one-time** — once scheduled, a collection stays scheduled, yet every collection is re-checked

### Suggested Fix

**Option A (event-driven frontier tracking)**: Instead of polling all frontiers every tick, use frontier change notifications. When a collection's frontier advances, mark it dirty. Only process dirty collections in `downgrade_warmup_capabilities` and `update_frontier_introspection`. This reduces the per-tick cost from O(C) to O(C_changed), which is typically << C.

**Option B (cache transitive dependencies)**: Pre-compute and cache `transitive_storage_dependency_write_frontiers` for each collection. Invalidate only when a dataflow is created or dropped (DDL). This eliminates the BFS traversal from every tick.

**Option C (separate cleanup from hot path)**: Move `cleanup_collections` to a lower-frequency timer (e.g., every 10 seconds) since dropped collections with empty frontiers are rare. Add a `pending_cleanup: BTreeSet<GlobalId>` that's populated when a collection is dropped, so only candidates are checked.

**Option D (incremental metrics)**: Instead of counting unscheduled collections and connected replicas from scratch, maintain counters that are updated when state changes. Same for wallclock lag — only recompute for collections whose frontiers have changed.

### Files Involved
- `src/compute-client/src/controller/instance.rs` — `maintain()` (line 2233), `downgrade_warmup_capabilities()` (line 2116), `forward_implied_capabilities()` (line 2172), `schedule_collections()` (line 1479), `cleanup_collections()` (line 745), `update_frontier_introspection()` (line 415), `refresh_wallclock_lag()` (line 489), `transitive_storage_dependency_write_frontiers()` (line 2077), `dependency_write_frontiers()` (line 2060)
- `src/compute-client/src/controller.rs` — `maintain()` (line 1044), maintenance ticker setup (line 254), `process()` (line 1032)

## Session 5: Redundant Per-Query RBAC Role Membership Graph Traversal (2026-03-08)

### Location
`src/sql/src/rbac.rs:208` — `validate()` calls `collect_role_membership()`
`src/sql/src/rbac.rs:305` — `check_usage()` calls `collect_role_membership()` again
`src/sql/src/rbac.rs:1773` — `check_object_privileges()` calls `collect_role_membership()` per distinct role
`src/adapter/src/catalog/state.rs:918-934` — `collect_role_membership()` implementation

### Problem

Every SQL statement triggers RBAC privilege checking via `rbac::check_plan()` (called from `src/adapter/src/coord/sequencer.rs:191`). Within a single query's RBAC validation, `collect_role_membership()` is called **at least twice** for the **same role** with no caching between calls:

**Call 1 — `validate()` (rbac.rs:208):**
```rust
fn validate(self, catalog, session, resolved_ids) -> Result<(), UnauthorizedError> {
    let role_membership =
        catalog.collect_role_membership(&session.role_metadata().current_role);  // BFS traversal
    check_usage(catalog, session, resolved_ids, self.item_usage)?;  // <-- calls it again inside!
    // ...
    check_object_privileges(catalog, self.privileges, role_membership, ...)?;
}
```

**Call 2 — `check_usage()` (rbac.rs:305), called from `validate()` at line 210:**
```rust
fn check_usage(catalog, session, resolved_ids, item_types) -> Result<(), UnauthorizedError> {
    let role_membership = catalog.collect_role_membership(&session.role_metadata().current_role);  // SAME role, SAME result
    // ... uses role_membership for usage privilege checks ...
}
```

**Call 3+ — `check_object_privileges()` (rbac.rs:1771-1773), called from `validate()` at line 238:**
```rust
for (object_id, acl_mode, role_id) in privileges {
    let role_membership = role_memberships
        .entry(role_id)
        .or_insert_with_key(|role_id| catalog.collect_role_membership(role_id));  // per distinct role
    // ...
}
```

The `collect_role_membership()` implementation (state.rs:918-934) performs a **BFS traversal of the entire role membership graph** every time:

```rust
pub(crate) fn collect_role_membership(&self, id: &RoleId) -> BTreeSet<RoleId> {
    let mut membership = BTreeSet::new();
    let mut queue = VecDeque::from(vec![id]);
    while let Some(cur_id) = queue.pop_front() {
        if !membership.contains(cur_id) {
            membership.insert(cur_id.clone());
            let role = self.get_role(cur_id);
            queue.extend(role.membership().keys());  // expand all parent roles
        }
    }
    membership.insert(RoleId::Public);
    membership
}
```

### Compounding Issue: Recursive View Privilege Generation

For queries involving views, `generate_read_privileges_inner()` (rbac.rs:1665-1706) recursively walks the view dependency chain. At each level, it switches to the **view owner's role** for privilege checking:

```rust
fn generate_read_privileges_inner(catalog, ids, role_id, seen) {
    for id in ids {
        match item.item_type() {
            View | MaterializedView | ContinualTask => {
                privileges.push((..., AclMode::SELECT, role_id));
                views.push((item.references().items(), item.owner_id()));  // switch to owner
            }
            // ...
        }
    }
    for (view_ids, view_owner) in views {
        privileges.extend(generate_read_privileges_inner(catalog, view_ids, view_owner, seen));
        // ^ recursive call with different role_id (view_owner)
    }
}
```

Each distinct `view_owner` encountered generates a new `collect_role_membership()` call in `check_object_privileges()` (line 1773). With N views owned by K distinct roles, this triggers K additional BFS traversals.

Then in `check_object_privileges()`, for EACH privilege entry, the code iterates ALL role memberships to check ACLs:

```rust
let role_privileges = role_membership
    .iter()  // O(R) — iterate all roles in membership
    .flat_map(|role_id| object_privileges.get_acl_items_for_grantee(role_id))
    .map(|mz_acl_item| mz_acl_item.acl_mode)
    .fold(AclMode::empty(), |accum, acl_mode| accum.union(acl_mode));
```

### Scaling Impact

**Per-query cost breakdown:**
- **Minimum 2 BFS traversals** of role graph for `current_role` (calls 1 and 2 — pure waste)
- **K additional BFS traversals** for K distinct view owners in the query
- **O(P × R)** privilege checking where P = privileges to check, R = role membership size
- Each BFS traversal is **O(R)** where R = total reachable roles, with BTreeSet insertions

**With realistic numbers:**
- 50 roles with 3-level hierarchy → each BFS visits ~15 roles, does ~15 BTreeSet inserts
- Simple SELECT: 2 redundant BFS traversals + privilege checks = ~30 wasted operations
- SELECT from 10 views owned by 5 distinct roles: 2 + 5 = 7 BFS traversals × 15 roles = ~105 role graph operations
- Complex query with 100 dependencies: privilege Vec can have 200+ entries, each checked against 15 roles = 3,000+ ACL lookups

**At 1,000 QPS** (typical for Materialize):
- Minimum **2,000 redundant BFS traversals/second** just from the duplicate call
- With view hierarchies: **7,000+ BFS traversals/second**
- All on the **single-threaded coordinator**, blocking query processing

### Root Cause

1. **No cross-function caching**: `validate()` computes role membership at line 208 but `check_usage()` (called at line 210) recomputes the same thing. The result from line 208 is never passed to `check_usage()`.

2. **No session-level caching**: Role membership is a pure function of catalog state. It changes only on DDL (`CREATE ROLE`, `GRANT`, `REVOKE`), yet is recomputed from scratch on every single query. There's no mechanism to cache the result for a session or invalidate it on role changes.

3. **Privilege Vec as unsorted list**: Privileges are accumulated in a `Vec` during `generate_read_privileges_inner()`, which can contain duplicates (same object checked with different role_ids). The Vec is then linearly scanned in `check_object_privileges()`.

### Suggested Fix

**Option A (simple, immediate — fix redundant calls)**: Pass the already-computed `role_membership` from `validate()` into `check_usage()`, eliminating the duplicate BFS traversal. This is a 2-line change:

```rust
fn validate(self, catalog, session, resolved_ids) {
    let role_membership = catalog.collect_role_membership(&session.role_metadata().current_role);
    check_usage(catalog, session, resolved_ids, self.item_usage, &role_membership)?;  // pass it in
    // ...
}
```

**Option B (session-level caching)**: Cache `collect_role_membership()` results in the session metadata, keyed by `RoleId`. Invalidate the cache when the catalog's role-related state changes (i.e., on `CREATE/ALTER/DROP ROLE`, `GRANT`, `REVOKE`). Since role changes are rare (DDL-only), the cache hit rate would be ~100% for steady-state queries.

**Option C (catalog-level caching)**: Maintain a `role_membership_cache: BTreeMap<RoleId, BTreeSet<RoleId>>` in `CatalogState`. Invalidate/rebuild only when roles or role memberships change in `apply_updates()`. This benefits ALL sessions simultaneously.

**Option D (privilege deduplication)**: Use `BTreeSet` instead of `Vec` for privilege accumulation in `generate_read_privileges_inner()` to eliminate duplicate privilege checks in `check_object_privileges()`.

### Files Involved
- `src/sql/src/rbac.rs` — `validate()` (line 200), `check_usage()` (line 296), `check_object_privileges()` (line 1752), `generate_read_privileges_inner()` (line 1665), `check_plan()` entry point (line 337)
- `src/adapter/src/catalog/state.rs` — `collect_role_membership()` (line 918)
- `src/adapter/src/catalog.rs` — `collect_role_membership()` delegation (line 1930)
- `src/adapter/src/coord/sequencer.rs` — `check_plan()` call site (line 191)
- `src/sql/src/catalog.rs` — `SessionCatalog::collect_role_membership` trait method (line 195)

## Session 6: Statement Logging `mutate_record` — 9 Full Row Packs Per Query (2026-03-08)

### Location
`src/adapter/src/coord/statement_logging.rs:506-525` — `mutate_record()`
`src/adapter/src/statement_logging.rs:858-919` — `pack_statement_execution_inner()`
`src/adapter/src/statement_logging.rs:921-940` — `pack_statement_began_execution_update()`

### Problem

Every sampled SQL statement execution triggers **9 full Row serializations** of a 20-field record, when only 1-2 would be necessary. The root cause is the `mutate_record` pattern, which retracts and re-inserts the entire row to update a single field — and this is called 3 separate times per statement for 3 different fields.

**The mutate_record pattern (statement_logging.rs:506-525):**

```rust
fn mutate_record<F: FnOnce(&mut StatementBeganExecutionRecord)>(
    &mut self,
    StatementLoggingId(id): StatementLoggingId,
    f: F,
) {
    let record = self.statement_logging.executions_begun.get_mut(&id)...;
    let retraction = pack_statement_began_execution_update(record);  // FULL 20-field Row pack
    self.statement_logging.pending_statement_execution_events.push((retraction, Diff::MINUS_ONE));
    f(record);                                                        // mutate ONE field
    let update = pack_statement_began_execution_update(record);      // FULL 20-field Row pack AGAIN
    self.statement_logging.pending_statement_execution_events.push((update, Diff::ONE));
}
```

**Called 3 times per statement, each for a single field change:**

1. **`set_statement_execution_cluster`** (line 537): Sets `cluster_name` and `cluster_id` → 2 full Row packs
2. **`set_statement_execution_timestamp`** (line 549): Sets `execution_timestamp` → 2 full Row packs
3. **`set_transient_index_id`** (line 559): Sets `transient_index_id` → 2 full Row packs

**Plus the initial and final Row packs:**
4. **`write_began_execution_events`** (line 127): Initial Row with all NULLs for unknown fields → 1 full Row pack
5. **`end_statement_execution`** (line 414): Retraction of current + full row with ended fields → 2 full Row packs

**Total: 9 full Row packs per sampled statement** (1 begin + 6 mutations + 2 end).

### What Each Row Pack Does

`pack_statement_execution_inner()` (statement_logging.rs:858-919) performs for EACH of the 9 packs:

1. `cluster_id.map(|id| id.to_string())` — UUID → String allocation (line 880)
2. `transient_index_id.map(|id| id.to_string())` — UUID → String allocation (line 881)
3. `packer.push_list(search_path.iter()...)` — iterates and packs all search path schemas (line 894)
4. `packer.try_push_array(...)` — iterates and packs all query parameters (lines 905-914)
5. Packs 14+ additional scalar Datums into the Row buffer
6. Allocates a new `Row::default()` buffer each time (line 922)

### Per-Statement Overhead Breakdown

For a typical query with 3 search_path schemas and 5 parameters:

| Operation | Count | Cost per |
|-----------|-------|----------|
| `Row::default()` allocation | 9 | heap alloc |
| UUID → String conversion | 18 | 2 per pack × 9 packs |
| search_path iteration + pack | 9 | 3 items × 9 packs |
| params array iteration + pack | 9 | 5 items × 9 packs |
| Scalar Datum packing | 9 | 14+ fields × 9 packs |
| `Vec::push` for pending events | 9 | 1 per pack |

**Total per statement: 9 Row allocations, 18 UUID→String conversions, 72 search_path Datum packs, 45 param Datum packs, 126+ scalar Datum packs.**

### Scaling Impact

- **With statement sampling at 100%** (common for debugging): At 1,000 QPS, this generates **9,000 Row packs/second**, **18,000 UUID→String allocations/second**, and pushes **9,000 entries into the pending events Vec** every 5 seconds (45,000 accumulated entries per drain)
- **Even at 1% sampling**: 10 QPS × 9 = 90 Row packs/second — still significant overhead on the single-threaded coordinator
- All 9 Row packs run on the **coordinator's main loop**, blocking other query processing
- The `pending_statement_execution_events` Vec grows unboundedly between 5-second drains (line 197), accumulating 7 entries per sampled statement (1 initial + 3×2 mutations)
- The `executions_begun` BTreeMap (line 53) holds the full `StatementBeganExecutionRecord` for every in-flight sampled statement — no timeout or cleanup mechanism if execution end is never received

### Root Cause

The `mutate_record` pattern treats statement execution logging as a **mutable differential collection** where every field change must be immediately reflected as a retract+insert. But the fields being set (cluster, timestamp, transient_index) are always known before the statement finishes — they just aren't known at the time `begin_statement_execution` is called.

The architecture eagerly emits a "began" row with NULL fields, then retroactively corrects each field via retract+reinsert. This is architecturally backwards — the row should be packed once when all fields are known, not incrementally patched 3 times.

### Suggested Fix

**Option A (simple, highest impact — batch mutations)**: Instead of calling `mutate_record` 3 separate times, accumulate field updates on the `StatementBeganExecutionRecord` in `executions_begun` without packing any rows. Only pack the retraction+update once, when all fields are set or when execution ends. This reduces 7 Row packs (1 begin + 6 mutations) to 1 Row pack, for a **7× reduction**.

```rust
// Instead of mutate_record, just update the record directly:
fn set_statement_execution_cluster(&mut self, id: StatementLoggingId, cluster_id: ClusterId) {
    let record = self.statement_logging.executions_begun.get_mut(&id.0)...;
    record.cluster_name = Some(cluster_name);
    record.cluster_id = Some(cluster_id);
    // NO row packing — defer until end_statement_execution
}
```

Then in `end_statement_execution`, the retraction uses the original "began" row (which can be stored alongside the record), and the update uses the final record with all fields set.

**Option B (defer began row)**: Don't emit the initial "began" row at all. Instead, when `end_statement_execution` is called, emit a single complete row with Diff::ONE. This reduces 9 Row packs to 1 (the final complete row). The downside is that in-flight statements won't appear in `mz_statement_execution_history` until they finish, but this is acceptable since the data is only flushed every 5 seconds anyway.

**Option C (pre-allocate Row buffer)**: At minimum, reuse a single `Row` buffer across the 9 packs instead of allocating `Row::default()` each time. This eliminates 8 of the 9 heap allocations but doesn't address the redundant packing work.

### Files Involved
- `src/adapter/src/coord/statement_logging.rs` — `mutate_record()` (line 506), `set_statement_execution_cluster()` (line 531), `set_statement_execution_timestamp()` (line 544), `set_transient_index_id()` (line 554), `write_began_execution_events()` (line 118), `end_statement_execution()` (line 394), `drain_statement_log()` (line 212)
- `src/adapter/src/statement_logging.rs` — `pack_statement_execution_inner()` (line 858), `pack_statement_began_execution_update()` (line 921), `StatementBeganExecutionRecord` struct
- `src/adapter/src/coord/sequencer.rs` — `set_statement_execution_cluster()` call (line 176)
- `src/adapter/src/coord/peek.rs` — `set_statement_execution_timestamp()` call (line 726)
- `src/adapter/src/coord/sequencer/inner/peek.rs` — `set_transient_index_id()` call (line 867)

## Session 8: Catalog Transaction Double-Apply with Expression Cache Triple-Clone and SQL Re-Parsing (2026-03-08)

### Location
`src/adapter/src/catalog/transact.rs:550-709` — `transact_inner()`
`src/adapter/src/catalog/apply.rs:99-153` — `apply_updates()`
`src/adapter/src/catalog/apply.rs:2089-2101` — `sort_items_topological()` (SQL re-parsing)
`src/adapter/src/catalog/state.rs:178-196` — `LocalExpressionCache`

### Problem

Every catalog transaction (DDL) applies all state updates **twice** — once on a "preliminary" state copy (to let successive ops see prior ops' results) and once on the real state (to produce the final builtin table updates). This is an explicit design choice acknowledged as suboptimal in the source comments (line 575: *"We won't win any DDL throughput benchmarks"*).

**The two-phase architecture (transact.rs:555-674):**

```
Phase 1 — Per-op loop (lines 608-661):
  For each op in the transaction:
    1. transact_op() — derives state updates for this op
    2. Clone cached_exprs (BTreeMap<GlobalId, LocalExpressions>)       ← CLONE #1
    3. Clone op_updates (Vec<StateUpdate>)                             ← CLONE
    4. preliminary_state.to_mut().apply_updates(op_updates.clone())
       → consolidate_updates() + sort_updates()
         → sort_items_topological() → RE-PARSES CREATE SQL for each item
    5. Accumulate updates

Phase 2 — Final batch apply (lines 663-674):
    1. Clone cached_exprs again                                        ← CLONE #2
    2. Clone ALL accumulated updates                                   ← CLONE
    3. state.to_mut().apply_updates(updates.clone())
       → consolidate_updates() + sort_updates()
         → sort_items_topological() → RE-PARSES CREATE SQL AGAIN

Phase 3 — Post-storage apply (lines 688-700):
    1. Clone cached_exprs again                                        ← CLONE #3
    2. Clone storage updates                                           ← CLONE
    3. state.to_mut().apply_updates(updates.clone())
       → consolidate_updates() + sort_updates()
```

### What Gets Cloned

**Expression cache (`cached_exprs`)** — `BTreeMap<GlobalId, LocalExpressions>`:
- Contains one `OptimizedMirRelationExpr` per view/MV in the transaction
- `OptimizedMirRelationExpr` is a recursive tree of MIR expressions — can be deeply nested for complex queries
- Cloned up to 3 times per transaction because of "remove semantics" (line 652-653)
- The remove semantics exist because `apply_updates` consumes expressions from the cache during deserialization

**Update vectors (`op_updates`, `updates`)** — `Vec<StateUpdate>`:
- `StateUpdate` contains `StateUpdateKind` — a large enum with 20+ variants including full `Item` proto objects with `create_sql` strings
- Cloned before each `apply_updates()` call because the caller retains the originals for accumulation
- Phase 1 clones per-op updates N times (once per op), Phase 2 clones ALL accumulated updates

### SQL Re-Parsing in Topological Sort

Inside each `apply_updates()` call, `sort_updates()` calls `sort_items_topological()` (apply.rs:2089-2101) for connections and derived items (views, MVs, indexes):

```rust
fn sort_items_topological(items: &mut Vec<(Item, Timestamp, StateDiff)>) {
    let dependencies_fn = |item: &(Item, _, _)| {
        let statement = mz_sql::parse::parse(&item.0.create_sql)  // RE-PARSE SQL
            .expect("valid create_sql")
            .into_element()
            .ast;
        mz_sql::names::dependencies(&statement)                    // EXTRACT DEPS
    };
    sort_topological(items, key_fn, dependencies_fn);
}
```

This **re-parses the CREATE SQL** of every view, MV, index, and connection just to determine dependency ordering — and this happens in BOTH Phase 1 (per-op) AND Phase 2 (final batch). The dependencies are inherently static properties of each item and could be cached or stored alongside the item rather than re-extracted from SQL text on every transaction.

### Scaling Impact

For a transaction creating M views/MVs in a catalog with N total items:

| Operation | Phase 1 (per-op) | Phase 2 (batch) | Phase 3 (storage) | Total |
|---|---|---|---|---|
| Expression cache clones | M × BTreeMap clone | 1 × BTreeMap clone | 1 × BTreeMap clone | M + 2 |
| Update vector clones | M × per-op Vec clone | 1 × full Vec clone | 1 × Vec clone | M + 2 |
| `apply_updates()` calls | M calls | 1 call | 0-1 calls | M + 1 to M + 2 |
| SQL re-parsing | Up to M² parses (cumulative) | M parses | 0 | O(M²) |
| Consolidation passes | M passes | 1 pass | 0-1 | M + 1 to M + 2 |

**The SQL re-parsing is particularly bad**: In Phase 1, each op's `apply_updates()` sorts the current op's items. But items accumulate, so if Phase 2's `sort_items_topological` receives all M items, it parses M SQL strings. Across Phase 1's M iterations, the total parses can approach O(M²) because each iteration may sort an overlapping subset.

**Concrete example — creating 100 materialized views in one transaction:**
- 100 expression cache clones (Phase 1) + 2 more (Phases 2-3) = 102 BTreeMap clones
- 100 update vector clones (Phase 1) + 2 (Phases 2-3) = 102 Vec clones
- ~100 SQL re-parses in Phase 1 + 100 in Phase 2 = ~200 SQL parses
- 102 `apply_updates()` calls, each with consolidation + sorting overhead

### Root Cause

The two-phase design exists because `transact_op()` for op N needs to see the state changes from ops 1..N-1. The current approach achieves this by fully applying each op to a preliminary state copy. The "final" apply then replays everything from scratch to produce the correct builtin table updates.

The expression cache uses `remove` semantics (each `apply_updates` takes expressions out of the cache), so the cache must be cloned before each call. This is a data flow design issue — the cache could instead use `get`/`clone` semantics or reference counting.

### Suggested Fix

**Option A (eliminate Phase 1 per-op applies — highest impact):** Refactor `transact_op()` to not require a fully-applied catalog state. Instead, maintain a lightweight "pending changes" overlay that successive ops can query. This eliminates M intermediate `apply_updates()` calls, reducing the total to 1-2.

**Option B (share expression cache with Arc):** Wrap `LocalExpressions` in `Arc` so the BTreeMap clone is O(N) pointer copies instead of O(N) deep expression tree clones:
```rust
// Before: BTreeMap<GlobalId, LocalExpressions>  — deep clone
// After:  BTreeMap<GlobalId, Arc<LocalExpressions>>  — shallow clone
```

**Option C (cache item dependencies):** Store each item's dependency set alongside the item (e.g., in `durable::Item`) instead of re-extracting from CREATE SQL via parsing. This eliminates ALL SQL re-parsing in `sort_items_topological()`.

**Option D (switch to get semantics for expression cache):** Change `LocalExpressionCache` from `remove`+clone to `get`+clone-on-demand. This avoids cloning the entire BTreeMap — only the specific expressions that are actually needed get cloned:
```rust
// Before: clone entire cache, then remove entries
// After: borrow cache reference, clone individual entries as needed
```

### Files Involved
- `src/adapter/src/catalog/transact.rs` — `transact_inner()` (line 533), `extract_expressions_from_ops()` (line 498)
- `src/adapter/src/catalog/apply.rs` — `apply_updates()` (line 99), `sort_updates()` (line 1944), `sort_items_topological()` (line 2089), `sort_item_updates()` (line 2118), `consolidate_updates()` (line 162)
- `src/adapter/src/catalog/state.rs` — `LocalExpressionCache` (line 178)
- `src/catalog/src/expr_cache.rs` — `LocalExpressions` (line 57, contains `OptimizedMirRelationExpr`)

---

## Session 7: Storage Controller `maintain()` — Triple Frontier Scan with Redundant Lock+Clone (2026-03-08)

### Location
`src/storage-controller/src/lib.rs:3805-3813` — `maintain()`
`src/storage-client/src/storage_collections.rs:1436-1451` — `active_collection_frontiers()`
`src/storage-controller/src/lib.rs:3548-3641` — `update_frontier_introspection()`
`src/storage-controller/src/lib.rs:3661-3716` — `refresh_wallclock_lag()`
`src/storage-controller/src/lib.rs:3725-3799` — `maybe_record_wallclock_lag()`

### Problem

The storage controller's `maintain()` runs **every ~1 second** (line 3803) and calls two sub-methods that independently fetch the **same frontier data** by calling `active_collection_frontiers()` — a method that acquires a mutex lock, iterates ALL non-dropped collections, and clones 3 `Antichain` values per collection. When the wallclock lag recording interval elapses (~1 minute), a **third** call fetches the same data again.

**The `maintain()` method (lib.rs:3805-3813):**

```rust
fn maintain(&mut self) {
    self.update_frontier_introspection();   // Call 1: active_collection_frontiers()
    self.refresh_wallclock_lag();           // Call 2: active_collection_frontiers()
                                           // Call 2b (inside): maybe_record_wallclock_lag()
                                           //   → Call 3: active_collection_frontiers()
    for instance in self.instances.values_mut() {
        instance.refresh_state_metrics();
    }
}
```

**What `active_collection_frontiers()` does each time (storage_collections.rs:1436-1451):**

```rust
fn active_collection_frontiers(&self) -> Vec<CollectionFrontiers<Self::Timestamp>> {
    let collections = self.collections.lock().expect("lock poisoned");  // MUTEX LOCK
    collections
        .iter()                                    // O(N) iteration over ALL collections
        .filter(|(_id, c)| !c.is_dropped())        // check dropped flag
        .map(|(id, c)| CollectionFrontiers {
            id: id.clone(),                                    // Clone GlobalId
            write_frontier: c.write_frontier.clone(),          // Clone Antichain (heap alloc)
            implied_capability: c.implied_capability.clone(),  // Clone Antichain (heap alloc)
            read_capabilities: c.read_capabilities.frontier().to_owned(),  // Clone Antichain
        })
        .collect_vec()                             // Allocate Vec of all results
}
```

**Each call:**
1. Acquires the `collections` mutex (contended with storage ingestion threads)
2. Iterates ALL collections (including non-relevant ones like system tables)
3. Clones 3 `Antichain<T>` values per collection (each is a heap-allocated `Vec<T>`)
4. Allocates a `Vec<CollectionFrontiers>` of size N
5. Drops the lock

**Call 1 — `update_frontier_introspection()` (lines 3548-3641):**
Uses the frontiers to compute a diff against the previously recorded frontiers (`self.recorded_frontiers`). Builds `global_frontiers: BTreeMap` and `replica_frontiers: BTreeMap` from the cloned data, then diffs to emit introspection Row updates. Also clones `upper` again per replica at line 3569: `replica_frontiers.insert((id, replica_id), upper.clone())`.

**Call 2 — `refresh_wallclock_lag()` (lines 3661-3716):**
Uses the **same** frontiers to compute wallclock lag per collection. For each collection:
- Looks up `self.collections.get_mut(&id)` (line 3673) — a second map lookup for the same collection already iterated in `active_collection_frontiers()`
- Computes `PartialOrder::less_equal` between write_frontier and read_capabilities (line 3678)
- Constructs a `BTreeMap` entry per collection with histogram labels (line 3709) — includes `workload_class.clone()` allocation

**Call 3 — `maybe_record_wallclock_lag()` (lines 3725-3799):**
Called from `refresh_wallclock_lag()` at line 3715. Has an early return if the recording interval hasn't elapsed (line 3742), but when it fires (~every minute), it fetches ALL frontiers a **third** time just to read `wallclock_lag_max` from `self.collections` — data that was already computed in Call 2.

### Redundancy Analysis

All three calls need the same data: `(GlobalId, write_frontier, read_capabilities)` for every active collection. The data doesn't change between calls since they run synchronously within the same `maintain()` invocation.

| What's Duplicated | Call 1 | Call 2 | Call 3 |
|---|---|---|---|
| Mutex lock acquisition | Yes | Yes | Yes (when interval fires) |
| Full collection iteration | Yes | Yes | Yes |
| 3× Antichain clone per collection | Yes | Yes | Yes |
| Vec<CollectionFrontiers> allocation | Yes | Yes | Yes |
| Lookup self.collections per ID | Yes (line 3559) | Yes (line 3673) | Yes (line 3753) |

Furthermore, `maybe_record_wallclock_lag()` (Call 3) doesn't even use the frontier data from `active_collection_frontiers()` — it only reads `collection.wallclock_lag_max` (line 3757) and `collection.wallclock_lag_histogram_stash` (line 3766), which were **already populated by Call 2**. The only reason it calls `active_collection_frontiers()` is to get the list of active collection IDs — it could instead iterate `self.collections` directly.

### Scaling Impact

**Per `maintain()` tick (every second):**

With N = 5,000 active collections (realistic for production with sources, tables, MVs, indexes):

| Operation | Per Call | × Calls/tick | Total/tick |
|---|---|---|---|
| Mutex lock acquisitions | 1 | 2 (or 3) | 2-3 |
| Collection iterations | N = 5,000 | 2-3 | 10,000-15,000 |
| Antichain clones (heap allocs) | 3N = 15,000 | 2-3 | 30,000-45,000 |
| Vec allocations (5,000 items) | 1 | 2-3 | 2-3 |
| self.collections lookups | N = 5,000 | 2-3 | 10,000-15,000 |

**Total per second: ~50,000-75,000 redundant operations** that produce identical data.

The mutex contention is particularly concerning because `self.collections` is shared with the storage ingestion path — ingestion threads update `write_frontier` as new data arrives. Each `active_collection_frontiers()` call holds the lock for O(N) time while cloning all frontiers, blocking ingestion updates.

### Root Cause

Each `maintain()` sub-method independently fetches frontier data through the same `active_collection_frontiers()` API. There is no mechanism to:
1. Fetch the frontiers once and share the result between sub-methods
2. Cache the result within a `maintain()` call
3. Track which collections' frontiers have changed since the last tick

The `active_collection_frontiers()` API is designed as a "snapshot the world" operation — it always clones everything, even if 99% of frontiers haven't changed since the last call.

### Suggested Fix

**Option A (simple, highest impact — fetch once, share)**: Compute `active_collection_frontiers()` once at the top of `maintain()` and pass the result to both sub-methods:

```rust
fn maintain(&mut self) {
    let frontiers = self.storage_collections.active_collection_frontiers();
    self.update_frontier_introspection(&frontiers);
    self.refresh_wallclock_lag(&frontiers);
    // ...
}
```

This immediately eliminates 50% of the work (1 call instead of 2, or 1 instead of 3 when recording). No API changes needed beyond the method signatures.

**Option B (eliminate Call 3 entirely)**: `maybe_record_wallclock_lag()` doesn't need frontier data — it only reads `wallclock_lag_max` and `wallclock_lag_histogram_stash` from `self.collections`. Refactor it to iterate `self.collections` directly instead of calling `active_collection_frontiers()`. This saves the 3rd lock+clone entirely.

**Option C (incremental frontier tracking)**: Instead of cloning all frontiers every tick, maintain a change log. When `write_frontier` or `read_capabilities` changes on a collection, add it to a `dirty_collections: BTreeSet<GlobalId>`. In `maintain()`, only process dirty collections. This reduces per-tick cost from O(N) to O(N_changed), which is typically << N since most frontiers advance infrequently.

**Option D (lock-free frontier reads)**: Replace the mutex-protected `collections` map with per-collection `AtomicAntichain` or `watch::Sender<Antichain<T>>` for the frontiers. This eliminates the global lock entirely, allowing `maintain()` to read frontiers without blocking ingestion.

### Files Involved
- `src/storage-controller/src/lib.rs` — `maintain()` (line 3805), `update_frontier_introspection()` (line 3548), `refresh_wallclock_lag()` (line 3661), `maybe_record_wallclock_lag()` (line 3725), `active_ingestion_exports()` (line 496, 4th call site outside maintain)
- `src/storage-client/src/storage_collections.rs` — `active_collection_frontiers()` (line 1436), `CollectionFrontiers` struct (line 344), trait definition (line 137)

## Session 9: Vec-Based Dependency Tracking — O(N·M) Retain Scans on Every DROP (2026-03-08)

### Location
`src/catalog/src/memory/objects.rs:642-647` — `CatalogEntry` struct
`src/adapter/src/catalog/apply.rs:1799-1810` — `drop_item()`
`src/adapter/src/catalog/apply.rs:1714-1738` — `insert_entry()`
`src/adapter/src/catalog/consistency.rs:374-460` — `check_object_dependencies()`

### Problem

The `CatalogEntry` stores reverse dependency tracking using **`Vec<CatalogItemId>`** instead of a set:

```rust
// src/catalog/src/memory/objects.rs:639-647
pub struct CatalogEntry {
    pub item: CatalogItem,
    pub referenced_by: Vec<CatalogItemId>,   // O(n) contains, O(n) retain
    // TODO(database-issues#7922)––this should have an invariant tied to it that all
    // dependents (i.e. entries in this field) have IDs greater than this entry's ID.
    pub used_by: Vec<CatalogItemId>,         // O(n) contains, O(n) retain
    ...
}
```

This causes three scaling problems:

**1. O(N·M) work in `drop_item()` (apply.rs:1799-1810)**

When dropping an item, every dependency's `referenced_by` and `used_by` Vecs must be scanned linearly via `.retain()`:

```rust
fn drop_item(&mut self, id: CatalogItemId) -> CatalogEntry {
    let metadata = self.entry_by_id.remove(&id).expect("catalog out of sync");
    for u in metadata.references().items() {           // N references
        if let Some(dep_metadata) = self.entry_by_id.get_mut(u) {
            dep_metadata.referenced_by.retain(|u| *u != metadata.id())  // O(M) scan
        }
    }
    for u in metadata.uses() {                         // N uses
        if let Some(dep_metadata) = self.entry_by_id.get_mut(&u) {
            dep_metadata.used_by.retain(|u| *u != metadata.id())        // O(M) scan
        }
    }
}
```

Total cost: O(N_refs × M_avg_referenced_by + N_uses × M_avg_used_by) per drop.

**2. `uses()` allocates a new BTreeSet on every call (objects.rs:1873-1894)**

The `CatalogItem::uses()` method creates a fresh `BTreeSet` every call:

```rust
pub fn uses(&self) -> BTreeSet<CatalogItemId> {
    let mut uses: BTreeSet<_> = self.references().items().copied().collect();  // ALLOC
    match self {
        CatalogItem::View(view) => uses.extend(view.dependencies.0.iter().copied()),
        CatalogItem::MaterializedView(mview) => uses.extend(mview.dependencies.0.iter().copied()),
        CatalogItem::ContinualTask(ct) => uses.extend(ct.dependencies.0.iter().copied()),
        _ => {}
    }
    uses
}
```

This is called in:
- `drop_item()` (apply.rs:1806) — every DROP
- `insert_entry()` (apply.rs:1724) — every CREATE
- `item_dependents()` (state.rs:622) — recursive dependent traversal for CASCADE drops
- `check_object_dependencies()` (consistency.rs) — every DDL in debug builds

Each call allocates a BTreeSet, copies all reference IDs into it, then potentially extends with dependency IDs. The result is never cached.

**3. O(E · D²) consistency checks (consistency.rs:374-460)**

The consistency check iterates ALL entries and for each entry does `.contains()` on the Vec:

```rust
for (id, entry) in &self.entry_by_id {              // E entries
    for referenced_id in entry.references().items() {  // D_refs per entry
        if !referenced_entry.referenced_by().contains(id) { ... }  // O(M) linear scan
    }
    for used_id in entry.uses() {                      // D_uses per entry (ALLOC!)
        if !used_entry.used_by().contains(id) { ... }             // O(M) linear scan
    }
    for referenced_by in entry.referenced_by() {       // M per entry
        if !referenced_by_entry.references().contains_item(id) { ... }
    }
    for used_by in entry.used_by() {                   // M per entry
        if !used_by_entry.uses().contains(id) { ... }             // ALLOC + O(D) scan
    }
}
```

This runs on every DDL when soft assertions are enabled (ddl.rs:123, ddl.rs:190). With 10,000 entries averaging 5 dependencies each, that's 50,000 `.contains()` calls, each scanning Vecs of average length 5 = 250,000 comparisons. Plus 50,000 `uses()` BTreeSet allocations.

### Scaling Impact

**Scenario: Production catalog with 5,000 objects, a popular base table referenced by 200 materialized views:**

| Operation | Cost | Detail |
|---|---|---|
| DROP one MV referencing 3 tables | 3 × O(200) = 600 comparisons | `.retain()` on each table's `referenced_by` |
| DROP CASCADE on the base table | 200 × `drop_item()` calls | Each doing `.retain()` on their dependencies |
| `uses()` per DROP | BTreeSet alloc + D insertions | For each of 200 items |
| Consistency check (debug) | O(5000 × 5 × 200) = 5M comparisons | Every DDL in test builds |

The `.retain()` calls are particularly expensive because they must scan the entire Vec even when the element is at position 0 — `retain` always visits every element to maintain order.

### Root Cause

The `Vec` was likely chosen for simplicity and because early catalogs had few objects. The existing TODO (database-issues#7922) acknowledges the need for an ordering invariant, but the fundamental problem is the data structure choice. A `Vec` provides:
- O(1) push (amortized) — good for insert
- O(n) retain — bad for delete (hot path)
- O(n) contains — bad for consistency checks

### Suggested Fix

**Option A (minimal — change Vec to BTreeSet):** Replace `Vec<CatalogItemId>` with `BTreeSet<CatalogItemId>` for both `referenced_by` and `used_by`:

```rust
pub referenced_by: BTreeSet<CatalogItemId>,
pub used_by: BTreeSet<CatalogItemId>,
```

This gives O(log n) insert, O(log n) remove, O(log n) contains. The `drop_item()` `.retain()` calls become `.remove()` calls — O(log M) instead of O(M). The consistency `.contains()` calls go from O(M) to O(log M). The only downside is slightly slower iteration (BTreeSet vs Vec), but the dependency lists are typically iterated to do lookups anyway.

**Option B (cache `uses()`):** Store the computed `uses` set as a field on `CatalogEntry` instead of recomputing it every call. It's derived from `references()` + `dependencies` and never changes after creation:

```rust
pub struct CatalogEntry {
    pub item: CatalogItem,
    pub referenced_by: BTreeSet<CatalogItemId>,
    pub used_by: BTreeSet<CatalogItemId>,
    pub uses: BTreeSet<CatalogItemId>,  // computed once at insert time
    ...
}
```

This eliminates all the per-call BTreeSet allocations.

**Option C (IndexMap for O(1) operations):** Use `IndexSet<CatalogItemId>` (from the `indexmap` crate, already used elsewhere in Materialize) for O(1) contains and O(1) swap-remove. This is ideal if ordering doesn't matter.

### Files Involved
- `src/catalog/src/memory/objects.rs` — `CatalogEntry` struct (line 639), `CatalogItem::uses()` (line 1873), `CatalogEntry::uses()` (line 2866)
- `src/adapter/src/catalog/apply.rs` — `drop_item()` (line 1799), `insert_entry()` (line 1703)
- `src/adapter/src/catalog/consistency.rs` — `check_object_dependencies()` (line 374)

## Session 10: Redundant Expression Tree Traversals — 5× Full Plan Walks Per SELECT Query (2026-03-08)

### Location
`src/adapter/src/coord/catalog_serving.rs:44-69` — `auto_run_on_catalog_server()` calls `depends_on()` + `could_run_expensive_function()`
`src/adapter/src/coord/catalog_serving.rs:217-225` — `check_cluster_restrictions()` calls `depends_on()` again
`src/adapter/src/coord/sequencer/inner/peek.rs:330` — `peek_stage_validate()` calls `depends_on()` a third time
`src/adapter/src/frontend_peek.rs:438,451,547` — Frontend peek path has the same 3× `depends_on()` pattern
`src/expr/src/relation.rs:1980-2001` — `could_run_expensive_function()` full tree + scalar traversal
`src/expr/src/relation.rs:1680-1684` — `contains_temporal()` full scalar traversal

### Problem

Every SELECT query triggers **at least 5 independent full traversals** of the same `MirRelationExpr` expression tree before optimization even begins. Each traversal visits every node in the query's MIR plan, and the results are computed fresh each time despite being pure functions of the (immutable) plan.

**Traversal 1 & 2 — `auto_run_on_catalog_server()` (catalog_serving.rs:44-49):**
```rust
Plan::Select(plan) => (
    plan.source.depends_on(),                    // traversal 1: walks entire tree
    plan.source.could_run_expensive_function(),  // traversal 2: walks entire tree + all scalars
),
```

`depends_on()` (relation.rs:2230-2238) recursively visits every `MirRelationExpr` node to collect `GlobalId`s from `Get` nodes. `could_run_expensive_function()` (relation.rs:1980-2001) uses `visit_pre` to traverse the entire expression tree, AND at each node calls `try_visit_scalars` to traverse all scalar sub-expressions.

After computing both results, `auto_run_on_catalog_server` iterates the dependency set to check each against the catalog (lines 162-178), calling `catalog.resolve_item_id()` and `introspection_dependencies()` per dependency.

**Traversal 3 — `check_cluster_restrictions()` (catalog_serving.rs:217-225):**
```rust
Plan::Select(plan) => Box::new(plan.source.depends_on().into_iter()),
```

Computes `depends_on()` on the **same `plan.source`** expression. The result is identical to traversal 1 but is recomputed from scratch.

**Traversal 4 — `peek_stage_validate()` / frontend peek (peek.rs:330 / frontend_peek.rs:547):**
```rust
let source_ids = plan.source.depends_on();
```

Computes `depends_on()` a **third time** on the same expression. This result is then used for timeline validation, log reads checking, and PlanValidity construction.

**Traversal 5 — `contains_temporal()` (peek.rs:335 / frontend_peek.rs:553):**
```rust
if ... && plan.source.contains_temporal()? {
```

Another full traversal of the expression tree's scalar expressions to check for `mz_now()` calls.

### Both Hot Paths Affected

The redundancy exists on **both** the coordinator peek path and the frontend peek path:

**Coordinator path** (sequencer.rs → peek.rs):
```
sequencer.rs:158  → auto_run_on_catalog_server → depends_on() + could_run_expensive_function()
sequencer.rs:182  → check_cluster_restrictions  → depends_on()
peek.rs:330       → peek_stage_validate         → depends_on()
peek.rs:335       → peek_stage_validate         → contains_temporal()
```

**Frontend peek path** (frontend_peek.rs):
```
frontend_peek.rs:438 → auto_run_on_catalog_server → depends_on() + could_run_expensive_function()
frontend_peek.rs:451 → check_cluster_restrictions  → depends_on()
frontend_peek.rs:547 → source_ids =                → depends_on()
frontend_peek.rs:553 → contains_temporal check     → contains_temporal()
```

### Cost Per Traversal

`depends_on()` uses recursive `visit_children` (relation.rs:2237):
```rust
fn depends_on_into(&self, out: &mut BTreeSet<GlobalId>) {
    if let MirRelationExpr::Get { id: Id::Global(id), .. } = self {
        out.insert(*id);
    }
    self.visit_children(|expr| expr.depends_on_into(out))
}
```

For a query with E expression nodes, each traversal is O(E). A moderately complex query joining 5 tables through views may have E = 50-200 nodes.

`could_run_expensive_function()` is even more expensive — it calls `visit_pre` (O(E)) and at each node calls `try_visit_scalars` which traverses all owned scalar expressions. For a query with S total scalar sub-expressions, this is O(E + S).

### Scaling Impact

**Per-query cost:**
- 3× `depends_on()` = 3 × O(E) = O(3E) tree traversals + 3 × BTreeSet allocations
- 1× `could_run_expensive_function()` = O(E + S) tree + scalar traversal
- 1× `contains_temporal()` = O(E) scalar traversal
- **Total: O(5E + S) per query**, when O(E) would suffice

**With realistic numbers:**
- Simple query (E=20, S=10): 5×20 + 10 = 110 node visits (vs 20 needed)
- Complex query with 10 joins (E=200, S=100): 5×200 + 100 = 1,100 node visits (vs 200 needed)
- At 1,000 QPS: 1,100,000 redundant node visits/second for complex queries

**All on the critical path:** These traversals run synchronously before any optimization work begins. On the frontend peek path, they block the connection handler. On the coordinator path, they block the single-threaded event loop.

### Additional Redundancy

Beyond the expression tree traversals, the `depends_on()` result feeds into further redundant work:

1. `auto_run_on_catalog_server` iterates dependencies to check `introspection_dependencies()` per id (lines 170-178)
2. `check_cluster_restrictions` iterates the same dependencies to resolve full names (lines 228-239)
3. `peek_stage_validate` iterates to compute `validate_timeline_context()` — which the TODO at frontend_peek.rs:548-550 explicitly flags as expensive:
   ```
   // TODO(peek-seq): validate_timeline_context can be expensive in real scenarios
   // because it traverses transitive dependencies even of indexed views and
   // materialized views (also traversing their MIR plans).
   ```

### Root Cause

Each function (`auto_run_on_catalog_server`, `check_cluster_restrictions`, `peek_stage_validate`) was written independently and computes what it needs from the plan without any shared pre-computation. There is no "plan metadata" struct that caches derived properties like `depends_on`, `could_run_expensive_function`, or `contains_temporal`. Each function takes `&Plan` and recomputes these properties from scratch.

### Suggested Fix

**Option A (simple — compute once, pass through):** Compute all plan-derived properties once in the sequencer before calling the individual functions:

```rust
// In sequencer.rs, before auto_run_on_catalog_server:
struct PlanMetadata {
    source_ids: BTreeSet<GlobalId>,
    could_run_expensive_function: bool,
    contains_temporal: bool,
}

let metadata = match &plan {
    Plan::Select(plan) => PlanMetadata {
        source_ids: plan.source.depends_on(),
        could_run_expensive_function: plan.source.could_run_expensive_function(),
        contains_temporal: plan.source.contains_temporal().unwrap_or(false),
    },
    // ... other plan types
};
```

Then pass `&metadata` to `auto_run_on_catalog_server`, `check_cluster_restrictions`, and the peek stage. This eliminates 4 of the 5 traversals with minimal API changes.

**Option B (lazy caching on plan):** Add `OnceCell` fields to `SelectPlan` to cache derived properties:

```rust
pub struct SelectPlan {
    pub source: HirRelationExpr,
    // Cached derived properties
    source_ids: OnceCell<BTreeSet<GlobalId>>,
    expensive_function: OnceCell<bool>,
}
```

This is more invasive but automatically deduplicates across any number of call sites.

## Session 11: `get_entry_by_global_id` — Full CatalogEntry Clone + O(N) Linear Scan for a Simple ID Lookup (2026-03-09)

### Location
`src/adapter/src/catalog/state.rs:743-762` — `get_entry_by_global_id()`

### Problem

`get_entry_by_global_id()` clones the entire `CatalogEntry` on every call, even though the vast majority of callers only need the `CatalogItemId` — which the method already had before cloning.

The method works as follows (state.rs:743-762):
```rust
pub fn get_entry_by_global_id(&self, id: &GlobalId) -> CatalogCollectionEntry {
    let item_id = self.entry_by_global_id.get(id)  // Step 1: GlobalId → CatalogItemId (fast)
        .unwrap_or_else(|| panic!("catalog out of sync"));
    let entry = self.get_entry(item_id).clone();     // Step 2: Clone ENTIRE CatalogEntry (expensive!)
    let version = match entry.item() {
        CatalogItem::Table(table) => {
            let (version, _) = table.collections.iter()
                .find(|(_ver, gid)| *gid == id)     // Step 3: O(N) linear scan of versions
                .expect("version to exist");
            RelationVersionSelector::Specific(*version)
        }
        _ => RelationVersionSelector::Latest,
    };
    CatalogCollectionEntry { entry, version }        // Returns owned struct with cloned entry
}
```

**Three compounding problems:**

1. **Full CatalogEntry clone on every call.** `CatalogEntry` (objects.rs:638-653) contains:
   - `item: CatalogItem` — an enum holding `Arc<HirRelationExpr>`, `Arc<OptimizedMirRelationExpr>`, `RelationDesc`, `BTreeMap<RelationVersion, GlobalId>`, create SQL strings, etc.
   - `referenced_by: Vec<CatalogItemId>` — can be large for widely-used tables
   - `used_by: Vec<CatalogItemId>` — can be large for complex views
   - `name: QualifiedItemName`, `privileges: PrivilegeMap`, etc.

2. **O(N) linear scan** of `table.collections` BTreeMap to find the version matching a GlobalId. This should be a reverse index.

3. **The `CatalogItemId` was already known before the clone!** The `entry_by_global_id` map (state.rs:124) is typed `imbl::OrdMap<GlobalId, CatalogItemId>` — so step 1 already has the answer, but the method throws it away inside the clone.

### Scale of Impact

**Direct callers of `get_entry_by_global_id` that only use `.id()`:**
- `state.rs:435` — `self.get_entry_by_global_id(&sink.from).id()` (introspection deps)
- `state.rs:439` — `self.get_entry_by_global_id(&idx.on).id()` (introspection deps)
- `state.rs:1305` — view dependency resolution: `.map(|gid| self.get_entry_by_global_id(&gid).id())` — called per-dependency per-view creation
- `state.rs:1390` — materialized view dependency resolution: same pattern
- `builtin_table_updates.rs:1731` — `.id()` on index updates
- `statement_logging.rs:992` — `.map(|gid| catalog_state.get_entry_by_global_id(&gid).id())` — per-query
- `appends.rs:1063` — `.map(|gid| catalog.get_entry_by_global_id(&gid).id())`

**Via `resolve_item_id` wrapper** (catalog.rs:960-962 which calls `get_entry_by_global_id(id).id()`):
- 34 call sites across 13 files including:
  - `src/sql/src/rbac.rs` — 8 occurrences (RBAC checks, per-query hot path)
  - `src/adapter/src/catalog/timeline.rs` — 5 occurrences (timeline resolution)
  - `src/adapter/src/coord/sequencer/inner.rs` — 4 occurrences
  - `src/adapter/src/coord/sequencer/inner/peek.rs` — 2 occurrences (peek path, highest QPS)
  - `src/adapter/src/coord/sequencer/inner/subscribe.rs` — 2 occurrences
  - `src/adapter/src/coord/introspection.rs` — 2 occurrences

**Total: ~40+ call sites that clone the full CatalogEntry just to extract a CatalogItemId.**

### Quantified Impact

For a SELECT query touching 5 tables with 3 indexes:
- RBAC checks: ~8 resolve_item_id calls
- Timeline resolution: ~5 resolve_item_id calls
- Peek sequencing: ~2 resolve_item_id calls
- Total per query: ~15 full CatalogEntry clones

A `CatalogEntry` for a view with 50 columns, 10 dependencies, and a complex expression tree could be kilobytes. At 1,000 QPS, that's 15,000 unnecessary deep clones per second of structures that contain `Arc` (cheap to clone) but also `Vec<CatalogItemId>` (proportional to dependency count), `PrivilegeMap`, `QualifiedItemName` (string allocations), etc.

### Bug: Typo in Variable Name

Line 755 has `_verison` instead of `_version` — a minor typo but indicative that this code hasn't been closely reviewed.

### Existing Better Alternative Already Exists

The codebase already has `try_get_entry_by_global_id` (state.rs:855-857) which returns `Option<&CatalogEntry>` **without cloning**:
```rust
pub fn try_get_entry_by_global_id(&self, id: &GlobalId) -> Option<&CatalogEntry> {
    let item_id = self.entry_by_global_id.get(id)?;
    self.try_get_entry(item_id)
}
```
This proves the clone is not necessary for the entry itself — it's only done to construct the owned `CatalogCollectionEntry`.

### Root Cause

`CatalogCollectionEntry` was introduced to bundle a `CatalogEntry` with its `RelationVersionSelector` for table versioning (ALTER TABLE schema evolution). However, the struct was made to own its `CatalogEntry`:
```rust
pub struct CatalogCollectionEntry {
    pub entry: CatalogEntry,       // Owned! Forces clone
    pub version: RelationVersionSelector,
}
```
This forces every caller to pay the clone cost even when they only need `.id()`, `.name()`, or other lightweight fields.

### Suggested Fix

**Option A (surgical — fix the ID-only callers):** Add a `resolve_item_id` method directly on `CatalogState` that uses the map without cloning:
```rust
pub fn resolve_item_id(&self, id: &GlobalId) -> CatalogItemId {
    *self.entry_by_global_id
        .get(id)
        .unwrap_or_else(|| panic!("catalog out of sync, missing id {id:?}"))
}
```
This is a single O(log N) map lookup — no entry fetch, no clone, no linear scan. Replace all 40+ `.get_entry_by_global_id(&gid).id()` / `resolve_item_id` call sites.

**Option B (structural — fix CatalogCollectionEntry):** Change `CatalogCollectionEntry` to borrow:
```rust
pub struct CatalogCollectionEntry<'a> {
    pub entry: &'a CatalogEntry,
    pub version: RelationVersionSelector,
}
```
This eliminates the clone for all callers, not just ID-only ones. Requires lifetime propagation but the entry always comes from `self.entry_by_id` which outlives all callers.

**Option C (fix the linear scan):** Add a reverse index `GlobalId → RelationVersion` to avoid the O(N) scan in `table.collections`. This can be a `BTreeMap<GlobalId, RelationVersion>` stored on `Table` or on `CatalogState` alongside `entry_by_global_id`.

Options A and B are complementary — A gives immediate wins for the 40+ ID-only callers, B eliminates clones for the remaining callers that need more than the ID.

**Option C (for catalog_serving specifically):** Merge `auto_run_on_catalog_server` and `check_cluster_restrictions` into a single function that computes `depends_on()` once and performs both checks. These are always called sequentially on the same plan.

### Files Involved
- `src/adapter/src/coord/catalog_serving.rs` — `auto_run_on_catalog_server()` (line 39), `check_cluster_restrictions()` (line 200)
- `src/adapter/src/coord/sequencer.rs` — call sites (lines 158, 182)
- `src/adapter/src/coord/sequencer/inner/peek.rs` — `peek_stage_validate()` (line 330)
- `src/adapter/src/frontend_peek.rs` — frontend peek path (lines 438, 451, 547)
- `src/expr/src/relation.rs` — `depends_on_into()` (line 2230), `could_run_expensive_function()` (line 1980), `contains_temporal()` (line 1680)
- `src/adapter/src/catalog/state.rs` — `item_dependents()` (line 610), `transitive_uses()` (line 384)

## Session 12: DataflowDescription `objects_to_build` Vec — O(N²) Linear Scans on Every Dataflow Import and Planning Pass (2026-03-09)

### Location
`src/compute-types/src/dataflows.rs` — `DataflowDescription` struct (line 31) and its lookup methods (lines 205-231, 401-408, 435-474)
`src/adapter/src/optimize/dataflows.rs` — `import_into_dataflow()` (line 310) and `import_view_into_dataflow()` (line 399)
`src/compute-types/src/plan.rs` — `finalize_dataflow()` (line 479) and 4 refinement passes (lines 558-715)

### Problem

`DataflowDescription` stores `objects_to_build` as a `Vec<BuildDesc<P>>` (line 40), but multiple methods perform **O(N) linear scans** over this Vec to look up items by `GlobalId`. This Vec is treated as a map but lacks indexing:

**1. `is_imported()` — 3 linear scans per call (lines 205-210):**
```rust
pub fn is_imported(&self, id: &GlobalId) -> bool {
    self.objects_to_build.iter().any(|bd| &bd.id == id)     // O(P) scan
        || self.index_imports.keys().any(|i| i == id)        // O(I) scan (BTreeMap — already O(log I))
        || self.source_imports.keys().any(|i| i == id)       // O(S) scan (BTreeMap — already O(log S))
}
```
Note: `index_imports` and `source_imports` are `BTreeMap<GlobalId, _>`, so these `.keys().any()` calls waste the map's O(log N) lookup by doing linear iteration instead of `.contains_key()`. But the real problem is the `objects_to_build` linear scan.

**Called from `import_into_dataflow()` (line 318)** which is recursive — each import call checks `is_imported`, and each import can trigger further imports via `import_view_into_dataflow`. For a dataflow importing V views, this is **O(V × P)** where P = current size of `objects_to_build`, growing toward **O(V²)** as the Vec grows.

**2. `arity_of()` — 3 sequential linear scans (lines 213-231):**
```rust
pub fn arity_of(&self, id: &GlobalId) -> usize {
    for (source_id, ..) in self.source_imports.iter() { .. }    // O(S) linear scan
    for IndexImport { desc, .. } in self.index_imports.values() { .. }  // O(I) linear scan
    for desc in self.objects_to_build.iter() { .. }              // O(P) linear scan
    panic!("GlobalId {} not found", id);
}
```
Called from `src/transform/src/dataflow.rs:277,286` during transform analysis.

**3. `build_desc()` — linear scan + extra iteration to verify uniqueness (lines 401-408):**
```rust
pub fn build_desc(&self, id: GlobalId) -> &BuildDesc<P> {
    let mut builds = self.objects_to_build.iter().filter(|build| build.id == id);
    let build = builds.next().unwrap_or_else(|| panic!("..."));
    assert!(builds.next().is_none());  // Continues iteration to check no duplicates
    build
}
```
Called from `depends_on_into()` (line 469) which itself recursively traverses dependencies. For a dependency chain of depth D with P objects, this is **O(D × P)** per call.

**4. Four sequential full-tree refinement passes in `finalize_dataflow()` (lines 487-521):**
After lowering MIR to LIR, the code makes 4 separate passes over all `objects_to_build`, each walking every plan tree:
- `refine_source_mfps()` (line 487) — **O(S × P × T)** nested loop: for each source, walks all objects' plan trees
- `refine_union_negate_consolidation()` (line 490) — O(P × T)
- `refine_single_time_operator_selection()` (line 494) — O(P × T)
- `refine_single_time_consolidation()` (line 521) — O(P × T)

Where S = source count, P = objects count, T = average plan tree size. The `refine_source_mfps` pass is particularly bad: it does a full nested loop (line 561-585) iterating all objects for each source, giving **O(S × P × T)** instead of the O(P × T) achievable with a single pass that collects source references.

### Scaling Impact

- **Dataflow import path**: Every SELECT on unindexed views triggers `import_into_dataflow`, which recursively imports dependencies. Each import checks `is_imported()` with a linear scan. With V views imported, cost is O(V²) — problematic for complex queries referencing many views.
- **Dependency resolution**: `depends_on()` calls `build_desc()` for each dependency, each doing O(P) scans. Deep view hierarchies compound this.
- **Plan finalization**: Every dataflow goes through 4 passes, and the source MFP pushdown has quadratic behavior. For a one-shot SELECT (single-time dataflow), all 4 passes execute. With many sources and objects, this becomes a bottleneck.
- **Index/source lookups waste BTreeMap**: `is_imported` and `arity_of` iterate `BTreeMap` keys linearly instead of using `contains_key()`/`get()`, losing O(log N) → O(N).

### Root Cause

The `objects_to_build` field is a `Vec` because order matters (objects must be built in dependency order, per the comment on line 39). But the code frequently needs random access by `GlobalId`, which a Vec cannot provide efficiently.

### Suggested Fix

1. **Add a parallel index**: Maintain a `BTreeMap<GlobalId, usize>` or `BTreeMap<GlobalId, ()>` alongside `objects_to_build` for O(log N) lookups by ID. The Vec preserves build order; the map provides fast membership/position lookups.
2. **Fix BTreeMap misuse**: Change `is_imported` and `arity_of` to use `.contains_key()` / `.get()` for `source_imports` and `index_imports` instead of `.keys().any()` / `.iter()`.
3. **Merge refinement passes**: Combine the 4 plan refinement passes into a single traversal with a visitor that applies all transformations in one walk.
4. **Invert `refine_source_mfps`**: Instead of nested S × P loop, do one pass over all objects collecting Get references by source ID, then update sources.

### Files Involved
- `src/compute-types/src/dataflows.rs` — `DataflowDescription` struct and `is_imported`, `arity_of`, `build_desc`, `depends_on_into`
- `src/adapter/src/optimize/dataflows.rs` — `import_into_dataflow` (line 310), `import_view_into_dataflow` (line 399)
- `src/compute-types/src/plan.rs` — `finalize_dataflow` (line 479), `refine_source_mfps` (line 558), `refine_union_negate_consolidation` (line 616), `refine_single_time_operator_selection` (line 650), `refine_single_time_consolidation` (line 699)
- `src/transform/src/dataflow.rs` — `arity_of` callers (lines 277, 286)

## Session 13: Optimizer Fixpoint Loop — Full Tree Clone Per Iteration for Change Detection (2026-03-09)

### Location
`src/transform/src/lib.rs:443-530` — `Fixpoint::actually_perform_transform()`
`src/transform/src/lib.rs:217-241` — `Transform::transform()` wrapper (per-transform hash overhead)
`src/transform/src/lib.rs:666-701` — nested fixpoints (`fuse_and_collapse_fixpoint`, `fold_constants_fixpoint`)
`src/transform/src/dataflow.rs:46-94` — `optimize_dataflow()` orchestrates 3 major optimization passes

### Problem

The optimizer's `Fixpoint` loop clones the **entire MIR expression tree** on every iteration, solely to detect whether the transforms made any changes. This happens across **8+ fixpoints per query** (some nested), producing **~60 full tree clones per query** for change detection alone. Additionally, a per-transform wrapper computes **2 full-tree hash traversals per transform application** for metrics.

**The Fixpoint inner loop (lib.rs:461-511):**

```rust
for i in iter_no..iter_no + self.limit {      // limit = 100
    let prev = relation.clone();               // FULL TREE CLONE — O(N) with allocation
    self.apply_transforms(relation, ctx, ...)?;// runs K transforms, each with 2 hashes
    if *relation == prev {                     // FULL TREE EQUALITY — O(N)
        return Ok(());                         // converged
    }
    let seen_i = seen.insert(relation.hash_to_u64(), i);  // FULL TREE HASH — O(N)
    if let Some(seen_i) = seen_i {
        // Collision detection: clone original, re-run transforms from scratch
        let mut again = original.clone();      // ANOTHER FULL TREE CLONE
        for j in 0..(seen_i + 2) {
            self.apply_transforms(&mut again, ctx, ...)?;  // re-run transforms
        }
    }
}
```

**The per-transform wrapper (lib.rs:217-241):**

```rust
fn transform(&self, relation: &mut MirRelationExpr, args: &mut TransformCtx) -> Result<...> {
    let hash_before = args.last_hash.get(&id).copied()
        .unwrap_or_else(|| relation.hash_to_u64());        // HASH #1 — O(N)
    soft_assert_eq_no_log!(hash_before, relation.hash_to_u64(), ...);  // HASH #2 in debug
    let res = self.actually_perform_transform(relation, args);
    let hash_after = args.update_last_hash(relation);       // HASH #3 — O(N)
    // ...metrics...
}
```

### Per-Iteration Cost Breakdown

For a single Fixpoint iteration with K transforms and tree size N:

| Operation | Count | Cost Each | Total |
|-----------|-------|-----------|-------|
| Full tree clone (line 462) | 1 | O(N) + alloc | O(N) |
| Full tree equality (line 464) | 1 | O(N) | O(N) |
| Full tree hash (line 471) | 1 | O(N) | O(N) |
| Per-transform hash-before (line 222) | K | O(N) | O(K·N) |
| Per-transform hash-after (line 233) | K | O(N) | O(K·N) |
| **Total per iteration** | | | **O((2K+3)·N)** |

### Fixpoints in the Full Pipeline

Every non-fast-path SELECT goes through `optimize_dataflow` which runs 3 optimizer passes, each containing nested fixpoints:

**logical_optimizer** (lines 737-793):
1. `normalize` — Fixpoint(limit=100, 2 transforms)
2. `fuse_and_collapse_fixpoint` — Fixpoint(limit=100, 12 transforms)
   - **Contains nested** `fold_constants_fixpoint` — Fixpoint(limit=100, 3 transforms)
3. `fixpoint_logical_01` — Fixpoint(limit=100, 4 transforms)
4. `fixpoint_logical_02` — Fixpoint(limit=100, 7 transforms)
   - **Contains nested** `FuseAndCollapse` which contains `fold_constants_fixpoint`

**logical_cleanup_pass** (lines 904-937):
5. `fixpoint_logical_cleanup_pass_01` — Fixpoint(limit=100, 10 transforms)
   - **Contains nested** `fold_constants_fixpoint`

**physical_optimizer** (lines 806-883):
6. `fixpoint_physical_01` — Fixpoint(limit=100, 5 transforms)
   - **Contains nested** `fold_constants_fixpoint`
7. `fixpoint_join_impl` — Fixpoint(limit=100, 1 transform)
8. Final `fold_constants_fixpoint` — Fixpoint(limit=100, 3 transforms)

### Total Per-Query Cost

Assuming typical convergence in 3 iterations per fixpoint (typical for most queries):

| Fixpoint | Transforms | Iterations | Clones | Hashes |
|----------|-----------|------------|--------|--------|
| normalize | 2 | 3 | 3 | 3×(2×2+1)=15 |
| fuse_and_collapse | 12 | 3 | 3 | 3×(2×12+1)=75 |
| └─ fold_constants (nested, ×3) | 3 | 3×3=9 | 9 | 9×(2×3+1)=63 |
| fixpoint_logical_01 | 4 | 3 | 3 | 3×(2×4+1)=27 |
| fixpoint_logical_02 | 7 | 3 | 3 | 3×(2×7+1)=45 |
| └─ fold_constants (nested, ×3) | 3 | 9 | 9 | 63 |
| cleanup_pass | 10 | 3 | 3 | 3×(2×10+1)=63 |
| └─ fold_constants (nested, ×3) | 3 | 9 | 9 | 63 |
| fixpoint_physical_01 | 5 | 3 | 3 | 3×(2×5+1)=33 |
| └─ fold_constants (nested, ×3) | 3 | 9 | 9 | 63 |
| fixpoint_join_impl | 1 | 3 | 3 | 3×(2×1+1)=9 |
| fold_constants (final) | 3 | 3 | 3 | 3×(2×3+1)=21 |
| **Total** | | | **~60 clones** | **~540 hashes** |

**Per-query total: ~60 tree clones + ~540 tree hash traversals = ~600 full O(N) tree operations.**

For a 1,000-node expression: **600,000 node operations** just for change detection
For a 10,000-node expression: **6,000,000 node operations** just for change detection

### Root Cause

The Fixpoint loop uses **full clone + deep equality** for change detection (lines 462-464). This was likely chosen for correctness — equality comparison is exact, while hash comparison could have false positives. However:

1. The hash is **already computed** at line 471 after every iteration for cycle detection
2. The hash before the iteration is implicitly known (it's the hash from the previous iteration, stored in `seen`)
3. If `hash_before == hash_after`, the expression hasn't changed (false positive rate: ~2^-64 for u64 hash, essentially zero)
4. False positive for convergence is harmless — it only means we stop optimizing slightly early; the result is still correct

The clone on line 462 is therefore **entirely redundant** with the hash-based convergence check that could be done using data already computed.

Additionally, the per-transform wrapper (lines 217-241) computes hashes before and after purely for metrics. While the "after" hash is cached in `last_hash` (line 205), the cost is still 2 full tree traversals per transform application.

### Suggested Fix

**Option A (eliminate clone — highest impact):** Replace the clone+equality convergence check with hash comparison:

```rust
// Before (current):
let prev = relation.clone();                    // O(N) clone
self.apply_transforms(relation, ctx, ...)?;
if *relation == prev { return Ok(()); }         // O(N) equality
let new_hash = relation.hash_to_u64();          // O(N) hash (already computed)
seen.insert(new_hash, i);

// After (proposed):
let prev_hash = last_hash;                      // O(1) — stored from previous iteration
self.apply_transforms(relation, ctx, ...)?;
let new_hash = relation.hash_to_u64();          // O(N) — still needed for cycle detection
if new_hash == prev_hash { return Ok(()); }     // O(1) comparison
seen.insert(new_hash, i);
```

This eliminates **~60 full tree clones per query** — the most expensive per-query operation. The `original.clone()` at line 458 can also be eliminated if we store the original hash instead (collision detection can re-hash rather than re-clone+re-run).

**Option B (reduce hash overhead):** The per-transform wrapper computes `hash_before` at line 222-225 and `hash_after` at line 233. If hash caching (`last_hash`) is reliable, skip the `hash_before` computation when a cached value exists (it currently does this, but with a `soft_assert_eq_no_log` verification that computes ANOTHER hash in debug mode).

**Option C (dirty-flag transforms):** Have each transform return a `bool` indicating whether it made changes, instead of relying on post-hoc change detection. Many transforms already track this internally (e.g., they have local `changed` flags). This would eliminate both the clone AND the hash for convergence detection.

**Option D (reduce fixpoint nesting):** `fold_constants_fixpoint` is nested inside 4 outer fixpoints. Since FoldConstants + NormalizeLets + ReduceScalars rarely need more than 1-2 inner iterations, reduce the inner limit from 100 to 10 or inline the fold_constants transforms directly into the outer fixpoint.

### Files Involved
- `src/transform/src/lib.rs` — `Fixpoint::actually_perform_transform()` (line 443), `Transform::transform()` wrapper (line 217), `Fixpoint` struct (line 406), `fuse_and_collapse_fixpoint()` (line 666), `fold_constants_fixpoint()` (line 686), `normalize()` (line 711), `logical_optimizer()` (line 736), `logical_cleanup_pass()` (line 897), `physical_optimizer()` (line 806), `FuseAndCollapse` (line 601)
- `src/transform/src/dataflow.rs` — `optimize_dataflow()` (line 46), `optimize_dataflow_relations()` (line 228)
- `src/expr/src/relation.rs` — `hash_to_u64()` (line ~2005), `size()` (recursive tree size computation)

## Session 14: Double Timestamp Determination — Full Read Hold Acquire+Release Purely for a Metric (2026-03-09)

### Location
`src/adapter/src/coord/timestamp_selection.rs:943-973` — inside `determine_timestamp()`

### Problem

Every strict serializable query that doesn't respond immediately triggers a **complete second call** to `determine_timestamp_for()` — including full read hold acquisition, frontier queries, and immediate read hold release — purely to record a single histogram metric measuring the timestamp difference between StrictSerializable and Serializable modes.

**The metrics-only code path (timestamp_selection.rs:943-973):**

```rust
pub(crate) fn determine_timestamp(...) -> Result<...> {
    // ... first call (the real one) ...
    let (det, read_holds) = self.determine_timestamp_for(
        session, id_bundle, when, compute_instance,
        timeline_context, oracle_read_ts, real_time_recency_ts,
        isolation_level, &constraint_based,
    )?;   // ← acquires read holds, queries frontiers, determines timestamp

    // THE PROBLEM: second call purely for metrics
    if !det.respond_immediately()                              // query will block
        && isolation_level == &IsolationLevel::StrictSerializable  // the DEFAULT
        && real_time_recency_ts.is_none()                      // the common case
    {
        if let Some(strict) = det.timestamp_context.timestamp() {
            let (serializable_det, _tmp_read_holds) = self.determine_timestamp_for(
                session, id_bundle, when, compute_instance,
                timeline_context, oracle_read_ts, real_time_recency_ts,
                &IsolationLevel::Serializable,   // ← re-runs with Serializable
                &constraint_based,
            )?;   // ← AGAIN: acquires read holds, queries frontiers, determines timestamp
                  // _tmp_read_holds immediately dropped → releases all holds

            if let Some(serializable) = serializable_det.timestamp_context.timestamp() {
                self.metrics
                    .timestamp_difference_for_strict_serializable_ms
                    .observe(...);  // ← this single metric observation is the sole purpose
            }
        }
    }
    Ok((det, read_holds))
}
```

### What the Second Call Does

`determine_timestamp_for()` (line 605-636) is not a cheap function:

```rust
fn determine_timestamp_for(&self, ...) -> Result<(TimestampDetermination, ReadHolds), ...> {
    let read_holds = self.acquire_read_holds(id_bundle);   // EXPENSIVE — Step 1
    let upper = self.least_valid_write(id_bundle);          // EXPENSIVE — Step 2
    Self::determine_timestamp_for_inner(...)                 // Step 3
}
```

**Step 1 — `acquire_read_holds()` (read_policy.rs:378-408):**
For each storage collection in the bundle:
- Acquires the shared `collections` mutex lock (contended with ingestion threads)
- Iterates all storage IDs, reads their `read_capabilities` frontier, clones each `since` Antichain
- Creates `ChangeBatch` capability updates
- Calls `update_read_capabilities_inner()` which processes the capability changes
- Creates `ReadHold` objects with channel senders

For each compute collection:
- Calls `compute.acquire_read_hold(instance_id, id)` — per-collection lock + capability change

**Step 2 — `least_valid_write()` (timestamp_selection.rs:811-828):**
- Calls `storage_frontiers(storage_ids)` → `storage.collections_frontiers(ids)` — acquires the same `collections` mutex AGAIN, iterates all storage IDs, clones 3 Antichain values per collection
- For each compute collection, calls `compute_write_frontier()` — per-collection

**Step 3 — `determine_timestamp_for_inner()`:** Pure computation, relatively cheap.

**Then `_tmp_read_holds` is dropped:** Each `ReadHold` sends a release message through its channel (read_holds.rs:146-154), triggering capability downgrade processing on the receiver end. Storage holds send via `holds_tx` channel; compute holds update capability state.

### Total Redundant Work Per Query

For a query touching S storage collections and C compute collections:

| Operation | Cost | Detail |
|-----------|------|--------|
| Mutex lock #1 (acquire_read_holds) | 1 lock | Contended with storage ingestion |
| Storage capability changes (acquire) | S changes | ChangeBatch creation + update_read_capabilities_inner |
| Compute read hold acquisitions | C per-collection | Lock + capability update each |
| Mutex lock #2 (least_valid_write) | 1 lock | Same mutex as #1, reacquired |
| Storage Antichain clones | 3×S clones | write_frontier + implied_capability + read_capabilities |
| Compute write frontier queries | C queries | Per-collection |
| Storage capability changes (release) | S changes | ReadHold::drop sends via channel |
| Compute capability changes (release) | C changes | ReadHold::drop sends via channel |
| Vec allocations | 3+ | For storage_ids, read_holds, frontiers |

### When This Triggers

The condition `!det.respond_immediately() && StrictSerializable && no real_time_recency` fires when:

1. **StrictSerializable** is the **default** isolation level (definitions.rs:427: `value!(IsolationLevel; IsolationLevel::StrictSerializable)`)
2. `!respond_immediately()` means `upper.less_equal(chosen_ts)` — the chosen timestamp is at or beyond the write frontier. This is the **common case** for:
   - Tables with infrequent writes (upper lags behind oracle timestamp)
   - Sources with any ingestion lag
   - Recently created collections whose upper hasn't fully caught up
   - Any query shortly after a write (upper hasn't advanced yet)
3. `real_time_recency_ts.is_none()` is the common case (real-time recency is an opt-in feature)

In production, a substantial fraction of strict serializable queries hit this path — potentially 30-50% or more depending on write patterns.

### Scaling Impact

With S = 10 storage collections and C = 5 compute collections per query:
- **2 mutex lock acquisitions** on the shared `collections` lock (blocking storage ingestion)
- **30 Antichain clones** (heap allocations)
- **30 capability change messages** (15 acquire + 15 release)
- **15 per-collection controller lookups**

At 1,000 QPS with 40% triggering the metric path:
- **800 extra mutex acquisitions/second** on the hot collections lock
- **12,000 unnecessary Antichain clones/second**
- **12,000 capability change messages/second** (purely to acquire and immediately release read holds)

All on the **single-threaded coordinator**, blocking other query processing. The mutex contention with storage ingestion threads is particularly harmful — holding the lock for O(S) operations per extra call delays frontier updates.

### Root Cause

The metric `timestamp_difference_for_strict_serializable_ms` was added to measure how much latency StrictSerializable adds compared to Serializable. While this is valuable for understanding the isolation level tradeoff, it's implemented by re-running the entire timestamp determination pipeline (including expensive read hold management) rather than computing the difference cheaply from already-available data.

The information needed for the metric — "what timestamp would Serializable have chosen?" — is entirely determined by the `read_holds`, `upper`, and session state that are **already computed** in the first call. The second call to `determine_timestamp_for` recollects this same data from scratch.

### Suggested Fix

**Option A (eliminate second call — highest impact):** Compute the Serializable timestamp directly using the already-available data from the first call. Since `determine_timestamp_for_inner` is a pure function of its inputs, just call it again with the existing `read_holds` and `upper`:

```rust
if !det.respond_immediately() && isolation_level == &IsolationLevel::StrictSerializable && ... {
    if let Some(strict) = det.timestamp_context.timestamp() {
        // Reuse the read_holds and upper from the first call — NO new acquisition needed
        let serializable_det = Self::determine_timestamp_for_inner(
            session, id_bundle, when, compute_instance,
            timeline_context, oracle_read_ts, real_time_recency_ts,
            &IsolationLevel::Serializable,
            &constraint_based,
            read_holds.clone(),   // cheap: just clone the ReadHolds struct (not re-acquire)
            upper.clone(),        // already computed
        );
        // ... observe metric ...
    }
}
```

Wait — cloning `ReadHolds` would also acquire new holds (the `Clone` impl sends capability changes). Instead:

**Option A' (best):** Extract the pure timestamp computation into a separate function that takes `&ReadHolds` and `&Antichain<Timestamp>` by reference. Call it with `&IsolationLevel::Serializable` using the existing data. No read hold acquisition, no frontier queries, no mutex locks. This is a ~5 line refactor.

**Option B (feature-flag the metric):** Gate the metric behind a dyncfg flag that defaults to `false`. Enable it only when actively debugging StrictSerializable latency. This eliminates the overhead entirely for the 99% of the time when the metric isn't being actively monitored.

**Option C (use the already-known upper):** Since `upper` is already computed in the first call (line 621), and the Serializable timestamp is simply `largest_not_in_advance_of_upper` (the Serializable path uses `candidate.join_assign(&largest_not_in_advance_of_upper)` at line 327), the metric can be computed as:

```rust
let serializable_ts = Coordinator::largest_not_in_advance_of_upper(&upper);
let diff = strict.saturating_sub(serializable_ts);
self.metrics.timestamp_difference_for_strict_serializable_ms.observe(f64::cast_lossy(u64::from(diff)));
```

This is O(1) with no controller interaction at all — just arithmetic on values already in scope.

### Files Involved
- `src/adapter/src/coord/timestamp_selection.rs` — `determine_timestamp()` (line 898), `determine_timestamp_for()` (line 605), `determine_timestamp_for_inner()` (line 639), `determine_timestamp_classical()` (line 248), `least_valid_write()` (line 811)
- `src/adapter/src/coord/read_policy.rs` — `acquire_read_holds()` (line 378)
- `src/storage-client/src/storage_collections.rs` — `acquire_read_holds()` (line 2321), `collections_frontiers()` (line 1408) — both acquire the `collections` mutex
- `src/storage-types/src/read_holds.rs` — `ReadHold::drop()` (line 197), `ReadHold::release()` (line 161), `ReadHold::try_downgrade()` (line 135) — sends capability change on channel
- `src/sql/src/session/vars/definitions.rs` — `TRANSACTION_ISOLATION` default is `StrictSerializable` (line 427)

---

## Session 15: Compute Controller `send()` — O(R) Deep Clones of Entire Dataflow Plans Per Command (2026-03-09)

### Location
`src/compute-client/src/controller/instance.rs:1017-1026` — `send()`
`src/compute-client/src/controller/instance.rs:1060-1068` — replica hydration history replay
`src/compute-client/src/controller/instance.rs:1228-1246` — per-export dependency cloning
`src/compute-client/src/protocol/history.rs:27-38` — `ComputeCommandHistory`

### Problem

The compute controller's `send()` method deep-clones every `ComputeCommand` **R+1 times** — once for the command history and once per active replica:

```rust
// instance.rs:1017-1026
fn send(&mut self, cmd: ComputeCommand<T>) {
    // Record the command so that new replicas can be brought up to speed.
    self.history.push(cmd.clone());          // Clone #1: history

    // Clone the command for each active replica.
    for replica in self.replicas.values_mut() {
        let _ = replica.client.send(cmd.clone());  // Clone #2..R+1: per replica
    }
}
```

For `CreateDataflow` commands, each clone deep-copies an entire `DataflowDescription<RenderPlan<T>, CollectionMetadata, T>` which contains:
- `source_imports: BTreeMap<GlobalId, SourceImport<CollectionMetadata, T>>` — all source imports with storage metadata
- `index_imports: BTreeMap<GlobalId, IndexImport>` — all index imports
- `objects_to_build: Vec<BuildDesc<RenderPlan<T>>>` — **the full render plan tree** for every object in the dataflow
- `index_exports: BTreeMap<GlobalId, (IndexDesc, ReprRelationType)>` — relation types
- `sink_exports: BTreeMap<GlobalId, ComputeSinkDesc<CollectionMetadata, T>>` — sink descriptions with metadata
- Various antichains, schedules, strings, etc.

`RenderPlan` is a deeply nested recursive tree representing the entire dataflow execution plan. For complex queries involving joins, aggregates, and filters, this tree can be very large.

**This happens on three paths:**

**Path 1 — Normal command sending (line 1017):** Every `CreateDataflow`, `Peek`, `AllowCompaction`, `Schedule`, `AllowWrites`, and `CancelPeek` goes through `send()`. While small commands like `AllowCompaction` are cheap to clone, `CreateDataflow` and `Peek` (which contains `Row` literal values and a `RowSetFinishing`) are expensive.

**Path 2 — Replica hydration (line 1060-1068):** When a new replica is added (scaling up or replacing a failed replica), the entire command history is replayed with each command cloned:

```rust
// instance.rs:1060-1068
for command in self.history.iter() {
    if client.send(command.clone()).is_err() {   // Clone EVERY historical command
        tracing::warn!("Replica connection terminated during hydration");
        break;
    }
}
```

If history contains D dataflows, each with their full `RenderPlan`, hydrating one replica clones D large plans. The history `reduce()` method (history.rs:85) does compact away completed dataflows, but active dataflows remain.

**Path 3 — Per-export dependency map cloning (line 1228-1246):** During `create_dataflow()`, for EACH export, the dependency maps and read hold vectors are cloned:

```rust
// instance.rs:1228-1246
for export_id in dataflow.export_ids() {
    self.add_collection(
        export_id,
        as_of.clone(),
        shared,
        storage_dependencies.clone(),      // BTreeMap<GlobalId, ReadHold> cloned per export
        compute_dependencies.clone(),      // BTreeMap<GlobalId, ReadHold> cloned per export
        replica_input_read_holds.clone(),  // Vec<ReadHold> cloned per export
        write_only,
        storage_sink,
        dataflow.initial_storage_as_of.clone(),
        dataflow.refresh_schedule.clone(),
    );
}
```

Each `ReadHold::clone()` acquires a new capability hold (sends a message on a channel), so this isn't just memory — it's R×E channel sends for R read holds and E exports. Then inside `add_collection()` (line 307-310), `replica_input_read_holds` is cloned **again per replica**:

```rust
// instance.rs:307-310
for replica in self.replicas.values_mut() {
    replica.add_collection(id, as_of.clone(), replica_input_read_holds.clone());
}
```

### Scaling Impact

**Per CreateDataflow command:**
- Deep clones: R+1 (where R = number of replicas in the cluster)
- Each clone includes the full RenderPlan tree
- With 10 replicas (common for high-availability production clusters), that's 11 deep clones of every dataflow plan

**Per replica hydration:**
- Deep clones: D (where D = number of active dataflows in history)
- A cluster with 500 active materialized views replays 500 full dataflow plans per replica add
- If scaling from 5 to 10 replicas, that's 5 × 500 = 2,500 plan clones

**Per-export dependency cloning:**
- For a dataflow with E exports, I source imports, and R replicas:
  - `storage_dependencies` cloned E times: O(E × I)
  - `compute_dependencies` cloned E times: O(E × I)
  - `replica_input_read_holds` cloned E times: O(E × I)
  - Then inside add_collection, replica_input_read_holds cloned E × R times: O(E × R × I)
  - Each ReadHold clone sends a channel message (capability tracking)

**Concrete example — 500 MVs on a 10-replica cluster:**
- 500 CreateDataflow commands × 11 clones each = 5,500 deep plan copies
- Each plan contains the full RenderPlan tree with joins, aggregates, etc.
- Adding one more replica replays all 500 commands = 500 additional deep copies
- All on the single-threaded coordinator

### Root Cause

`ComputeCommand` derives `Clone` and is passed by value. The `send()` method takes ownership and must produce copies for each destination. There is no shared-ownership mechanism (like `Arc`) to avoid the deep copies.

The per-export dependency cloning exists because each export needs independent read capabilities. However, the dependency *maps* (which keys/values map to which holds) could be shared, with only the read hold capabilities being independently tracked.

### Suggested Fix

**Option A (Arc-wrap commands — highest impact):** Wrap large command payloads in `Arc`:

```rust
// Change CreateDataflow to hold Arc'd payload
CreateDataflow(Arc<Box<DataflowDescription<RenderPlan<T>, CollectionMetadata, T>>>),

fn send(&mut self, cmd: ComputeCommand<T>) {
    self.history.push(cmd.clone());  // Now just Arc::clone — O(1)
    for replica in self.replicas.values_mut() {
        let _ = replica.client.send(cmd.clone());  // Arc::clone — O(1)
    }
}
```

This works because commands are immutable after creation — replicas only read them, never modify. The only mutation happens in `history.reduce()` which already takes ownership of commands via `drain()`.

**Option B (share dependency maps across exports):** Instead of cloning `storage_dependencies` and `compute_dependencies` per-export, use `Arc<BTreeMap<GlobalId, ReadHold<T>>>` shared across all exports of the same dataflow. Each export doesn't independently downgrade dependencies — they all track the same dataflow's compaction frontier.

**Option C (batch replica sends):** Instead of cloning per replica, serialize the command once and send the serialized bytes to each replica. Since commands are serialized for transport anyway, this avoids the intermediate clone step entirely.

### Files Involved
- `src/compute-client/src/controller/instance.rs` — `send()` (line 1017), `add_replica()` (line 1060, hydration), `create_dataflow()` (line 1166), `add_collection()` (line 271, per-replica cloning)
- `src/compute-client/src/protocol/command.rs` — `ComputeCommand` enum (line 38), `CreateDataflow` variant (line 144), derives `Clone`
- `src/compute-client/src/protocol/history.rs` — `ComputeCommandHistory` (line 27), `push()` (line 59), `reduce()` (line 85), `iter()` (used for hydration replay)
- `src/compute-types/src/dataflows.rs` — `DataflowDescription` struct (line 31), `BuildDesc` (line 627) — the large structures being cloned
- `src/compute-types/src/plan/render_plan.rs` — `RenderPlan` — the deeply nested execution plan tree

## Session 16: Global Timeline ReadHold Downgrade — O(N) Channel Messages Per Group Commit (2026-03-09)

### Location
`src/adapter/src/coord/timeline.rs:299-338` — `advance_timelines()`
`src/adapter/src/coord/read_policy.rs:87-95` — `ReadHolds::downgrade()`
`src/storage-types/src/read_holds.rs:135-158` — `ReadHold::try_downgrade()`
`src/storage-types/src/read_holds.rs:167-194` — `ReadHold::clone()`

### Problem

Every call to `advance_timelines()` performs O(N) channel sends where N is the total number of collections across all timelines. This is triggered:
1. **After every group commit** via `Message::AdvanceTimelines` (appends.rs:613)
2. **Every ~1 second** via `advance_timelines_interval` tick (coord.rs message loop)

The flow is:

```rust
// coord/timeline.rs:299-338
pub(crate) async fn advance_timelines(&mut self) {
    let global_timelines = std::mem::take(&mut self.global_timelines);
    for (timeline, TimelineState { oracle, mut read_holds }) in global_timelines {
        // ... timeline-specific oracle advancement ...

        // THIS runs for ALL timelines including EpochMilliseconds:
        let read_ts = oracle.read_ts().await;
        read_holds.downgrade(read_ts);  // <-- O(N) channel messages!
    }
}

// coord/read_policy.rs:87-95
pub fn downgrade(&mut self, time: T) {
    let frontier = Antichain::from_elem(time);
    for hold in self.storage_holds.values_mut() {
        let _ = hold.try_downgrade(frontier.clone());  // sends channel msg
    }
    for hold in self.compute_holds.values_mut() {
        let _ = hold.try_downgrade(frontier.clone());  // sends channel msg
    }
}
```

Each `try_downgrade` on a `ReadHold` (read_holds.rs:135-158) computes a `ChangeBatch` and sends it on an unbounded `tokio::sync::mpsc` channel:

```rust
// storage-types/src/read_holds.rs:135-158
pub fn try_downgrade(&mut self, frontier: Antichain<T>) -> Result<(), ...> {
    let mut changes = ChangeBatch::new();
    changes.extend(self.since.iter().map(|t| (t.clone(), -1)));  // retract old
    changes.extend(frontier.iter().map(|t| (t.clone(), 1)));      // add new
    self.since = frontier;

    if !changes.is_empty() {
        let _ = (self.change_tx)(self.id, changes);  // CHANNEL SEND
    }
    Ok(())
}
```

The EpochMilliseconds timeline contains **all user tables, sources, materialized views, indexes, and system objects**. After every group commit, the read timestamp advances, so every single hold produces a non-empty ChangeBatch and sends a message.

### Channel Message Cascade

The channel messages are processed on the receiving side, creating additional work:

**For storage holds** (storage_collections.rs:2827-2856):
```rust
// Background task batches messages via try_recv loop
Some(holds_changes) = self.holds_rx.recv() => {
    let mut batched_changes = BTreeMap::new();
    batched_changes.insert(holds_changes.0, holds_changes.1);
    while let Ok(mut holds_changes) = self.holds_rx.try_recv() {
        // O(N) try_recv iterations to drain pending messages
        batched_changes.entry(...).and_modify(|e| e.extend(...))...;
    }
    update_read_capabilities_inner(..., &mut batched_changes);
}
```

**For compute holds** (compute-client/src/controller/instance.rs:1706-1746):
- Each message triggers `apply_read_hold_change` which acquires a **per-collection mutex**
- Then propagates changes to **all dependencies**, sending MORE channel messages:

```rust
// instance.rs:1736-1746 — dependency cascade per collection
for read_hold in collection.compute_dependencies.values_mut() {
    read_hold.try_downgrade(new_since.clone())?;  // another channel send!
}
for read_hold in collection.storage_dependencies.values_mut() {
    read_hold.try_downgrade(new_since.clone())?;  // another channel send!
}
```

### Additional Channel Message Sources

The `ReadHold::clone()` method (read_holds.rs:167-194) also sends a channel message per clone:

```rust
impl<T: TimelyTimestamp> Clone for ReadHold<T> {
    fn clone(&self) -> Self {
        let mut changes = ChangeBatch::new();
        changes.extend(self.since.iter().map(|t| (t.clone(), 1)));
        if !changes.is_empty() {
            match (self.change_tx)(self.id.clone(), changes) { ... }  // CHANNEL SEND
        }
        // ...
    }
}
```

This means that `ReadHolds::subset()` (read_policy.rs:106-126), which is called during peek setup and transaction initialization, also generates O(M) channel messages where M is the number of collections in the subset.

### Scaling Impact

**Per group commit (10K collections, EpochMilliseconds timeline):**
- `advance_timelines` → `downgrade()` → 10,000 channel sends
- Storage background task receives and batches → 10,000 try_recv iterations
- Compute controller processes messages → 10,000 mutex acquisitions
- Dependency propagation → additional D × N messages (D = avg dependencies per collection)

**Per query (timedomain with 500 collections):**
- `acquire_read_holds` → 500 read hold creations (500 channel messages)
- `store_transaction_read_holds` → 500 `merge_assign` calls (500 channel messages each with retract+add)
- Transaction end → 500 read hold drops (500 channel messages)
- **Total: ~2,000 channel messages per query**

**At scale (10K collections, 100 QPS, 10 group commits/sec):**
- Timeline advancement: 10 × 10,000 = 100,000 channel messages/sec
- Query read holds: 100 × 2,000 = 200,000 channel messages/sec
- Dependency cascades: additional 50,000+ messages/sec
- **Total: ~350,000+ channel messages/sec** on the single-threaded coordinator and background tasks

### Root Cause

Read capabilities use **per-hold individual channel messaging** rather than **batched frontier updates**. The `ReadHold` abstraction is designed for individual ownership tokens (like RAII), but the `ReadHolds` collection in `TimelineState` uses them as a **bulk frontier tracking mechanism**. The individual-token design sends O(N) messages where a bulk design could send O(1) — a single "advance all collections on this timeline to timestamp T" message.

Additionally, the `ReadHolds::downgrade()` method always iterates ALL holds even though most downgrades advance the same frontier (the read timestamp) by the same amount. This is redundant when all collections in a timeline share the same compaction frontier.

### Suggested Fix

**Option A (batch downgrade for timeline holds):** Instead of individual `try_downgrade` calls, add a `batch_downgrade` method that collects all changes into a single `BTreeMap<GlobalId, ChangeBatch<T>>` and sends them in one message:

```rust
impl ReadHolds<T> {
    pub fn downgrade_batched(&mut self, time: T) {
        let frontier = Antichain::from_elem(time);
        let mut all_changes: BTreeMap<GlobalId, ChangeBatch<T>> = BTreeMap::new();

        for (id, hold) in self.storage_holds.iter_mut().chain(...) {
            let mut changes = ChangeBatch::new();
            changes.extend(hold.since.iter().map(|t| (t.clone(), -1)));
            changes.extend(frontier.iter().map(|t| (t.clone(), 1)));
            hold.since = frontier.clone();
            if !changes.is_empty() {
                all_changes.insert(id, changes);
            }
        }
        // Send all changes in one channel message
        batch_change_tx(all_changes);
    }
}
```

This changes O(N) channel sends to O(1), with a single batched message containing all N collection changes.

**Option B (shared frontier for timeline collections):** Since all collections in a timeline's global read holds are downgraded to the same frontier, maintain a single "timeline frontier" that the storage/compute controllers reference, rather than per-collection frontiers. When the timeline advances, update one frontier instead of N.

**Option C (coalesce on the sender side):** Buffer ReadHold changes in the coordinator and flush them periodically (e.g., once per message loop iteration) rather than sending immediately. This naturally batches changes from multiple operations into fewer channel messages.

### Files Involved
- `src/adapter/src/coord/timeline.rs` — `advance_timelines()` (line 299), O(N) downgrade loop
- `src/adapter/src/coord/read_policy.rs` — `ReadHolds::downgrade()` (line 87), iterates all holds; `ReadHolds::subset()` (line 106), clones per-hold
- `src/storage-types/src/read_holds.rs` — `ReadHold::try_downgrade()` (line 135), per-hold channel send; `ReadHold::clone()` (line 167), per-clone channel send; `ReadHold::drop()` (line 197), per-drop channel send
- `src/storage-client/src/storage_collections.rs` — background task processing hold changes (line 2827), O(N) try_recv loop
- `src/compute-client/src/controller/instance.rs` — `apply_read_hold_change()` (line 1706), per-hold mutex + dependency cascade

## Session 17: Persist State Commit — 4 Redundant Full Trace Traversals on Every Consensus Write (2026-03-09)

### Location
`src/persist-client/src/internal/state_versions.rs:270-380` — metrics update block in `try_compare_and_set_current()`
`src/persist-client/src/internal/state.rs:2413-2444` — `spine_batch_count()` and `size_metrics()`
`src/persist-client/src/internal/trace.rs:503-504, 655-667` — `num_spine_batches()` and `spine_metrics()`

### Problem

Every successful consensus CAS commit triggers **4 separate full traversals** of the entire trace (all spine batches, all hollow batches, all parts) purely for metrics reporting. This happens in the `CaSResult::Committed` branch of `try_compare_and_set_current()` (state_versions.rs:270-382).

The 4 traversals on every commit:

1. **`spine_batch_count()`** (line 288) → calls `trace.num_spine_batches()` which calls `spine.spine_batches().count()` — iterates ALL spine batches just to count them
2. **`size_metrics()`** (line 289) → calls `state.blobs().for_each(...)` which iterates ALL batches AND for each batch iterates ALL parts (checking `part_count()`, `len`, `encoded_size_bytes()`, `ts_rewrite()`, `is_inline()`, `inline_bytes()`) AND all rollups
3. **`spine_metrics()`** (line 349) → ANOTHER full iteration of ALL spine batches, checking `is_compact()`, `is_merging()` to categorize each batch
4. **`batch_parts_by_version`** (lines 360-380) → YET ANOTHER full traversal of ALL batches, ALL parts, performing string operations (`split_once('/')`, substring slicing) on every part key to extract writer version info

```rust
// state_versions.rs:270-382 — ALL of this runs on EVERY successful CAS commit
CaSResult::Committed => {
    // Traversal 1: count spine batches
    shard_metrics.spine_batch_count
        .set(u64::cast_from(new_state.spine_batch_count()));  // O(B) full scan

    // Traversal 2: size metrics — iterates ALL batches and ALL parts
    let size_metrics = new_state.size_metrics();  // O(B × P) full scan
    shard_metrics.hollow_batch_count.set(...);
    shard_metrics.batch_part_count.set(...);
    shard_metrics.rewrite_part_count.set(...);  // checked per-part
    shard_metrics.update_count.set(...);
    shard_metrics.inline_part_count.set(...);   // checked per-part
    shard_metrics.inline_part_bytes.set(...);   // checked per-part
    // ... 10+ metric updates from this single traversal ...

    // Traversal 3: spine metrics — another full batch scan
    let spine_metrics = new_state.collections.trace.spine_metrics();  // O(B) full scan
    shard_metrics.compact_batches.set(...);
    shard_metrics.compacting_batches.set(...);
    shard_metrics.noncompact_batches.set(...);

    // Traversal 4: batch parts by version — full batch+part scan with string ops
    let batch_parts_by_version = new_state.collections.trace.batches()
        .flat_map(|x| x.parts.iter())     // O(B × P)
        .flat_map(|part| {
            let key = ...;
            let (writer_key, _) = key.0.split_once('/')?;  // string alloc per part
            match &writer_key[..1] { ... }
        });
    shard_metrics.set_batch_part_versions(batch_parts_by_version);
}
```

### Scaling Impact

Every persist operation goes through this path:
- **`compare_and_append`** (every write) → `apply_unbatched_cmd` → `try_compare_and_set_current`
- **`merge_res`** (every compaction result) → `apply_unbatched_idempotent_cmd` → same path
- **`downgrade_since`** (every since advance) → same path
- **`register`** (reader/writer registration) → same path
- **`add_rollup`**, **`remove_rollups`** → same path
- **`spine_exert`** (maintenance) → same path

With a shard containing 500 spine batches and 5,000 parts:
- Each commit does: 500 + (500 × 5,000 per-part checks) + 500 + (500 × 5,000 string splits) = **~5,002,000 iterations**
- At high write throughput (e.g., 100 commits/sec), that's **500 million iterations/sec** purely for metrics
- The string operations in traversal 4 (`split_once`, substring matching) add CPU overhead on top of the iteration cost

Additionally, `spine_batch_count()` (trace.rs:503-504) is particularly wasteful:
```rust
pub fn num_spine_batches(&self) -> usize {
    self.spine.spine_batches().count()  // iterates ALL batches just to count
}
```
The Spine struct already has `self.merging: Vec<MergeState<T>>` — the batch count could be maintained incrementally as a field.

### Redundancy Analysis

Traversals 1 and 3 both iterate spine batches but extract different information. Traversals 2 and 4 both iterate all batches and their parts. All 4 could be fused into a **single pass**.

### Proposed Fix

**Option A (fuse into single traversal):** Replace the 4 separate traversals with a single `compute_all_metrics()` method that walks the trace once and returns a struct with all metric values:

```rust
struct TraceMetrics {
    spine_batch_count: usize,
    compact_batches: u64,
    compacting_batches: u64,
    noncompact_batches: u64,
    hollow_batch_count: usize,
    batch_part_count: usize,
    // ... all other metrics ...
    batch_parts_by_version: BTreeMap<String, (usize, usize)>,
}

// Single O(B × P) pass instead of 4 passes
fn compute_all_metrics(&self) -> TraceMetrics { ... }
```

This reduces 4 × O(B × P) to 1 × O(B × P) — a **4× reduction** in iteration work.

**Option B (incremental metric maintenance):** Maintain metric counters incrementally as the trace is modified. When a batch is inserted/removed/compacted, update the counters in O(1) instead of recomputing from scratch. `spine_batch_count` is the most obvious candidate — the Spine already knows when batches are added/removed.

**Option C (sample or throttle metrics):** Not every CAS commit needs fresh metrics. Update the full metrics only every N commits or every T seconds, using cached values in between. Most metrics (batch counts, part versions) change slowly relative to commit frequency.

### Files Involved
- `src/persist-client/src/internal/state_versions.rs` — `try_compare_and_set_current()` (line 270), 4 separate traversals in committed path
- `src/persist-client/src/internal/state.rs` — `spine_batch_count()` (line 2413), `size_metrics()` (line 2417), `blobs()` (line 2697) — each does a full trace walk
- `src/persist-client/src/internal/trace.rs` — `num_spine_batches()` (line 503), `spine_metrics()` (line 655), `batches()` — iterated separately 4 times
- `src/persist-client/src/internal/apply.rs` — `apply_unbatched_cmd()` (line 357), the entry point that routes ALL state mutations through this path
- `src/persist-client/src/internal/machine.rs` — `compare_and_append` (line 474), `merge_res` (line 1092), `downgrade_since` (line 618), etc. — all callers

## Session 18: Quadratic Monotonicity Analysis — O(V²·N) Expression Clones+Analyses Per SELECT Through Views (2026-03-09)

### Location
`src/adapter/src/optimize/dataflows.rs:483-563` — `monotonic_object()` / `monotonic_object_inner()`
`src/adapter/src/optimize/dataflows.rs:310-388` — `import_into_dataflow()` (calls monotonic_object per import)
`src/adapter/src/optimize/dataflows.rs:399-409` — `import_view_into_dataflow()` (recursively imports view deps)

### Problem

During dataflow construction for every SELECT query, `import_into_dataflow()` calls `monotonic_object()` for **each imported collection** (line 322). This function creates a **fresh memo** on every top-level call (line 484: `&mut BTreeMap::new()`), then **clones the entire optimized MIR expression** of each view encountered (line 513), traverses it to find dependencies, and runs a **full Monotonic analysis** pass (lines 537-539). Because the memo is discarded between calls, the same views are re-analyzed multiple times.

**The per-call cost (lines 483-563):**

```rust
fn monotonic_object(&self, id: GlobalId, features: &OptimizerFeatures) -> bool {
    self.monotonic_object_inner(id, &mut BTreeMap::new(), features)  // FRESH MEMO every call
        .unwrap_or_else(|e| { ... false })
}

fn monotonic_object_inner(&self, id: GlobalId, memo: &mut BTreeMap<GlobalId, bool>, ...) {
    if let Some(monotonic) = memo.get(&id) { return Ok(*monotonic); }  // only caches within this call

    match self.catalog.get_entry(&id).item() {
        CatalogItem::View(View { optimized_expr, .. }) => {
            let view_expr = optimized_expr.as_ref().clone().into_inner();  // FULL CLONE — O(N)

            // Traverse ALL nodes to find Gets
            view_expr.try_visit_post(&mut |e| {                           // O(N) traversal
                if let MirRelationExpr::Get { id: Id::Global(got_id), .. } = e {
                    self.monotonic_object_inner(*got_id, memo, ...)?;     // recurse into deps
                }
            });

            // Run FULL Monotonic analysis on the cloned expression
            let mut builder = DerivedBuilder::new(features);
            builder.require(Monotonic::new(monotonic_ids.clone()));       // clone BTreeSet
            let derived = builder.visit(&view_expr);                      // O(N) analysis pass
        }
        // ...
    }
    memo.insert(id, monotonic);  // only lives for this call chain
}
```

**The calling pattern in `import_into_dataflow` (lines 310-408):**

```rust
pub fn import_into_dataflow(&mut self, id: &GlobalId, dataflow: &mut DataflowDesc, ...) {
    if dataflow.is_imported(id) { return Ok(()); }   // skip if already imported

    let monotonic = self.monotonic_object(*id, ...);  // NEW memo, full recursive analysis

    // ... import the item ...

    // For views: recursively import dependencies
    CatalogItem::View(view) => {
        self.import_view_into_dataflow(id, expr, dataflow, ...)?;
    }
}

pub fn import_view_into_dataflow(&mut self, ...) {
    for get_id in view.depends_on() {
        self.import_into_dataflow(&get_id, dataflow, ...)?;  // each calls monotonic_object again
    }
    dataflow.insert_plan(*view_id, view.clone());             // ANOTHER full expression clone
}
```

### Execution Trace for a View Chain

Consider a query `SELECT * FROM v3` where `v3 → v2 → v1 → source`:

```
import_into_dataflow(v3):
  monotonic_object(v3) [memo={}]:
    clone v3 expr, traverse it, find Get(v2)
    monotonic_object_inner(v2) [same memo]:
      clone v2 expr, traverse it, find Get(v1)
      monotonic_object_inner(v1) [same memo]:
        clone v1 expr, traverse it, find Get(source)
        monotonic_object_inner(source) → lookup, no clone
        DerivedBuilder.visit(v1_expr)                    ← analysis of v1
      DerivedBuilder.visit(v2_expr)                      ← analysis of v2
    DerivedBuilder.visit(v3_expr)                        ← analysis of v3
  import_view_into_dataflow(v3):
    import_into_dataflow(v2):
      monotonic_object(v2) [NEW memo={}]:                ← v2 RE-ANALYZED from scratch
        clone v2 expr, traverse it, find Get(v1)
        monotonic_object_inner(v1) [same memo]:          ← v1 RE-ANALYZED
          clone v1 expr, traverse it, ...
          DerivedBuilder.visit(v1_expr)                  ← analysis of v1 (2nd time)
        DerivedBuilder.visit(v2_expr)                    ← analysis of v2 (2nd time)
      import_view_into_dataflow(v2):
        import_into_dataflow(v1):
          monotonic_object(v1) [NEW memo={}]:            ← v1 RE-ANALYZED from scratch
            clone v1 expr, traverse it, ...
            DerivedBuilder.visit(v1_expr)                ← analysis of v1 (3rd time!)
          import_view_into_dataflow(v1):
            import_into_dataflow(source): ...            ← no view, just source lookup
```

**v1's expression is cloned and analyzed 3 times. v2's is cloned and analyzed 2 times. v3's once.**

### Cost Analysis

For a chain of V views, each with average expression size N nodes:

| View | Times cloned | Times analyzed | Total work |
|------|-------------|----------------|-----------|
| v_1 (leaf) | V | V | V × 3N |
| v_2 | V-1 | V-1 | (V-1) × 3N |
| ... | ... | ... | ... |
| v_V (root) | 1 | 1 | 1 × 3N |

**Total: Σᵢ₌₁ᵛ i × 3N = 3N × V(V+1)/2 = O(V² × N)**

Per-clone cost: O(N) allocation + copying
Per-analysis cost: O(N) tree traversal (DerivedBuilder.visit)
Per-traversal cost: O(N) try_visit_post to find Gets

With V=10 views and N=500 nodes each:
- 10+9+8+...+1 = 55 expression clones = **27,500 nodes allocated**
- 55 DerivedBuilder analysis passes = **27,500 nodes visited**
- 55 Get-finding traversals = **27,500 nodes visited**
- **Total: ~82,500 node operations** for monotonicity alone

With V=20 views: 210 clones × 3 × 500 = **315,000 node operations**

### Additional Waste

1. **Line 513**: `optimized_expr.as_ref().clone().into_inner()` — the clone is only needed because `try_visit_post` takes `&MirRelationExpr` and the analysis also takes a reference. The expression could be inspected in-place without cloning.

2. **Line 538**: `Monotonic::new(monotonic_ids.clone())` — clones the BTreeSet of monotonic IDs for the analysis builder.

3. **Line 409**: `dataflow.insert_plan(*view_id, view.clone())` — in `import_view_into_dataflow`, the optimized expression is cloned AGAIN for insertion into the dataflow. This is on top of the clone done for monotonicity checking.

### Root Cause

The `monotonic_object()` function (line 483) creates a fresh `BTreeMap::new()` memo on every call. This memo correctly caches results within a single recursive call chain (e.g., v3 → v2 → v1 computes v1 once). But when `import_into_dataflow` is called for v2 separately, it starts with a clean memo, re-computing v1 from scratch.

The fundamental issue is that **the memo is scoped to a single `monotonic_object()` call, not to the `DataflowBuilder`**. Since each `import_into_dataflow` call makes an independent `monotonic_object` call, the results from one import's analysis are never shared with another.

### Suggested Fix

**Option A (persist memo in DataflowBuilder — simple, high impact):** Add a `monotonic_cache: BTreeMap<GlobalId, bool>` field to `DataflowBuilder`. Use it across all `monotonic_object` calls:

```rust
// In DataflowBuilder:
monotonic_cache: BTreeMap<GlobalId, bool>,

fn monotonic_object(&mut self, id: GlobalId, features: &OptimizerFeatures) -> bool {
    self.monotonic_object_inner(id, features)  // use self.monotonic_cache instead of local memo
}
```

This reduces the total cost from O(V² × N) to O(V × N) — each view is analyzed exactly once.

**Option B (avoid the clone):** Instead of cloning the expression at line 513, inspect it in-place:

```rust
// Instead of:
let view_expr = optimized_expr.as_ref().clone().into_inner();
// Use:
let view_expr = optimized_expr.as_ref();
```

Both `try_visit_post` and `DerivedBuilder::visit` take references, so the clone should be unnecessary. This eliminates all expression allocation overhead from monotonicity checking.

**Option C (combined):** Apply both fixes. This gives O(V × N) work with zero allocation overhead from monotonicity analysis. For V=20 views with N=500 nodes, this reduces from ~315,000 operations to ~30,000 (with Option A) or eliminates all cloning entirely (with Option B).

### Files Involved
- `src/adapter/src/optimize/dataflows.rs` — `monotonic_object()` (line 483), `monotonic_object_inner()` (line 491), `import_into_dataflow()` (line 310), `import_view_into_dataflow()` (line 399)
- `src/transform/src/analysis/monotonic.rs` — `Monotonic` analysis that is run per view per call
- `src/expr/src/relation.rs` — `depends_on()` used at line 406 in `import_view_into_dataflow`

## Session 19: Redundant Search Path Vec Allocation — Up to 6 Vec Clones Per Name Resolution (2026-03-09)

### Location
`src/adapter/src/catalog.rs:384-390` — `ConnCatalog::effective_search_path()`
`src/adapter/src/catalog/state.rs:2004-2035` — `CatalogState::effective_search_path()`
`src/adapter/src/catalog/state.rs:2073-2150` — `CatalogState::resolve()`
`src/sql/src/names.rs:1484-1596` — `NameResolver::resolve_item_name_name()`

### Problem

Every SQL name resolution goes through a chain that allocates and clones the search path Vec **multiple times per name**. For a single unqualified table reference like `SELECT * FROM t`, the allocation chain is:

**Step 1 — `resolve_item_name_name` (names.rs:1484):**
When resolving an unqualified name with `config = { types: true, relations: true }` (common for column type references, line 1830), this calls up to **3 catalog resolve methods** sequentially:

```rust
// names.rs:1502-1532
if r.is_err() && config.types {
    r = self.catalog.resolve_type(&raw_name);     // attempt 1 → allocates search path
}
if r.is_err() && config.functions {
    r = self.catalog.resolve_function(&raw_name);  // attempt 2 → allocates search path
}
if r.is_err() && config.relations {
    r = self.catalog.resolve_item(&raw_name);      // attempt 3 → allocates search path
}
```

Each of these calls allocates a fresh search path Vec.

**Step 2 — `ConnCatalog::resolve_item` (catalog.rs:1967-1982):**
Each `resolve_item`/`resolve_type`/`resolve_function` call invokes `effective_search_path()`:

```rust
fn resolve_item(&self, name: &PartialItemName) -> Result<...> {
    let r = self.state.resolve_entry(
        self.database.as_ref(),
        &self.effective_search_path(true),  // ← ALLOCATES Vec (catalog.rs:1973)
        name,
        &self.conn_id,
    )?;
    ...
}
```

**Step 3 — `effective_search_path` (state.rs:2004-2035):**
This creates a **new Vec** every time, with a `.contains()` linear scan:

```rust
pub fn effective_search_path(&self, search_path: &[...], include_temp_schema: bool)
    -> Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>
{
    let mut v = Vec::with_capacity(search_path.len() + 3);
    if include_temp_schema && !search_path.contains(&temp_schema) {  // O(S) scan
        v.push(temp_schema);
    }
    for schema in default_schemas.into_iter() {
        if !search_path.contains(&schema) {  // O(S) scan × 2
            v.push(schema);
        }
    }
    v.extend_from_slice(search_path);
    v  // ← New Vec allocated and returned
}
```

**Step 4 — `resolve()` (state.rs:2107):**
For unqualified names, the search path parameter is **cloned yet again**:

```rust
None => search_path.to_vec(),  // ← CLONE #2 of the search path (line 2107)
```

### Allocation Count Per Name

For one unqualified name with `types=true, relations=true`:

| Phase | What happens | Allocations |
|-------|-------------|-------------|
| `resolve_type` → `effective_search_path(false)` | New Vec (S+2 elements) | **1** |
| `resolve_type` → `resolve()` → `search_path.to_vec()` | Clone Vec (S+2 elements) | **2** |
| `resolve_item` → `effective_search_path(true)` | New Vec (S+3 elements) | **3** |
| `resolve_item` → `resolve()` → `search_path.to_vec()` | Clone Vec (S+3 elements) | **4** |

That's **4 Vec allocations per name** in the common case (type fails, relation succeeds). In the worst case with all three attempts (`types=true, functions=true, relations=true`), it's **6 Vec allocations per name**.

Each Vec contains S+3 elements where S = user search path length (typically 1-3 schemas, but can be more).

### Scaling Impact

For a `SELECT` query referencing R table names and T type names:
- **Table references** (relations only, line 1819): 2 allocations each
- **Type references** (types+relations, line 1830): 4 allocations each
- **Function calls** (functions only, line 2212): 2 allocations each

A moderately complex query with 5 table refs, 3 type refs, and 4 function calls:
- 5 × 2 + 3 × 4 + 4 × 2 = **30 Vec allocations** just for name resolution
- Each allocation is S+3 tuples of `(ResolvedDatabaseSpecifier, SchemaSpecifier)` — two enums

This is **pure overhead on the single-threaded coordinator** for every SELECT query, multiplied by query rate.

Additionally, the `effective_search_path` function does O(S) `.contains()` scans for each of 3 default schemas, adding O(3S) comparison work per allocation on top of the allocation cost.

### Root Cause

1. **`effective_search_path` returns an owned Vec** instead of being cached or returning a reference. The search path only changes when session variables change, but it's reconstructed from scratch on every name resolution call.

2. **`resolve()` clones the search path** via `search_path.to_vec()` (line 2107) even though it's already an owned Vec passed by reference. This second clone serves no purpose — the function only reads the search path.

3. **No combined resolution**: `resolve_item_name_name` calls `resolve_type`, `resolve_function`, and `resolve_item` as completely independent operations. Each rebuilds the search path from scratch and traverses it independently. A combined resolve that walks the search path once looking for matches across all categories would be far more efficient.

### Suggested Fix

**Option A (cache effective_search_path — simple, high impact):** Cache the effective search path in `ConnCatalog` as two `Vec`s (with and without temp schema), computed once at construction in `for_session()`:

```rust
struct ConnCatalog<'a> {
    // ... existing fields ...
    effective_search_path_with_temp: Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
    effective_search_path_without_temp: Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
}

pub fn effective_search_path(&self, include_temp_schema: bool)
    -> &[(ResolvedDatabaseSpecifier, SchemaSpecifier)]
{
    if include_temp_schema {
        &self.effective_search_path_with_temp
    } else {
        &self.effective_search_path_without_temp
    }
}
```

This eliminates all `effective_search_path` allocations — going from 2-6 per name down to 0.

**Option B (eliminate to_vec in resolve — simple):** Change `resolve()` line 2107 from `search_path.to_vec()` to just use the reference directly:

```rust
// Instead of:
None => search_path.to_vec(),
// Use:
None => Cow::Borrowed(search_path),
```

The `schemas` variable at line 2086 could be `Cow<[(ResolvedDatabaseSpecifier, SchemaSpecifier)]>` — in the qualified case it's `Cow::Owned(vec![...])`, in the unqualified case it's `Cow::Borrowed(search_path)`.

**Option C (combined resolve — highest impact):** Add a `resolve_any()` method that walks the search path once and tries to match items, types, and functions in a single pass:

```rust
fn resolve_any(&self, name: &PartialItemName, config: ItemResolutionConfig)
    -> Result<&CatalogEntry, SqlCatalogError>
{
    for (db_spec, schema_spec) in effective_search_path {
        let schema = self.try_get_schema(db_spec, schema_spec, conn_id)?;
        if config.types { if let Some(id) = schema.types.get(&name.item) { return Ok(...); } }
        if config.functions { if let Some(id) = schema.funcs.get(&name.item) { return Ok(...); } }
        if config.relations { if let Some(id) = schema.items.get(&name.item) { return Ok(...); } }
    }
}
```

This reduces the total work from O(K × S) down to O(S) for K attempted resolve categories.

**Combining A + B + C** would eliminate all Vec allocations and reduce search path traversals from K per name (where K=1-3) to just 1, giving a ~6× reduction in name resolution overhead.

### Files Involved
- `src/adapter/src/catalog.rs` — `ConnCatalog::effective_search_path()` (line 384), `resolve_item()` (line 1967), `resolve_function()` (line 1984), `resolve_type()` (line 2005)
- `src/adapter/src/catalog/state.rs` — `CatalogState::effective_search_path()` (line 2004), `resolve()` (line 2073, specifically line 2107 `to_vec()`)
- `src/sql/src/names.rs` — `resolve_item_name_name()` (line 1484) where up to 3 resolve attempts happen per name

## Session 20: Broadcast Notice Fan-Out — O(N) Clones Per Replica Status Event with Post-Send Filtering (2026-03-09)

### Location
`src/adapter/src/coord.rs:3767-3780` — `broadcast_notice_tx()`
`src/adapter/src/coord/message_handler.rs:697-706` — `message_cluster_event()` (the caller)
`src/adapter/src/session.rs:552-567` — `notice_filter()` (post-send filter)
`src/adapter/src/session.rs:102-103` — unbounded notice channel

### Problem

When a cluster replica status changes, the coordinator **broadcasts the notice to every connected session** by cloning the notice N times, then each session **filters it out after receiving it** if it doesn't belong to that session's cluster.

**The broadcast path** (`broadcast_notice_tx`, lines 3767-3780):

```rust
pub(crate) fn broadcast_notice_tx(&self)
    -> Box<dyn FnOnce(AdapterNotice) -> () + Send + 'static>
{
    let senders: Vec<_> = self
        .active_conns
        .values()
        .map(|meta| meta.notice_tx.clone())  // Step 1: Clone N senders into a Vec
        .collect();
    Box::new(move |notice| {
        for tx in senders {
            let _ = tx.send(notice.clone());  // Step 2: Clone notice N times and send
        }
    })
}
```

**The filter path** (`notice_filter`, session.rs lines 552-567):

```rust
fn notice_filter(&self, notice: AdapterNotice) -> Option<AdapterNotice> {
    let minimum_client_severity = self.vars.client_min_messages();
    let sev = notice.severity();
    if !minimum_client_severity.should_output_to_client(&sev) {
        return None;  // Cloned and sent, but thrown away here
    }
    if let AdapterNotice::ClusterReplicaStatusChanged { cluster, .. } = &notice {
        if cluster != self.vars.cluster() {
            return None;  // Cloned and sent, but thrown away here
        }
    }
    Some(notice)
}
```

**The filtering happens on two axes, both AFTER the broadcast:**
1. **Severity filter**: If a session has `client_min_messages` set above `Notice` severity, the notice is discarded
2. **Cluster filter**: If the notice is for cluster X but the session is connected to cluster Y, it's discarded

This means with 10 clusters and sessions evenly distributed, **90% of all cloned notices are immediately discarded** by the receiver.

### Additional Problem: Unbounded Channel

The notice channel is `mpsc::unbounded_channel()` (session.rs line 320). If a session is slow to drain notices (e.g., a long-running query), notices accumulate without limit. In a system with frequent replica status changes (flapping replicas, scaling events), the channel can grow unboundedly.

The drain path (`drain_notices`, session.rs lines 542-549) only runs when the session actively polls:

```rust
pub fn drain_notices(&mut self) -> Vec<AdapterNotice> {
    let mut notices = Vec::new();
    while let Ok(notice) = self.notices_rx.try_recv() {
        if let Some(notice) = self.notice_filter(notice) {
            notices.push(notice);
        }
    }
    notices
}
```

A session stuck in a long SUBSCRIBE or COPY won't drain, causing memory to grow.

### Cost Analysis

Per replica status change event:
1. **N `UnboundedSender` clones** — each is an `Arc` ref-count bump + Vec allocation for the senders collection
2. **N `AdapterNotice` clones** — `ClusterReplicaStatusChanged` contains 2 `String` fields (`cluster`, `replica`) + a `ClusterStatus` enum + a `DateTime<Utc>`, so each clone allocates 2 heap strings
3. **N channel sends** — each enqueues a notice into the unbounded MPSC channel
4. **N filter evaluations** — most of which discard the notice

With C clusters, R replicas per cluster, and N total sessions:
- Each replica flap generates N notice clones
- With R total replicas flapping during a scaling event, that's **N × R clones**
- Of these, only N/C are relevant per event (sessions on the right cluster), so **(1 - 1/C) × N × R notices are wasted**

**Example**: 1,000 sessions across 10 clusters, 5 replicas flapping during a resize:
- 1,000 × 5 = 5,000 notice clones
- 4,500 immediately discarded (90% waste)
- Each clone = 2 String allocations = 9,000 wasted heap allocations

### Root Cause

1. **No sender-side filtering**: The broadcast function has no knowledge of which sessions care about which clusters. It blindly sends to all.
2. **No shared ownership**: Each notice is fully cloned per session instead of using `Arc<AdapterNotice>` for shared immutable data.
3. **Unbounded channel with no backpressure**: The MPSC channel can grow without limit, creating memory pressure for slow consumers.

### Suggested Fix

**Option A (sender-side filtering — highest impact, simple):** Pass the cluster name into the broadcast function and filter at send time:

```rust
pub(crate) fn broadcast_cluster_notice_tx(
    &self,
    target_cluster: &str,
) -> Box<dyn FnOnce(AdapterNotice) -> () + Send + 'static> {
    let senders: Vec<_> = self
        .active_conns
        .values()
        .filter(|meta| meta.cluster() == target_cluster)  // Filter BEFORE cloning
        .map(|meta| meta.notice_tx.clone())
        .collect();
    Box::new(move |notice| {
        for tx in senders {
            let _ = tx.send(notice.clone());
        }
    })
}
```

This requires tracking which cluster each connection is using in `ConnMeta`. This would reduce clones from N to N/C for C clusters.

**Option B (Arc-wrap notices — reduces clone cost):** Use `Arc<AdapterNotice>` instead of cloning the full notice:

```rust
type SharedNotice = Arc<AdapterNotice>;

// In broadcast:
let notice = Arc::new(notice);
for tx in senders {
    let _ = tx.send(Arc::clone(&notice));  // O(1) ref-count bump instead of deep clone
}
```

This eliminates all String allocations — only ref-count bumps remain.

**Option C (bounded channel + drop policy):** Replace `mpsc::unbounded_channel()` with a bounded channel that drops old notices when full:

```rust
let (notices_tx, notices_rx) = mpsc::channel(1024);  // Bounded to 1024
// Use try_send() instead of send() — drops notice if channel full
```

This prevents memory growth from slow consumers.

**Combining A + B** would reduce both the number of sends (from N to N/C) and the cost per send (from String clone to Arc bump), giving a ~C× reduction in allocation overhead with near-zero per-send cost.

### Files Involved
- `src/adapter/src/coord.rs` — `broadcast_notice_tx()` (line 3767), `broadcast_notice()` (line 3759, dead code), `ConnMeta` (line 1114)
- `src/adapter/src/coord/message_handler.rs` — `message_cluster_event()` (line 697, the broadcast call site)
- `src/adapter/src/session.rs` — `notice_filter()` (line 552), `drain_notices()` (line 542), `recv_notice()` (line 526), unbounded channel (line 320)
- `src/adapter/src/notice.rs` — `AdapterNotice` enum (line 33), `ClusterReplicaStatusChanged` variant (line 66)

## Session 21: Orphaned Watchsets — Controller State Leak + Dead `uninstall_watch_set` Code (2026-03-09)

### Location
- `src/adapter/src/coord.rs:3918-3924` — `cancel_pending_watchsets()`
- `src/controller/src/lib.rs:461-474` — `uninstall_watch_set()` (DEAD CODE — never called)
- `src/controller/src/lib.rs:536-572` — `handle_frontier_updates()`
- `src/controller/src/lib.rs:184-202` — `unfulfilled_watch_sets_by_object` and `unfulfilled_watch_sets` state
- `src/adapter/src/coord/statement_logging.rs:774-805` — `install_peek_watch_sets()`

### Problem

The coordinator's watchset system has a **controller-side state leak** when connections are cancelled, combined with **per-query overhead** when statement logging is enabled.

**The Leak — Missing Controller Cleanup:**

When a connection terminates, `cancel_pending_watchsets()` (coord.rs:3918) cleans up the *coordinator-side* state:
```rust
pub fn cancel_pending_watchsets(&mut self, conn_id: &ConnectionId) {
    if let Some(ws_ids) = self.connection_watch_sets.remove(conn_id) {
        for ws_id in ws_ids {
            self.installed_watch_sets.remove(&ws_id);
            // BUG: Does NOT call self.controller.uninstall_watch_set(&ws_id)
        }
    }
}
```

It removes from `installed_watch_sets` and `connection_watch_sets`, but **never calls `controller.uninstall_watch_set()`**. This leaves orphaned entries in two controller-side maps:
- `unfulfilled_watch_sets: BTreeMap<WatchSetId, (BTreeSet<GlobalId>, T)>` (lib.rs:193)
- `unfulfilled_watch_sets_by_object: BTreeMap<GlobalId, BTreeSet<WatchSetId>>` (lib.rs:191)

The `uninstall_watch_set()` function (lib.rs:461-474) **exists and properly cleans both maps**, but it is dead code — never called from anywhere in the entire codebase.

**What Happens to Orphaned Watchsets:**

Every frontier update triggers `handle_frontier_updates()` (lib.rs:536-572), which:
1. For each `(obj_id, antichain)` in the update batch
2. Looks up ALL watchset IDs in `unfulfilled_watch_sets_by_object[obj_id]`
3. For EACH watchset, checks if the frontier has advanced past the watchset's timestamp
4. If yes, removes from `unfulfilled_watch_sets` and marks as `finished`

Orphaned watchsets remain in these maps until their frontier naturally advances past the timestamp. The `finished` IDs are returned to the coordinator via `WatchSetFinished`, but at line 394 in message_handler.rs, the coordinator skips them because they're no longer in `installed_watch_sets`:
```rust
let Some((conn_id, rsp)) = self.installed_watch_sets.remove(&ws_id) else {
    continue;  // Orphaned watchset silently discarded
};
```

So orphaned watchsets waste work in `handle_frontier_updates` until their timestamp is naturally passed.

**Per-Query Watchset Overhead:**

When statement logging is active (sample rate > 0, which is the default in many environments), every SELECT/peek creates **2 watchsets** via `install_peek_watch_sets()`:
1. One storage watchset for all transitive storage dependencies (tables, sources)
2. One compute watchset for all transitive compute dependencies (materialized views, indexes)

Each watchset installation:
- Calls `transitive_uses()` — BFS through entire dependency graph per input ID (state.rs:384-412)
- Inserts into 4 maps: `unfulfilled_watch_sets`, `unfulfilled_watch_sets_by_object`, `installed_watch_sets`, `connection_watch_sets`
- Checks current frontiers for each dependency to determine if immediately fulfilled

### Scaling Impact

**Memory leak:**
- With C connections disconnecting per hour, each with P pending peeks, up to 2×C×P orphaned watchsets accumulate in the controller
- Each orphaned watchset stores a `BTreeSet<GlobalId>` of dependencies + a timestamp
- These persist until all dependency frontiers advance past the orphaned timestamp

**CPU waste on hot path:**
- `handle_frontier_updates` runs on every storage and compute frontier response
- For each frontier update on object X, it iterates ALL watchsets in `unfulfilled_watch_sets_by_object[X]`, including orphaned ones
- With popular objects (e.g., `mz_tables` system table), hundreds of orphaned watchsets can accumulate per object
- Total wasted work per frontier batch: O(U × W_orphaned) where U = updated objects, W_orphaned = orphaned watchsets per object

**Connection churn amplification:**
- Short-lived connections (e.g., monitoring scripts, connection poolers with aggressive cycling) create and orphan watchsets rapidly
- In environments with 100+ queries/sec and frequent disconnects, thousands of orphaned entries can accumulate

### Root Cause

The `cancel_pending_watchsets` function was written to only clean up coordinator-side state, likely because the controller's `uninstall_watch_set` function was added later or the bi-directional cleanup was overlooked. The fact that `uninstall_watch_set` is dead code strongly suggests this is a bug, not an intentional design choice.

### Suggested Fix

**Fix the leak (1 line):** Add `self.controller.uninstall_watch_set(&ws_id);` inside `cancel_pending_watchsets`:

```rust
pub fn cancel_pending_watchsets(&mut self, conn_id: &ConnectionId) {
    if let Some(ws_ids) = self.connection_watch_sets.remove(conn_id) {
        for ws_id in ws_ids {
            self.installed_watch_sets.remove(&ws_id);
            self.controller.uninstall_watch_set(&ws_id);  // FIX: clean up controller state
        }
    }
}
```

This is a safe fix because `uninstall_watch_set` is already a no-op for already-finished watchsets (lib.rs:462: `if let Some(...) = self.unfulfilled_watch_sets.remove(ws_id)`).

**Optional — reduce per-query watchset overhead:**
- Consider batching watchset installations across concurrent queries watching the same objects
- For statement logging specifically, consider a lighter-weight mechanism than full watchsets (e.g., just recording the timestamp and checking lazily)

### Files Involved
- `src/adapter/src/coord.rs` — `cancel_pending_watchsets()` (line 3918), `install_compute_watch_set()` (line 3882), `install_storage_watch_set()` (line 3901), `installed_watch_sets` field, `connection_watch_sets` field
- `src/controller/src/lib.rs` — `uninstall_watch_set()` (line 461, DEAD CODE), `handle_frontier_updates()` (line 536), `install_compute_watch_set()` (line 381), `install_storage_watch_set()` (line 424), `unfulfilled_watch_sets` (line 193), `unfulfilled_watch_sets_by_object` (line 191)
- `src/adapter/src/coord/statement_logging.rs` — `install_peek_watch_sets()` (line 774)
- `src/adapter/src/coord/message_handler.rs` — `WatchSetFinished` handler (line 391), where orphaned watchsets are silently skipped
- `src/adapter/src/coord/command_handler.rs` — `cancel_pending_watchsets` call sites (lines 1750, 1793)
- `src/adapter/src/statement_logging.rs` — `WatchSetCreation::new()` (line 981), `transitive_uses()` BFS per dependency

## Session 22: O(N²) Listen Batch Lookup in Persist — Linear Scan Per Batch on Every SUBSCRIBE Tick (2026-03-09)

### Location
`src/persist-client/src/internal/state.rs:2617-2629` — `next_listen_batch()`
`src/persist-client/src/internal/machine.rs:932-1029` — retry loop calling `next_listen_batch`
`src/persist-client/src/read.rs:285-304` — `Listen::next()` consumer
`src/persist-client/src/operators/shard_source.rs:472-481` — Timely dataflow operator calling `listen.next()`

### Problem

The `next_listen_batch` function — which is the core iteration primitive for SUBSCRIBE, tailing, and every persist-backed source operator — performs a **linear scan through ALL batches in the shard's spine** to find the next batch matching a frontier. The function itself has a TODO acknowledging this:

```rust
pub fn next_listen_batch(&self, frontier: &Antichain<T>) -> Result<HollowBatch<T>, SeqNo> {
    // TODO: Avoid the O(n^2) here: `next_listen_batch` is called once per
    // batch and this iterates through all batches to find the next one.
    self.collections
        .trace
        .batches()
        .find(|b| {
            PartialOrder::less_equal(b.desc.lower(), frontier)
                && PartialOrder::less_than(frontier, b.desc.upper())
        })
        .cloned()
        .ok_or(self.seqno)
}
```

**The call chain:**

1. `shard_source.rs:472` — Timely dataflow `persist_source` operator calls `listen.next()` in a loop to consume new data
2. `read.rs:292` — `Listen::next()` calls `machine.next_listen_batch(&self.frontier, ...)`
3. `machine.rs:940` — `Machine::next_listen_batch()` calls `self.applier.next_listen_batch(frontier)`
4. `apply.rs:338` — delegates to `state.next_listen_batch(frontier)` which does the linear scan
5. If no batch found yet, the machine enters a **retry loop** (machine.rs:972-1029) that calls `next_listen_batch` again on every watch/sleep wakeup — each retry re-scans all batches

**The `batches()` iterator** (trace.rs:496-501) traverses the full spine structure:
```rust
pub fn batches(&self) -> impl Iterator<Item = &HollowBatch<T>> {
    self.spine
        .spine_batches()
        .flat_map(|b| b.parts.as_slice())
        .map(|b| &*b.batch)
}
```

This iterates all `SpineBatch` objects in the spine's merge tree, then flat-maps over all `parts` within each spine batch. The spine is organized as a merge tree where batch count depends on write rate and compaction lag.

### Why This is O(N²)

To consume B batches from a listen (e.g., catching up a SUBSCRIBE after reconnect or processing a burst of writes):

- **Call 1**: Scan N batches, find batch at position ~0 → O(N)
- **Call 2**: Advance frontier, scan N batches, find batch at position ~1 → O(N)
- **Call 3**: Advance frontier, scan N batches, find batch at position ~2 → O(N)
- ...
- **Call B**: Scan N batches, find batch at position ~B → O(N)

**Total: O(N × B)**, and since B ≈ N when catching up, this is **O(N²)**.

Each scan does two `PartialOrder` comparisons per batch (less_equal on lower, less_than on upper), plus the spine traversal overhead.

### Where This Manifests

1. **SUBSCRIBE catch-up**: When a SUBSCRIBE reconnects or starts from an older `as_of`, it must consume all batches between the `as_of` and the current upper. With a high-write-rate shard that has accumulated thousands of uncompacted batches, this catch-up is quadratic.

2. **persist_source operator**: Every Timely dataflow source operator (powering tables, sources, materialized views) uses `listen.next()` in `shard_source.rs:472`. During cluster rehydration, ALL sources must catch up from their last known frontier, hitting this O(N²) path concurrently.

3. **Compaction lag amplification**: When compaction falls behind (e.g., due to resource pressure), the number of spine batches grows. This makes the listen path slower, which can delay frontier advancement, which in turn can delay compaction — a **positive feedback loop**.

4. **Retry loop amplification**: In the retry loop (machine.rs:972-1029), when a batch isn't found, the code waits via watch/sleep and retries. Each retry re-executes the full linear scan. Under write pressure where batches arrive faster than they're consumed, the retry loop can amplify the scan cost.

### Scaling Impact

With realistic production numbers:
- **N = 1,000 uncompacted batches** (common for high-write-rate shards with compaction lag)
- Catching up from the beginning: 1,000 × 1,000 = **1,000,000 batch comparisons**
- Each comparison involves `PartialOrder` operations on `Antichain<T>` (element-wise comparisons)
- Plus `.cloned()` on the found `HollowBatch` (which includes all part metadata, run_meta, descriptions)

With **N = 10,000 batches** (possible during compaction stalls or large backfills):
- 10,000 × 10,000 = **100,000,000 comparisons** just to catch up
- This is on **every persist source worker**, and a cluster rehydrating may have dozens of sources catching up concurrently

### Root Cause

The `Trace` spine structure is optimized for merging and compaction, not for sequential iteration by frontier. There's no index from frontier/timestamp to batch position. The `batches()` iterator always starts from the oldest batch and scans forward, with no way to resume from a previous position.

### Suggested Fix

**Option A (iterator-based listen — simple)**: Instead of calling `next_listen_batch` independently each time, return an iterator/cursor that remembers position in the spine. The Listen struct already tracks `self.frontier`, so the cursor could skip directly to where the previous batch ended. This turns O(N²) into O(N).

**Option B (binary search on frontier)**: Since batches in the spine are ordered by time range, a binary search on `b.desc.lower()` could find the right batch in O(log N) per call, making the total O(N log N). The spine's merge tree structure may complicate this but the `SpineBatch` ordering within levels should be exploitable.

**Option C (cache last position)**: Store the last-returned batch index/position alongside the frontier. On the next call, start scanning from that position instead of the beginning. This is a minimal change — just add an `Option<usize>` skip hint to `next_listen_batch`.

### Files Involved
- `src/persist-client/src/internal/state.rs` — `next_listen_batch()` (line 2617), `snapshot()` (line 2585)
- `src/persist-client/src/internal/machine.rs` — `Machine::next_listen_batch()` (line 932), retry loop (lines 972-1029)
- `src/persist-client/src/internal/apply.rs` — `Applier::next_listen_batch()` (line 338), delegates to state
- `src/persist-client/src/read.rs` — `Listen::next()` (line 285), `Subscribe::next()` (line 153)
- `src/persist-client/src/operators/shard_source.rs` — `persist_source` Timely operator (line 472), where listen is consumed in production
- `src/persist-client/src/internal/trace.rs` — `Trace::batches()` (line 496), `Spine::spine_batches()` (line 1664)

## Session 23: DifferentialWriteTask — Unbounded In-Memory State + Full Clone on Every Conflict Retry (2026-03-09)

### Location
`src/storage-controller/src/collection_mgmt.rs:525-1037` — `DifferentialWriteTask`

### Problem

The `DifferentialWriteTask` manages 10 types of introspection collections (Frontiers, ReplicaFrontiers, ShardMapping, StorageSourceStatistics, StorageSinkStatistics, ComputeDependencies, ComputeOperatorHydrationStatus, ComputeMaterializedViewRefreshes, ComputeErrorCounts, ComputeHydrationTimes). Each task holds two parallel data structures that create compounding scaling problems:

**Problem 1 — Unbounded `desired` state in memory (lines 527-536)**

```rust
// This is memory inefficient: we always keep a full copy of
// desired, so that we can re-derive a to_write if/when someone else
// writes to persist and we notice because of an upper conflict.
desired: Vec<(Row, Diff)>,
```

The code's own comment acknowledges this is "memory inefficient." Every differential collection maintains a **complete copy** of the collection's desired state in a `Vec<(Row, Diff)>`. For the Frontiers introspection collection, this means one Row per collection per frontier type. With thousands of collections, this grows proportionally. The `desired` Vec is **never truncated** — it only grows via `extend_from_slice` (line 875) and shrinks only via consolidation.

**Problem 2 — Unconditional O(N log N) consolidation on every batch (lines 844-846)**

```rust
// TODO: Maybe don't do it every time?
consolidation::consolidate(&mut self.desired);
consolidation::consolidate(&mut self.to_write);
```

Every time `handle_updates` processes a batch of write ops, it consolidates **both** `desired` and `to_write` unconditionally. Consolidation sorts the entire Vec and deduplicates — O(N log N) where N = total entries. This happens even when only 1 or 2 rows were appended to a Vec of 10,000.

The code even has a TODO acknowledging this could be avoided. Since `update_frontier_introspection()` is called every ~1 second from `maintain()` (lib.rs:3806), and it sends differential updates for ALL collections to the Frontiers and ReplicaFrontiers tasks, these tasks consolidate their full state every second.

**Problem 3 — Full snapshot + full clone on conflict retry (lines 1012-1037)**

```rust
async fn sync_to_persist(&mut self) {
    let snapshot = read_handle.snapshot_and_fetch(as_of).await;   // Full persist read
    // ... negate all snapshot rows ...
    self.to_write.clear();
    self.to_write.extend(self.desired.iter().cloned());  // CLONE ENTIRE DESIRED
    self.to_write.append(&mut negated_oks);
    consolidation::consolidate(&mut self.to_write);      // RE-CONSOLIDATE EVERYTHING
}
```

When a `compare_and_append` fails due to an upper conflict (someone else wrote to the shard), the task:
1. Acquires a new `ReadHandle` (line 1013 — calls `read_handle_fn` which creates a new handle)
2. Reads the **entire persist snapshot** into memory (line 1019)
3. Negates all snapshot rows (lines 1022-1028 — allocates a new Vec, clones every Row)
4. **Clones the entire `desired` Vec** into `to_write` (line 1034 — `.iter().cloned()`)
5. Appends negated snapshot and consolidates (lines 1035-1036)

This retry logic runs in a loop with up to **20 retries** (line 909: `max_tries(20)`). Each retry re-executes this entire sequence.

**Problem 4 — Row clone per persist write (lines 921-931)**

```rust
let updates_to_write = self.to_write.iter().map(|(row, diff)| {
    ((SourceData(Ok(row.clone())), ()), self.current_upper.clone(), diff.into_inner())
}).collect::<Vec<_>>();
```

Every write to persist clones every Row in `to_write` to wrap it in `SourceData`. This is necessary for the persist API, but combined with the retry loop means every retry clones all rows twice (once for `desired → to_write`, once for `to_write → updates_to_write`).

**Problem 5 — Delete operation is O(N) scan (lines 878-882)**

```rust
StorageWriteOp::Delete { filter } => {
    let to_delete = self.desired.extract_if(.., |(row, _)| filter(row));
    let retractions = to_delete.map(|(row, diff)| (row, -diff));
    self.to_write.extend(retractions);
}
```

`extract_if` scans the entire `desired` Vec to find matching rows. For large collections, this is O(N) per delete operation.

### Scaling Impact

With realistic production numbers:
- **10 differential introspection tasks** running concurrently
- **C = 5,000 collections** (tables, sources, MVs, subsources)
- **R = 10 replicas** across clusters

The **Frontiers task** holds C = 5,000 rows in `desired`. The **ReplicaFrontiers task** holds C × R = 50,000 rows. Every second:
1. `update_frontier_introspection()` computes diffs and sends them (lib.rs:3548-3641)
2. Each task receives the updates, extends `desired` and `to_write`
3. Both vecs are fully consolidated: O(50,000 log 50,000) = ~800,000 comparisons for ReplicaFrontiers alone
4. If write conflicts (another envd in HA failover scenario), full snapshot read + full clone + re-consolidate, up to 20 times

Total memory: Each task holds `desired` (full copy) + `to_write` (full copy during retry) + `updates_to_write` (full copy for persist) = **3× the collection data in memory simultaneously** during conflict retries. Across 10 tasks, this is 30× the total introspection data.

The unconditional consolidation cost per second across all 10 tasks: O(10 × N_i × log(N_i)) where N_i is the size of each collection. Even at steady state with no conflicts.

### Root Cause

The architecture assumes that `desired` must be held fully in memory because the task needs to re-derive `to_write` from scratch on conflict. This "full state" design was chosen to "optimize for the case where we rarely have more than one writer" (line 530-531), but:

1. It trades **constant memory overhead** for **rare conflict recovery simplicity**
2. The consolidation cost is paid **every second** regardless of conflicts
3. No amortization strategy — a 1-row change triggers full consolidation of a 50,000-row Vec

### Suggested Fix

**Option A (amortized consolidation — minimal change)**: Only consolidate when the Vec grows beyond a threshold (e.g., 2× the last consolidated size). This turns steady-state consolidation cost from O(N log N) per second to amortized O(1) per update, since most seconds only add a handful of diffs.

```rust
// Instead of unconditional consolidation:
if self.desired.len() > self.desired_consolidated_len * 2 {
    consolidation::consolidate(&mut self.desired);
    self.desired_consolidated_len = self.desired.len();
}
```

**Option B (delta-based sync — the comment's own suggestion)**: As the code comments suggest (lines 532-535), maintain an open `ReadHandle` and track persist state continuously. On conflict, compute the diff between `desired` and the current persist state incrementally rather than re-reading the full snapshot.

**Option C (avoid Row clones for persist writes)**: Pass references or use `Arc<Row>` to avoid the clone in `updates_to_write` construction (line 926). The persist API could accept borrowed data for `compare_and_append`.

**Option D (indexed desired state)**: Replace `Vec<(Row, Diff)>` with a `BTreeMap<Row, Diff>` for `desired`. This makes `Delete` operations O(log N) instead of O(N) and avoids the need for consolidation entirely (updates just modify the map in place). The tradeoff is higher per-insert cost (log N vs amortized O(1)), but this is worth it for collections with frequent deletes.

### Files Involved
- `src/storage-controller/src/collection_mgmt.rs` — `DifferentialWriteTask` struct (lines 497-547), `apply_write_op` (lines 872-883), `handle_updates` with unconditional consolidation (lines 821-869), `write_to_persist` retry loop (lines 889-1003), `sync_to_persist` full clone (lines 1012-1037)
- `src/storage-controller/src/lib.rs` — `update_frontier_introspection` (lines 3548-3641), `maintain` (lines 3801-3813), `CollectionManagerKind::Differential` mapping (lines 3816-3843)
- `src/storage-client/src/controller.rs` — `StorageWriteOp` enum definition (Append and Delete variants)

## Session 24: QueryContext `derived_context()` — Triple Deep-Clone of Accumulated State Per Subquery (2026-03-09)

### Location
`src/sql/src/plan/query.rs:6562-6582` — `QueryContext::derived_context()`
`src/sql/src/plan/query.rs:6715-6718` — `ExprContext::derived_query_context()`

### Problem

Every subquery, CTE reference, lateral join, scalar subquery, and ROWS FROM clause in a SQL query triggers `derived_context()`, which **deep-clones three accumulated collections** that grow with query complexity:

```rust
fn derived_context(&self, scope: Scope, relation_type: SqlRelationType) -> QueryContext<'a> {
    let ctes = self.ctes.clone();                          // Clone 1: BTreeMap<LocalId, CteDesc>
    let outer_scopes = iter::once(scope)
        .chain(self.outer_scopes.clone())                  // Clone 2: Vec<Scope>
        .collect();
    let outer_relation_types = iter::once(relation_type)
        .chain(self.outer_relation_types.clone())           // Clone 3: Vec<SqlRelationType>
        .collect();
    // ...
}
```

There are **12 call sites** that trigger this cloning in `query.rs` alone:
- `derived_context()` called directly for lateral joins (line 3764), ROWS FROM (line 2800), rows-from-internal (line 3202)
- `derived_query_context()` called for every scalar subquery: EXISTS (line 4585), IN-subquery (line 4594), subquery expression (line 4680), array subquery (line 4783), nested query (line 3609)
- `empty_derived_context()` for empty-scope derivations (line 3202)

### What Gets Cloned (Deep Cost Analysis)

**Clone 1 — `self.ctes: BTreeMap<LocalId, CteDesc>`:**
- Each `CteDesc` (line 6516) contains a `String` (name) and a `RelationDesc`
- `RelationDesc` contains `SqlRelationType` (Vec<SqlColumnType> + Vec<Vec<usize>> keys) plus column names
- With 20 CTEs averaging 10 columns each → cloning ~200 column type descriptors + 20 strings + 20 key sets per call

**Clone 2 — `self.outer_scopes: Vec<Scope>`:**
- Each `Scope` (scope.rs:119) contains `Vec<ScopeItem>` + `Vec<ScopeUngroupedColumn>`
- Each `ScopeItem` (scope.rs:59) contains:
  - `table_name: Option<PartialItemName>` — allocated strings
  - `column_name: ColumnName` — allocated string
  - `exprs: BTreeSet<Expr<Aug>>` — **full SQL AST expression trees** (the heaviest field)
- Cloning a Scope with 50 columns means cloning 50 ScopeItems, each with potentially complex AST expression trees in the `exprs` BTreeSet

**Clone 3 — `self.outer_relation_types: Vec<SqlRelationType>`:**
- Each `SqlRelationType` (relation.rs:270) has `column_types: Vec<SqlColumnType>` + `keys: Vec<Vec<usize>>`
- A relation with 50 columns → 50 SqlColumnType allocations cloned

### Scaling Impact — O(D² × C) Where D = Nesting Depth, C = Columns

The cloning is **cumulative**: at subquery nesting depth D, there are D-1 entries in `outer_scopes` and `outer_relation_types`. Each new derived context clones all D-1 previous entries, so total cloning work across all levels is:

- Depth 1: clone 0 entries
- Depth 2: clone 1 entry
- Depth 3: clone 2 entries
- ...
- Depth D: clone D-1 entries
- **Total: 0 + 1 + 2 + ... + (D-1) = O(D²/2) cloning operations**

Each cloned entry is O(C) where C = columns per scope/relation. The CTE map is also cloned D times (O(D × K) where K = CTE count × columns per CTE).

**Concrete example**: A query with 10 CTEs (10 columns each), 5 levels of subquery nesting, and 20-column tables:
- CTE clones: 5 × (10 CTEs × 10 columns) = 500 column type clones
- Scope clones: (0+1+2+3+4) × 20 columns × ScopeItem = 200 ScopeItem deep-clones (each with AST trees)
- Relation type clones: (0+1+2+3+4) × 20 column types = 200 column type clones
- **Total: ~900 allocations** just for context propagation, before any actual planning work

For complex analytical queries with 50+ CTEs and 8+ nesting levels (common in dbt-generated SQL):
- CTE clones: 8 × (50 × 15) = 6,000 column type clones
- Scope clones: 28 × 30 = 840 ScopeItem deep-clones with full AST trees
- **This runs on the coordinator's single-threaded planning path**, blocking all other query planning

### Root Cause

`QueryContext` uses owned `Vec` and `BTreeMap` fields instead of shared references. The `derived_context()` method was written to create a new context by prepending the current scope to a cloned copy of the parent's scopes, creating a fresh owned collection each time. This is architecturally simple but doesn't scale.

### Suggested Fix

**Option A (Rc/Arc wrapping)**: Wrap the accumulated collections in `Rc<Vec<...>>` or use a linked-list/cons-cell pattern:
```rust
// Instead of Vec<Scope>, use a shared spine:
struct ScopeChain {
    head: Scope,
    tail: Option<Rc<ScopeChain>>,
}
```
This makes `derived_context()` O(1) — just wrap the current scope + Rc::clone the parent chain. No deep cloning needed.

**Option B (persistent data structures)**: Use `im::Vector` or `imbl::Vector` (already a dependency in the project) for `outer_scopes` and `outer_relation_types`. These support O(log N) prepend with structural sharing, avoiding full clones.

**Option C (arena + indices)**: Allocate scopes and relation types in an arena, store indices in the context. Derived contexts just add an index. Zero cloning cost.

All options reduce `derived_context()` from O(D × C) per call to O(1) or O(log D).

### Files Involved
- `src/sql/src/plan/query.rs` — `QueryContext` struct (lines 6522-6537), `derived_context()` (lines 6562-6582), `empty_derived_context()` (lines 6584-6589), `derived_query_context()` (lines 6715-6718), `resolve_table_name()` (lines 6593-6649)
- `src/sql/src/plan/scope.rs` — `Scope` struct (line 119), `ScopeItem` struct (line 59) with `exprs: BTreeSet<Expr<Aug>>` (the heaviest cloned field)
- `src/repr/src/relation.rs` — `SqlRelationType` struct (line 270), cloned via `outer_relation_types`
- `src/sql-parser/src/ast/defs/expr.rs` — `Expr<Aug>` AST enum (line 36), deep-cloned inside every `ScopeItem.exprs`

## Session 25: Compute Instance `maintain()` — 7 Redundant Full Collection Scans + Quadratic Dependency Walks Every Second (2026-03-09)

### Location
`src/compute-client/src/controller/instance.rs:2233-2242` — `Instance::maintain()`

### Problem

The `maintain()` method is called **once per second** per compute cluster instance. It runs **7 sequential sub-methods**, each performing an independent full iteration over all collections and/or all replicas. Many of these contain nested loops, and one (`downgrade_warmup_capabilities`) walks the dependency graph per collection, making it quadratic. The work is never amortized or deduplicated across sub-methods.

Here is the complete `maintain()` call chain and the per-call cost of each sub-method:

```rust
pub fn maintain(&mut self) {
    self.rehydrate_failed_replicas();        // O(R) — scan replicas
    self.downgrade_warmup_capabilities();    // O(C × D) — for each collection, walk dependency graph
    self.forward_implied_capabilities();     // O(C × D_transitive) — for each collection, walk TRANSITIVE deps
    self.schedule_collections();             // O(C × D) — for each collection, check dep frontiers
    self.cleanup_collections();              // O(C × R) — for each collection, check all replicas
    self.update_frontier_introspection();    // O(C + C×R) — scan collections, then replicas × collections
    self.refresh_state_metrics();            // O(C + R) — count unscheduled/connected
    self.refresh_wallclock_lag();            // O(C + C×R) — scan collections, then replicas × collections
}
```

Where C = number of collections, R = number of replicas, D = average dependency fan-out per collection.

### Detailed Cost Analysis Per Sub-Method

**1. `downgrade_warmup_capabilities()` (lines 2116-2143) — O(C × D)**

For every collection, calls `dependency_write_frontiers(collection)` (line 2130) which iterates the collection's compute + storage dependencies to fetch their write frontiers:

```rust
for (id, collection) in &self.collections {     // C iterations
    for frontier in self.dependency_write_frontiers(collection) {  // D iterations per collection
        for time in frontier {
            new_capability.insert(time.step_back().unwrap_or(time));
        }
    }
    new_capabilities.insert(*id, new_capability);
}
```

Then a second pass applies the new capabilities (lines 2139-2142). Total: 2 full passes over collections + D inner iterations.

**2. `forward_implied_capabilities()` (lines 2172-2205) — O(C × C) worst case**

For every collection, calls `transitive_storage_dependency_write_frontiers(collection)` (line 2191) which performs a **full graph traversal** via a BFS/DFS-style loop:

```rust
fn transitive_storage_dependency_write_frontiers(&self, collection: &CollectionState<T>) -> ... {
    let mut storage_ids: BTreeSet<_> = collection.storage_dependency_ids().collect();
    let mut todo: Vec<_> = collection.compute_dependency_ids().collect();
    let mut done = BTreeSet::new();
    while let Some(id) = todo.pop() {        // Walk entire transitive compute dep graph
        if let Some(dep) = self.collections.get(&id) {
            storage_ids.extend(dep.storage_dependency_ids());
            todo.extend(dep.compute_dependency_ids());
        }
        done.insert(id);
    }
    // ... then fetch frontiers for all storage_ids
}
```

This graph walk is **repeated from scratch for every collection** in `forward_implied_capabilities`. If collections form a deep dependency chain (A → B → C → D → ...), the transitive walk for A traverses the entire chain. Across all C collections, total work is O(C × average_transitive_deps). In the worst case of a linear dependency chain of length C, this is **O(C²)**.

Even though `forward_implied_capabilities` returns early when `!self.replicas.is_empty()` (line 2176), paused/scaled-to-zero clusters are a real production scenario (cost optimization), and they'll have the most collections (all indexes remain as collections).

**3. `schedule_collections()` (lines 1479-1484) — O(C × D)**

Iterates all collections and calls `maybe_schedule_collection(id)` for each, which:
- Checks `collection.scheduled` (early exit for already-scheduled — good)
- But for unscheduled: iterates all compute deps, then calls `self.storage_collections.collections_frontiers(storage_deps.collect())` gathering a Vec of all storage dep IDs

The early exit means steady-state cost is O(C) (just the scheduled check), but during startup or after adding many collections (e.g., a large dbt deployment), **all collections are unscheduled simultaneously**, making this O(C × D).

**4. `cleanup_collections()` (lines 745-762) — O(C × R)**

```rust
self.collections_iter()
    .filter(|(id, collection)| {
        collection.dropped
            && collection.shared.lock_read_capabilities(|c| c.is_empty())
            && self.replicas.values().all(|r| r.collection_frontiers_empty(*id))  // O(R) per collection
    })
```

For each dropped collection, checks every replica. In steady state, few collections are dropped, but during large teardowns (e.g., dropping a schema with 500 views), this is O(C × R).

**5 & 6. `update_frontier_introspection()` + `refresh_wallclock_lag()` — O(C + C×R) each**

Both methods perform the same structural pattern: first iterate all collections, then iterate all replicas × per-replica collections:

```rust
// update_frontier_introspection (lines 416-428):
for collection in self.collections.values_mut() { ... }       // O(C)
for replica in self.replicas.values_mut() {                     // O(R)
    for collection in replica.collections.values_mut() { ... }  // O(C) per replica
}

// refresh_wallclock_lag (lines 506-553):
for (id, collection) in &mut self.collections { ... }           // O(C)
for replica in self.replicas.values_mut() {                     // O(R)
    for (id, collection) in &mut replica.collections { ... }    // O(C) per replica
}
```

These two methods have **identical iteration patterns** but are separate loops, meaning the per-replica collection map is traversed twice.

### Total Cost Per `maintain()` Call

| Sub-method | Cost | Steady-state (1000 collections, 5 replicas) |
|---|---|---|
| `rehydrate_failed_replicas` | O(R) | 5 |
| `downgrade_warmup_capabilities` | O(C × D) | 1,000 × ~3 = 3,000 |
| `forward_implied_capabilities` | O(C × D_transitive) | 1,000 × ~10 = 10,000 (paused clusters only) |
| `schedule_collections` | O(C) steady / O(C×D) startup | 1,000 |
| `cleanup_collections` | O(C × R) worst | 1,000 × 5 = 5,000 (during teardown) |
| `update_frontier_introspection` | O(C + C×R) | 1,000 + 5,000 = 6,000 |
| `refresh_state_metrics` | O(C + R) | 1,005 |
| `refresh_wallclock_lag` | O(C + C×R) | 1,000 + 5,000 = 6,000 |
| **Total per second** | | **~27,000+ iterations** |

With 5,000 collections (realistic for large dbt deployments with many MVs + indexes + subsources) and 10 replicas:

| Sub-method | Cost |
|---|---|
| `downgrade_warmup_capabilities` | 15,000 |
| `update_frontier_introspection` | 55,000 |
| `refresh_wallclock_lag` | 55,000 |
| `cleanup_collections` | 50,000 (teardown) |
| **Total per second** | **~180,000+ iterations** |

### Root Cause

Each sub-method was written independently as a self-contained maintenance task, without considering the aggregate cost of running all 7 sequentially. This "separation of concerns" pattern is clean architecturally but means:

1. **No data sharing**: `update_frontier_introspection` and `refresh_wallclock_lag` iterate the same replica-collection maps independently — they could share a single pass
2. **No caching**: `downgrade_warmup_capabilities` and `forward_implied_capabilities` both compute dependency frontiers but don't share results. The transitive dependency walk in `forward_implied_capabilities` creates fresh `BTreeSet` + `Vec` allocations per collection
3. **No incrementality**: All sub-methods recompute from scratch every second rather than tracking what changed since the last `maintain()` call. Most seconds, only a handful of collections have frontier changes, but all 7 methods scan everything

### Suggested Fix

**Option A (single-pass fusion — moderate change)**: Merge `update_frontier_introspection` and `refresh_wallclock_lag` into a single `update_introspection_and_lag()` method that iterates collections and replica-collections once:

```rust
fn update_introspection_and_lag(&mut self) {
    // Single pass over instance-level collections
    for (id, collection) in &mut self.collections {
        collection.introspection.observe_frontiers(...);
        if let Some(stash) = &mut collection.wallclock_lag_histogram_stash {
            // wallclock lag histogram logic
        }
    }
    // Single pass over per-replica collections
    for replica in self.replicas.values_mut() {
        for (id, collection) in &mut replica.collections {
            collection.introspection.observe_frontier(...);
            // wallclock lag history logic
        }
    }
}
```

This eliminates one full O(C + C×R) scan per second.

**Option B (incremental warmup/capability downgrade — larger change)**: Instead of scanning all collections in `downgrade_warmup_capabilities`, maintain a "dirty set" of collection IDs whose dependency frontiers have changed. Only recompute warmup capabilities for dirty collections. Since frontier updates come through `handle_frontier_update`, the dirty set can be populated there:

```rust
fn handle_frontier_update(&mut self, id: GlobalId, new_frontier: Antichain<T>) {
    // ... existing logic ...
    // Mark reverse-dependents as needing warmup recalculation
    for dependent in self.reverse_dependencies.get(&id) {
        self.warmup_dirty.insert(*dependent);
    }
}
```

This requires building a reverse-dependency index (one-time O(C × D) at startup), but turns per-second work from O(C × D) to O(changed × D).

**Option C (cached transitive dependencies)**: Pre-compute and cache the transitive storage dependency set per collection (a `BTreeMap<GlobalId, BTreeSet<GlobalId>>`). Invalidate on collection add/remove. This turns `transitive_storage_dependency_write_frontiers` from O(D_transitive) graph walk to O(D_transitive) frontier lookups (no graph walk), and the cache can be shared between `forward_implied_capabilities` and any other method needing transitive deps.

**Option D (combined: fuse + incremental)**: Apply A + B + C together. This would reduce steady-state `maintain()` from ~27,000 iterations to ~5,000 (only metrics + the handful of dirty collections), a **5× improvement**.

### Files Involved
- `src/compute-client/src/controller/instance.rs` — `maintain()` (lines 2233-2242), `downgrade_warmup_capabilities` (lines 2116-2143), `forward_implied_capabilities` (lines 2172-2205), `transitive_storage_dependency_write_frontiers` (lines 2077-2102), `dependency_write_frontiers` (lines 2060-2074), `schedule_collections` (lines 1479-1484), `cleanup_collections` (lines 745-762), `update_frontier_introspection` (lines 415-429), `refresh_state_metrics` (lines 439-469), `refresh_wallclock_lag` (lines 489-553)

## Session 26: Uncached Index Lookup via Full Dependent Scan — O(D) Per Lookup × 3+ Lookups Per Query (2026-03-09)

### Location
`src/adapter/src/catalog/state.rs:1519-1539` — `get_indexes_on()`
`src/adapter/src/coord/indexes.rs:33-95` — `sufficient_collections()` and `DataflowBuilder::indexes_on()`
`src/adapter/src/optimize/dataflows.rs:310-345` — `import_into_dataflow()`
`src/transform/src/dataflow.rs:584-1071` — `prune_and_annotate_dataflow_index_imports()`
`src/transform/src/join_implementation.rs:565` — join key lookup
`src/transform/src/literal_constraints.rs:300` — literal constraint index matching

### Problem

Finding indexes on a collection requires **scanning ALL dependents** of that collection (not just indexes), and this scan is performed **3+ times per query** across different optimization phases with **no caching**.

**The core `get_indexes_on` function** (state.rs:1519-1539):

```rust
pub fn get_indexes_on(
    &self,
    id: GlobalId,
    cluster: ClusterId,
) -> impl Iterator<Item = (GlobalId, &Index)> {
    let index_matches = move |idx: &Index| idx.on == id && idx.cluster_id == cluster;

    self.try_get_entry_by_global_id(&id)
        .into_iter()
        .map(move |e| {
            e.used_by()          // ALL dependents: views, MVs, indexes, sinks, CTs...
                .iter()
                .filter_map(move |uses_id| match self.get_entry(uses_id).item() {
                    CatalogItem::Index(index) if index_matches(index) => {
                        Some((index.global_id(), index))
                    }
                    _ => None,   // Skips non-index dependents (the majority!)
                })
        })
        .flatten()
}
```

This iterates `entry.used_by()` — a `Vec<CatalogItemId>` containing **every object that depends on this collection**: views, materialized views, other indexes, sinks, continual tasks, etc. For a popular base table, this Vec can contain hundreds of entries, but only a handful are indexes. Every non-index entry is fetched from the catalog (`self.get_entry(uses_id)`) only to be discarded.

**The same collection is queried 3+ times per SELECT query** across different optimization phases:

**Phase 1 — `sufficient_collections`** (indexes.rs:33-88, called at peek.rs:460):
```rust
while let Some(id) = todo.iter().rev().next().cloned() {
    let mut available_indexes = self.indexes_on(id).map(|(id, _)| id).peekable();
    // ... if no indexes, recursively add view dependencies to todo
}
```
This walks the dependency graph, calling `indexes_on(id)` for every node visited. For a query through N nested views with no indexes, it visits all N views before reaching the base tables.

**Phase 2 — `import_into_dataflow`** (dataflows.rs:310-345):
```rust
let mut valid_indexes = self.indexes_on(*id).peekable();
if valid_indexes.peek().is_some() {
    for (index_id, idx) in valid_indexes {
        // Import ALL indexes on the collection, even though most will be pruned later
        dataflow.import_index(index_id, ...);
    }
}
```
For the same collection IDs already checked in `sufficient_collections`, `indexes_on` is called again to import indexes into the dataflow.

**Phase 3 — `prune_and_annotate_dataflow_index_imports`** (dataflow.rs:584-1071):
Called 3 more times within a single function:
- Line 584: for sink imports
- Line 610: for index exports
- Line 944: in `pick_index_for_full_scan` closure, which **collects all indexes into a Vec with cloned keys**:
```rust
let pick_index_for_full_scan = |on_id: &GlobalId| {
    choose_index(
        this.source_keys,
        on_id,
        &this.indexes_available.indexes_on(*on_id)
            .map(|(idx_id, key)| (idx_id, key.iter().cloned().collect_vec()))  // Clone all keys
            .collect_vec(),  // Materialize into Vec
    )
};
```
- Line 1071: for arrangement matching per Get expression

**Phase 4 — Transform passes** (join_implementation.rs:565, literal_constraints.rs:300):
```rust
// join_implementation.rs:565 — called for EVERY collection in a join
Id::Global(id) => Box::new(self.global.indexes_on(id).map(|(_idx_id, key)| key)),

// literal_constraints.rs:300 — called for every Get with literal predicates
.indexes_on(get_id)
    .map(|(index_id, key)| (index_id, key.to_owned(), match_index(key, &or_args)))
    .collect_vec();  // Collects + clones all keys
```

### Total Cost Per Query

For a SELECT joining 3 tables, each with 50 dependents (views, MVs, indexes) and 5 actual indexes:

| Phase | Calls to `indexes_on` | Dependents scanned per call | Catalog lookups | Key clones |
|---|---|---|---|---|
| `sufficient_collections` | 3 | 50 each = 150 | 150 | 0 |
| `import_into_dataflow` | 3 | 50 each = 150 | 150 | 15 (Vec to_vec) |
| `prune_and_annotate` | 3-6 | 50 each = 150-300 | 150-300 | 15-30 |
| `join_implementation` | 3 | 50 each = 150 | 150 | 0 |
| `literal_constraints` | 0-3 | 50 each = 0-150 | 0-150 | 0-15 |
| **Total** | **12-18** | **600-900** | **600-900** | **30-60** |

With a popular table referenced by 200 objects (realistic for a dbt deployment with many downstream views), each call scans 200 dependents, making the total **2,400-3,600 catalog entry fetches per query** just for index lookup.

### Additional Issue: Key Cloning in `pick_index_for_full_scan`

The `choose_index` helper (dataflow.rs:845-860) receives a `Vec<(GlobalId, Vec<MirScalarExpr>)>` — all index keys pre-cloned into owned Vecs. But it only returns one index. The other N-1 index keys are cloned and immediately discarded:

```rust
fn choose_index(
    source_keys: &BTreeMap<GlobalId, BTreeSet<Vec<MirScalarExpr>>>,
    id: &GlobalId,
    indexes: &Vec<(GlobalId, Vec<MirScalarExpr>)>,
) -> Option<(GlobalId, Vec<MirScalarExpr>)> {
    match source_keys.get(id) {
        None => indexes.iter().next().cloned(),  // Picks arbitrary, clones again
        Some(coll_keys) => match indexes
            .iter()
            .find(|(_idx_id, key)| coll_keys.contains(&*key))  // O(N*M) matching
        {
            Some((idx_id, key)) => Some((*idx_id, key.clone())),  // Found match, clones again
            None => indexes.iter().next().cloned(),  // Fallback to arbitrary
        },
    }
}
```

The key is cloned up to 3 times: once when collecting into the Vec (line 945), once inside `choose_index` when returning the result, and once more if the caller needs to store it.

### Root Cause

The `CatalogState` has no secondary index mapping collections to their indexes. The only way to find indexes on a collection is through the general-purpose `used_by` Vec, which contains all dependents regardless of type. Combined with the lack of any caching layer across optimization phases, the same expensive scan is repeated multiple times per query.

### Suggested Fix

**Option A (secondary index — simple, high impact)**: Add an `indexes_by_on: BTreeMap<GlobalId, Vec<(GlobalId, ClusterId)>>` to `CatalogState`. Maintain it in `insert_entry`/`drop_item`. This turns `get_indexes_on` from O(D) scan (D = all dependents) to O(I) iteration (I = only indexes on this collection). Typically I << D.

```rust
pub fn get_indexes_on(
    &self,
    id: GlobalId,
    cluster: ClusterId,
) -> impl Iterator<Item = (GlobalId, &Index)> {
    self.indexes_by_on
        .get(&id)
        .into_iter()
        .flatten()
        .filter(move |(_, c)| *c == cluster)
        .filter_map(|(idx_id, _)| {
            let entry = self.entry_by_global_id.get(idx_id)?;
            match entry.item() {
                CatalogItem::Index(idx) => Some((*idx_id, idx)),
                _ => None,
            }
        })
}
```

**Option B (per-query index cache — moderate change)**: Create an `IndexCache` struct that caches `indexes_on` results per GlobalId during a single optimization pass. Pass it through the optimization pipeline so `sufficient_collections`, `import_into_dataflow`, and `prune_and_annotate` share results:

```rust
struct IndexCache<'a> {
    catalog: &'a CatalogState,
    cluster: ClusterId,
    cache: BTreeMap<GlobalId, Vec<(GlobalId, &'a Index)>>,
}

impl<'a> IndexCache<'a> {
    fn indexes_on(&mut self, id: GlobalId) -> &[(GlobalId, &'a Index)] {
        self.cache.entry(id).or_insert_with(|| {
            self.catalog.get_indexes_on(id, self.cluster).collect()
        })
    }
}
```

**Option C (both)**: Apply A + B together. A reduces per-call cost from O(D) to O(I). B eliminates redundant calls. Combined, for 3 tables with 200 dependents and 5 indexes each, this reduces from ~3,000 catalog lookups to ~15 (5 indexes × 3 tables, computed once and cached).

### Files Involved
- `src/adapter/src/catalog/state.rs` — `get_indexes_on()` (lines 1519-1539), where the secondary index would be added
- `src/adapter/src/catalog/apply.rs` — `insert_entry()` (line 1714) and `drop_item()` (line 1799), where the secondary index would be maintained
- `src/adapter/src/coord/indexes.rs` — `sufficient_collections()` (lines 33-88), `DataflowBuilder::indexes_on()` (lines 90-95), `IndexOracle` impl (lines 98-108)
- `src/adapter/src/optimize/dataflows.rs` — `import_into_dataflow()` (lines 310-345), where index imports are built
- `src/transform/src/dataflow.rs` — `prune_and_annotate_dataflow_index_imports()` (lines 570-1120), `choose_index()` (lines 845-860), `pick_index_for_full_scan` closure (lines 936-948)
- `src/transform/src/join_implementation.rs` — line 565, join key lookup
- `src/transform/src/literal_constraints.rs` — line 300, literal constraint matching

## Session 27: ReadHold Clone Amplification in create_dataflow — O(E × (S + I) + E × R × S) Channel Messages Per Dataflow (2026-03-09)

### Location
`src/compute-client/src/controller/instance.rs:1166-1253` — `Instance::create_dataflow()`
`src/compute-client/src/controller/instance.rs:261-311` — `Instance::add_collection()`
`src/storage-types/src/read_holds.rs:167-195` — `ReadHold::clone()`

### Problem

When a new dataflow is created (every `CREATE INDEX`, `CREATE MATERIALIZED VIEW`, `SELECT` peek, `SUBSCRIBE`), the `create_dataflow` function clones `ReadHold` objects in a pattern that causes **multiplicative amplification of channel messages**.

**`ReadHold::clone()` is NOT cheap** (read_holds.rs:167-195):

```rust
impl<T: TimelyTimestamp> Clone for ReadHold<T> {
    fn clone(&self) -> Self {
        let mut changes = ChangeBatch::new();                    // allocation
        changes.extend(self.since.iter().map(|t| (t.clone(), 1))); // populate
        if !changes.is_empty() {
            match (self.change_tx)(self.id.clone(), changes) {   // CHANNEL SEND
                Ok(_) => (),
                Err(e) => panic!("cannot clone ReadHold: {}", e),
            }
        }
        Self {
            id: self.id.clone(),
            since: self.since.clone(),
            change_tx: Arc::clone(&self.change_tx),
        }
    }
}
```

Each clone: (1) allocates a `ChangeBatch`, (2) populates it from the antichain, (3) sends it through an `Arc<dyn Fn>` which calls `UnboundedSender::send()`. This is significantly more expensive than a simple struct copy.

**Clone amplification in `create_dataflow` (instance.rs:1228-1253):**

```rust
for export_id in dataflow.export_ids() {
    // ...
    self.add_collection(
        export_id,
        as_of.clone(),
        shared,
        storage_dependencies.clone(),    // Clones S ReadHolds → S channel messages
        compute_dependencies.clone(),    // Clones I ReadHolds → I channel messages
        replica_input_read_holds.clone(), // Clones S ReadHolds → S channel messages
        // ...
    );
}
```

For each of E exports, this clones the entire `storage_dependencies` BTreeMap (S entries), `compute_dependencies` BTreeMap (I entries), and `replica_input_read_holds` Vec (S entries). Each entry is a `ReadHold` whose clone sends a channel message.

**Further amplification in `add_collection` (instance.rs:307-310):**

```rust
for replica in self.replicas.values_mut() {
    replica.add_collection(id, as_of.clone(), replica_input_read_holds.clone());
}
```

For each replica R, the `replica_input_read_holds` (S entries) is cloned again.

### Total Channel Messages Per Dataflow

**Per dataflow: E × (2S + I) + E × R × S**

Where:
- E = number of exports (index exports + sink exports)
- S = source imports (storage dependencies)
- I = index imports (compute dependencies)
- R = number of replicas

### Scaling Impact

**Example: Materialized view joining 20 source tables on a cluster with 3 replicas:**
- E = 1 (the MV itself, though it has both a compute sink and storage collection)
- S = 20, I = 0, R = 3
- Channel messages = 1 × (40 + 0) + 1 × 3 × 20 = **100 channel messages**

**Example: Complex peek query importing 50 sources + 10 indexes, 5 replicas:**
- E = 1 (the peek subscribe)
- S = 50, I = 10, R = 5
- Channel messages = 1 × (100 + 10) + 1 × 5 × 50 = **360 channel messages**

**Example: Source with 100 subsource tables, creating indexes on all of them:**
- Each CREATE INDEX creates a separate dataflow, but the subsource tables generate S = 100
- With 5 replicas: 100 indexes × (200 + 0 + 5 × 100) = **70,000 channel messages**

Each channel message involves:
1. `ChangeBatch::new()` — heap allocation
2. Antichain iteration and element cloning
3. `mpsc::UnboundedSender::send()` — synchronization overhead
4. Receiver processing of the `ChangeBatch`

All of this runs on the **coordinator's main thread**, blocking other operations.

### Root Cause

The per-export, per-replica cloning of dependency ReadHolds is structurally required because each collection independently tracks its dependencies for compaction purposes. However, the implementation creates **separate ReadHold instances** where **shared ReadHolds with reference counting** would suffice.

The core insight is: all exports of the same dataflow share the same `as_of` and the same set of dependencies. They don't need independent ReadHolds — they need to share a single set of ReadHolds and only release them when ALL exports are dropped.

### Suggested Fix

**Option A (shared dependency ReadHolds — moderate change)**: Instead of cloning ReadHolds per export, wrap the dependency maps in `Arc`:

```rust
let storage_deps = Arc::new(storage_dependencies);
let compute_deps = Arc::new(compute_dependencies);
let replica_holds = Arc::new(replica_input_read_holds);

for export_id in dataflow.export_ids() {
    self.add_collection(
        export_id,
        as_of.clone(),
        shared,
        Arc::clone(&storage_deps),     // O(1) Arc bump, no channel messages
        Arc::clone(&compute_deps),     // O(1) Arc bump
        Arc::clone(&replica_holds),    // O(1) Arc bump
        // ...
    );
}
```

This requires changing `CollectionState` to hold `Arc<BTreeMap<GlobalId, ReadHold<T>>>` for dependencies. The ReadHolds are only dropped when the last collection referencing them is dropped.

**Option B (batch ReadHold acquisition — lower change)**: Instead of cloning existing ReadHolds, acquire separate holds in bulk. The `acquire_read_holds` API could be extended to accept a count parameter, generating N independent holds in a single batch operation with one consolidated `ChangeBatch` message instead of N separate messages.

**Option C (deferred hold registration)**: Accumulate all the `ChangeBatch` updates that would be generated by cloning, then send them as a single consolidated batch at the end of `create_dataflow`. This reduces N channel messages to 1 while keeping the current data model.

### Files Involved
- `src/compute-client/src/controller/instance.rs` — `create_dataflow()` (lines 1166-1253), `add_collection()` (lines 261-311)
- `src/storage-types/src/read_holds.rs` — `ReadHold::clone()` (lines 167-195), `ReadHold::drop()` (lines 197+)

## Session 28: Storage Controller `maintain()` — 3× Full-Collection Mutex Lock+Clone Per Second (2026-03-09)

### Location
`src/storage-client/src/storage_collections.rs:1436-1451` — `active_collection_frontiers()`
`src/storage-controller/src/lib.rs:3548-3641` — `update_frontier_introspection()`
`src/storage-controller/src/lib.rs:3661-3716` — `refresh_wallclock_lag()`
`src/storage-controller/src/lib.rs:3725-3799` — `maybe_record_wallclock_lag()`

### Problem

The storage controller's `maintain()` method (called ~once per second) calls `active_collection_frontiers()` **3 separate times**, each of which:

1. **Acquires a `std::sync::Mutex` lock** on the entire `BTreeMap<GlobalId, CollectionState<T>>` (line 1437)
2. **Iterates ALL collections** in the BTreeMap (line 1440)
3. **Clones 3 `Antichain` values per collection**: `write_frontier`, `implied_capability`, and `read_capabilities.frontier().to_owned()` (lines 1443-1446)
4. **Collects into a `Vec<CollectionFrontiers<T>>`** (line 1448)
5. Drops the Mutex lock

The three call sites per `maintain()` cycle:

```rust
// Call 1: update_frontier_introspection() — line 3552
for collection_frontiers in self.storage_collections.active_collection_frontiers() {
    // Builds global_frontiers BTreeMap + replica_frontiers BTreeMap
    // Then diffs against previous recorded_frontiers
}

// Call 2: refresh_wallclock_lag() — line 3671
for frontiers in self.storage_collections.active_collection_frontiers() {
    // Computes wallclock lag per collection, updates metrics
}

// Call 3: maybe_record_wallclock_lag() — line 3751 (conditional, ~once/minute)
for frontiers in self.storage_collections.active_collection_frontiers() {
    // Records lag history updates to introspection collections
}
```

Additionally, `active_ingestion_exports()` (line 500-505) calls `active_collection_frontiers()` and then builds **another** intermediate `BTreeMap` from the results just to check export active status:

```rust
fn active_ingestion_exports(&self, instance_id: StorageInstanceId) -> Box<dyn Iterator<Item = &GlobalId> + '_> {
    let active_storage_collections: BTreeMap<_, _> = self
        .storage_collections
        .active_collection_frontiers()  // Lock + iterate + clone ALL
        .into_iter()
        .map(|c| (c.id, c))
        .collect();  // Build entire BTreeMap just to check membership
    // ...
}
```

### Scaling Analysis

**Total cost per maintain() cycle (once per second):**

With C = total storage collections (active + dropped filtered out):

| Operation | Per Call | Calls/sec | Total/sec |
|-----------|---------|-----------|-----------|
| Mutex lock acquisition | O(1) | 3 | 3 |
| BTreeMap full iteration | O(C) | 3 | O(3C) |
| Antichain clones | O(C × 3) | 3 | O(9C) |
| Vec allocation | O(C) | 3 | O(3C) |

**Real-world impact:**
- With 1,000 collections: 9,000 Antichain clones + 3,000 BTreeMap node traversals per second
- With 10,000 collections (common with subsources): 90,000 Antichain clones + 30,000 BTreeMap node traversals per second
- With 50,000 collections: 450,000 Antichain clones per second

Each `Antichain` clone involves:
- `Vec<T>` heap allocation (for `write_frontier` and `implied_capability`)
- `MutableAntichain::frontier().to_owned()` — computes the frontier from the mutable antichain's internal state, then allocates a new `Antichain` (for `read_capabilities`)

The `std::sync::Mutex` (not `tokio::sync::Mutex`) means the lock is blocking — holding it while iterating 10,000+ entries blocks any concurrent access from the background task that processes read capability changes (line 2838) and write frontier updates (line 2868).

### Amplification in `update_frontier_introspection()`

After cloning all collection frontiers, `update_frontier_introspection()` does additional O(C × R) work:

```rust
for collection_frontiers in self.storage_collections.active_collection_frontiers() {
    // ... lookup instance ...
    if let Some(instance) = instance {
        for replica_id in instance.replica_ids() {
            replica_frontiers.insert((id, replica_id), upper.clone());  // Line 3569
        }
    }
    global_frontiers.insert(id, (since, upper));  // Line 3573
}
```

This creates a `BTreeMap<(GlobalId, ReplicaId), Antichain<T>>` with C × R entries. Then it diffs the entire map against the previous `recorded_replica_frontiers` (lines 3618-3632), requiring additional clones for changed entries.

With 10,000 collections × 5 replicas = 50,000 BTreeMap entries built, diffed, and swapped every second.

### Root Cause

The `StorageCollectionsImpl` uses a `Mutex<BTreeMap>` as its only interface for accessing collection state, with no way to:
1. **Iterate without cloning** — the Mutex forces callers to clone everything out before releasing the lock
2. **Access only changed collections** — there's no change tracking, so every caller must scan everything
3. **Share a single snapshot** — each `maintain()` sub-function independently locks, clones, and releases

### Suggested Fix

**Option A (single snapshot per maintain cycle — simple)**: Call `active_collection_frontiers()` once at the start of `maintain()` and pass the resulting `Vec` to all sub-functions:

```rust
fn maintain(&mut self) {
    let frontiers = self.storage_collections.active_collection_frontiers();
    self.update_frontier_introspection(&frontiers);
    self.refresh_wallclock_lag(&frontiers);
    // maybe_record_wallclock_lag can reuse too
}
```

This immediately reduces lock acquisitions from 3 to 1 and eliminates 2/3 of the cloning. No architectural changes required.

**Option B (incremental change tracking — moderate)**: Maintain a `changed_since_last_maintain: BTreeSet<GlobalId>` that is updated whenever `write_frontier` or `read_capabilities` change. Then `maintain()` only processes changed collections:

```rust
fn update_frontier_introspection(&mut self) {
    let changed = std::mem::take(&mut self.frontier_changes);
    for id in &changed {
        // Only clone frontiers for collections that actually changed
        let frontiers = self.storage_collections.collection_frontiers(*id)?;
        // Update recorded_frontiers incrementally
    }
}
```

This reduces work from O(C) to O(Δ) where Δ = collections with frontier changes since last tick.

**Option C (lock-free frontier access — larger change)**: Replace the `Mutex<BTreeMap>` with a lock-free data structure or use `Arc<Antichain>` for frontiers so they can be read without cloning:

```rust
struct CollectionState<T> {
    write_frontier: Arc<Antichain<T>>,
    implied_capability: Arc<Antichain<T>>,
    // ...
}
```

Readers get O(1) `Arc::clone()` instead of heap-allocating Antichain copies.

### Files Involved
- `src/storage-client/src/storage_collections.rs` — `active_collection_frontiers()` (line 1436), `active_collection_metadatas()` (line 1398), `CollectionState` (line 2556), Mutex field (line 392)
- `src/storage-controller/src/lib.rs` — `maintain()` (line 3805), `update_frontier_introspection()` (line 3548), `refresh_wallclock_lag()` (line 3661), `maybe_record_wallclock_lag()` (line 3725), `active_ingestion_exports()` (line 496)
- `src/storage-client/src/storage_collections.rs:344-362` — `CollectionFrontiers` struct definition
- `src/compute-client/src/controller/replica.rs` — `add_collection()` (clones replica_input_read_holds per replica)

## Session 29: Redundant SQL Parsing in Catalog Update Topological Sort — O(N × parse_time) Per Batch DDL (2026-03-09)

### Location
- `src/adapter/src/catalog/apply.rs:2089-2101` — `sort_items_topological()`
- `src/adapter/src/catalog/apply.rs:2118-2181` — `sort_item_updates()`
- `src/adapter/src/util.rs:487-527` — `sort_topological()`

### Problem

Every catalog transaction that modifies catalog items goes through `apply_updates()` → `sort_updates()` → `sort_item_updates()`. Within `sort_item_updates`, the code calls `sort_items_topological()` on two groups of items — **connections** (line 2154) and **derived items** (views, MVs, indexes — line 2155) — for both retractions AND additions (lines 2183-2184).

`sort_items_topological()` discovers each item's dependencies by **re-parsing its SQL from scratch**:

```rust
fn sort_items_topological(items: &mut Vec<(mz_catalog::durable::Item, Timestamp, StateDiff)>) {
    let key_fn = |item: &(mz_catalog::durable::Item, _, _)| item.0.id;
    let dependencies_fn = |item: &(mz_catalog::durable::Item, _, _)| {
        let statement = mz_sql::parse::parse(&item.0.create_sql)  // FULL SQL PARSE per item
            .expect("valid create_sql")
            .into_element()
            .ast;
        mz_sql::names::dependencies(&statement)                    // AST WALK per item
            .expect("failed to find dependencies of item")
    };
    sort_topological(items, key_fn, dependencies_fn);
}
```

Then in `sort_topological()` (util.rs:509-510), `dependencies_fn` is called for **every item** in the set:

```rust
for (&key, item) in &items_by_key {
    let mut dependencies = dependencies_fn(item);  // Parses SQL for each item
    dependencies.retain(|dep| items_by_key.contains_key(dep) && *dep != key);
    // ...
}
```

This means for every item in the connections + derived_items groups, the code:
1. Invokes the full SQL parser on `create_sql` (tokenization + parsing + AST construction)
2. Walks the resulting AST to extract `CatalogItemId` references

### Why This Is Redundant

The dependency information extracted here is **already available** through two other mechanisms:

1. **For retractions**: The items still exist in the in-memory `CatalogState`. Each `CatalogEntry` has `references()`, `uses()`, `referenced_by()`, and `used_by()` fields that already contain the resolved dependency graph. The code could simply look up dependencies from the catalog.

2. **For additions**: The same SQL will be parsed **again** in `deserialize_item()` → `parse_plan()` (state.rs:1004-1014) when the item is actually applied to the catalog. So the parse done for sorting is strictly redundant — the SQL gets parsed twice.

### Scaling Impact

- **Normal DDL (1-2 items)**: Negligible — just 1-2 parses
- **DROP OWNED BY role_with_N_objects**: Parses SQL for every connection + derived item owned by that role, twice (once for retraction sort, once for the deserialization that follows)
- **DROP CASCADE on heavily-depended object**: Parses SQL for every transitively dependent view/MV/index
- **Catalog migrations / upgrades**: When many system objects are updated, parses SQL for every updated connection + derived item
- **ALTER SCHEMA / RENAME operations** that cascade: Retracts + re-adds every item in the affected schema

For a `DROP OWNED BY` affecting 500 views/MVs, this means 500 full SQL parses just for dependency sorting, then another 500 parses for deserialization — totaling ~1000 SQL parse operations where 0 should be needed (since the catalog already has the dependency graph).

### Root Cause

The `sort_updates` function operates on raw `StateUpdate`s which only contain `create_sql` strings, not parsed catalog entries. It doesn't have access to the in-memory catalog's dependency graph. This is an architectural layering issue: the sorting happens before the updates are applied to the catalog, but it needs information that the catalog already maintains.

### Suggested Fix

**Option A (use catalog for retraction deps — simple)**: Pass a reference to `CatalogState` into `sort_item_updates`. For retractions, look up dependencies from `entry.references()` / `entry.uses()` instead of parsing SQL. For additions, the item may not exist yet, but the SQL IDs in `create_sql` can be extracted with a lighter-weight regex/ID scan rather than a full parse.

**Option B (store dependency IDs in durable Item — moderate)**: Add a `dependency_ids: BTreeSet<CatalogItemId>` field to the durable `Item` struct. This eliminates the need to parse SQL for dependency discovery entirely. The field would be populated when items are first created and maintained through ALTER operations.

**Option C (defer sorting to after parse — architectural)**: Restructure `apply_updates` so that items are first deserialized (parsed) and then sorted using the already-resolved dependency information from the `CatalogItem`. This eliminates the double-parse entirely, though it requires restructuring the apply pipeline.

### Files Involved
- `src/adapter/src/catalog/apply.rs:2089-2101` — `sort_items_topological()` where SQL is parsed for dependency discovery
- `src/adapter/src/catalog/apply.rs:2118-2184` — `sort_item_updates()` which calls topological sort on connections + derived items, for both retractions and additions
- `src/adapter/src/catalog/apply.rs:99-153` — `apply_updates()` entry point that calls `sort_updates`
- `src/adapter/src/util.rs:487-527` — `sort_topological()` generic helper that calls `dependencies_fn` per item
- `src/adapter/src/catalog/state.rs:1004-1014` — `parse_plan()` which parses the same SQL again during `deserialize_item`
- `src/sql/src/names.rs:2590-2600` — `dependencies()` function that walks AST to extract IDs

## Session 30: Double `optimize_orders` Computation + Double Join Clone in Multiway Join Planning (2026-03-09)

### Location
`src/transform/src/join_implementation.rs:460-523` — multiway join planning in the `JoinImplementation` MIR transform

### Problem

For every multiway join (>2 inputs), the join implementation transform computes join orderings **twice** and clones the entire join expression **twice** — once for delta query planning and once for differential join planning. A TODO comment at line 460-461 explicitly acknowledges this:

```rust
// TODO(mgree): with this refactoring, we should compute `orders` once---both joins
//              call `optimize_orders` and we can save some work.
```

**The double computation chain:**

1. **`differential::plan()`** (line 397-406):
   - Clones the entire `MirRelationExpr::Join` (line 692: `let mut new_join = join.clone()`)
   - Calls `optimize_orders()` (line 709-717) → creates `Orderer`, computes N orderings
   - Processes orderings to choose the best differential plan

2. **`delta_queries::plan()`** (line 462-470):
   - Clones the entire `MirRelationExpr::Join` **again** (line 602: `let mut new_join = join.clone()`)
   - Calls `optimize_orders()` **again** (line 611-618) with identical inputs → creates another `Orderer`, computes the same N orderings
   - Processes orderings to choose the best delta plan

3. The caller then **compares** the two plans and picks one, discarding the other

### Cost of `optimize_orders`

The `optimize_orders` function (line 1014-1035) creates an `Orderer` struct and computes one join ordering for each of N inputs:

```rust
fn optimize_orders(...) -> Result<Vec<Vec<(JoinInputCharacteristics, Vec<MirScalarExpr>, usize)>>, TransformError> {
    let mut orderer = Orderer::new(...);  // O(N × E) setup: builds reverse_equivalences, unique_arrangement
    (0..available.len())                  // N iterations
        .map(move |i| orderer.optimize_order_for(i))  // Each: O(N² log N) priority-queue-based greedy ordering
        .collect::<Result<Vec<_>, _>>()
}
```

Each `Orderer::new()` (lines 1060-1114):
- Builds `reverse_equivalences`: iterates all equivalences × all expressions × all inputs — O(N × E × K)
- Builds `unique_arrangement`: iterates all arrangements × all unique keys — O(A × U)
- Allocates `placed`, `bound`, `equivalences_active`, `arrangement_active` vectors

Each `optimize_order_for()` (lines 1116-1179+):
- Resets all internal state vectors — O(N + E)
- Initializes the priority queue with N entries — O(N log N)
- Runs the main ordering loop N-1 times, each popping from and pushing to the priority queue — O(N log N) per iteration
- Inside each iteration, processes equivalences and arrangements — O(E × A)

**Per-call cost:** O(N × (N log N + E × A))
**Total cost:** 2 × O(N × (N log N + E × A)) — paying the full price twice for identical results

### Join Expression Cloning

Each `plan()` function starts by cloning the entire join expression:

- `delta_queries::plan()` line 602: `let mut new_join = join.clone()`
- `differential::plan()` line 692: `let mut new_join = join.clone()`

A `MirRelationExpr::Join` contains:
- `inputs: Vec<MirRelationExpr>` — the full expression trees of all N join inputs (deep clone)
- `equivalences: Vec<Vec<MirScalarExpr>>` — all equivalence expressions (deep clone)
- `implementation: JoinImplementation` — the implementation plan

For a 10-input join where each input is a filtered table scan, this clones ~10 expression subtrees. One of the two clones is always discarded after comparison.

### Fixpoint Amplification

`JoinImplementation` runs inside a fixpoint loop (lib.rs:854-858):
```rust
Box::new(Fixpoint {
    name: "fixpoint_join_impl",
    limit: 100,
    transforms: vec![Box::new(JoinImplementation::default())],
})
```

On the second fixpoint iteration (lines 367-391), the transform detects the existing implementation and may re-plan:
- For differential plans: calls `delta_queries::plan()` again (line 378) to check if delta is now possible
- This triggers another `optimize_orders()` + join clone

In the worst case with K fixpoint iterations, the total cost is K × (2 clones + 2 × `optimize_orders`).

### Scaling Impact

- **5-input join**: 2 × 5 orderings = 10 `optimize_order_for` calls, 2 full join clones, wasted
- **10-input join (e.g., TPC-H Q2, Q8)**: 2 × 10 = 20 orderings + 2 deep clones of 10 input subtrees
- **15-input join**: 2 × 15 = 30 orderings + exponential ordering cost per call
- With 2 fixpoint iterations: all costs doubled again

The `optimize_order_for` function's inner loop involves priority queue operations, equivalence class matching, and arrangement lookup — it's CPU-intensive, not just a simple iteration. The O(N²) component comes from the main loop (N-1 iterations) × the per-iteration work of updating equivalences and the priority queue.

### Suggested Fix

**Option A (cache orders — simple, as the TODO suggests)**: Compute `optimize_orders()` once before calling both `differential::plan()` and `delta_queries::plan()`. Pass the pre-computed orders as a parameter. This eliminates one full `Orderer` construction and N `optimize_order_for` calls.

**Option B (lazy cloning — moderate)**: Don't clone the join expression upfront in each `plan()` function. Instead, work with references to compute the orderings and arrangement counts, only cloning when the plan is actually selected. This eliminates one of the two deep clones.

**Option C (combined planning — architectural)**: Merge the differential and delta planning into a single function that computes orders once, then evaluates both strategies against the same orderings. This eliminates all redundancy and also enables better comparison logic.

### Files Involved
- `src/transform/src/join_implementation.rs:397-523` — multiway join planning entry point where both plans are computed and compared
- `src/transform/src/join_implementation.rs:595-680` — `delta_queries::plan()` with join clone (line 602) and `optimize_orders()` call (line 611)
- `src/transform/src/join_implementation.rs:680-800` — `differential::plan()` with join clone (line 692) and `optimize_orders()` call (line 709)
- `src/transform/src/join_implementation.rs:1014-1035` — `optimize_orders()` function
- `src/transform/src/join_implementation.rs:1060-1180` — `Orderer` construction and `optimize_order_for()` greedy algorithm
- `src/transform/src/lib.rs:854-858` — fixpoint loop that runs `JoinImplementation` up to 100 times

## Session 31: HIR-to-MIR Lowering `branch()` — Triple Tree Traversal Per Subquery with O(D² × N) Nesting Amplification (2026-03-09)

### Location
`src/sql/src/plan/lowering.rs:1835-1909` — `branch()` triple traversal
`src/sql/src/plan/lowering.rs:95-117` — `ColumnMap::enter_scope()` clone-and-rebuild
`src/sql/src/plan/lowering.rs:1961-1977` — `apply_scalar_subquery()` → `branch()`
`src/sql/src/plan/lowering.rs:2036-2052` — `apply_existential_subquery()` → `branch()`

### Problem

The `branch()` function performs decorrelation of every correlated subquery during HIR-to-MIR lowering. It traverses the inner HIR expression tree **3 separate times** before doing any actual work:

**Traversal 1 — Simplicity check (lines 1835-1844):**
```rust
let mut is_simple = true;
#[allow(deprecated)]
inner.visit(0, &mut |expr, _| match expr {
    HirRelationExpr::Constant { .. }
    | HirRelationExpr::Project { .. }
    | HirRelationExpr::Map { .. }
    | HirRelationExpr::Filter { .. }
    | HirRelationExpr::CallTable { .. } => (),
    _ => is_simple = false,
});
```

This walks **every node** in the inner expression to check if it contains only simple operations. Critically, it **does not short-circuit**: after `is_simple` is set to `false` on the first non-simple node, all remaining nodes are still visited because `visit` uses `visit_fallible` which does a post-order traversal calling the closure on every node unconditionally (hir.rs:1942-1952).

**Traversal 2 — Outer column collection (lines 1860-1870):**
```rust
let mut outer_cols = BTreeSet::new();
#[allow(deprecated)]
inner.visit_columns(0, &mut |depth, col| {
    if col.level > depth {
        outer_cols.insert(ColumnRef {
            level: col.level - depth,
            column: col.column,
        });
    }
});
```

This walks the entire relation tree AND all scalar expressions within each node (via `visit_scalar_expressions` at hir.rs:2225) to find column references that escape the subquery scope. This is the deepest traversal since it visits scalars inside relations.

**Traversal 3 — CTE reference collection (lines 1873-1909):**
```rust
#[allow(deprecated)]
inner.visit(0, &mut |e, _| match e {
    HirRelationExpr::Get { id: mz_expr::Id::Local(id), .. } => {
        if let Some(cte_desc) = cte_map.get(id) {
            outer_cols.extend(col_map.inner.iter().filter(...).map(...));
        }
    }
    HirRelationExpr::Let { id, .. } => {
        assert!(!cte_map.contains_key(id));
    }
    _ => {}
});
```

This walks the entire tree a third time to find CTE references and extend the outer column set.

### Nesting Amplification — O(D² × N)

`branch()` is called for **every** correlated subquery via:
- `apply_scalar_subquery()` (line 1970) — for `SELECT (SELECT ...)` scalar subqueries
- `apply_existential_subquery()` (line 2045) — for `WHERE EXISTS (...)` / `WHERE NOT EXISTS (...)`
- Lateral join decorrelation (line 585) — for implicit lateral references in joins

Each call to `branch()` triggers `apply()` which calls `inner.applied_to()`, which recursively lowers the inner expression. If that inner expression contains its own subqueries, `branch()` is called again on those.

For **D levels of nested subqueries**, each with subtree size proportional to the nesting:
- The outermost `branch()` traverses the full tree (size ≈ D × N) three times
- The next level traverses a subtree (size ≈ (D-1) × N) three times
- ... and so on

Total traversal work: `3 × Σ(i=0..D) (D-i) × N ≈ 3/2 × D² × N` — **quadratic in nesting depth**.

### ColumnMap::enter_scope Clone-and-Rebuild (lines 95-117)

```rust
fn enter_scope(&self, arity: usize) -> ColumnMap {
    let existing = self
        .inner
        .clone()               // CLONES entire BTreeMap
        .into_iter()
        .update(|(col, _i)| col.level += 1);  // Rebuilds with incremented levels

    let new = (0..arity).map(|i| {
        (ColumnRef { level: 1, column: i }, self.len() + i)
    });

    ColumnMap::new(existing.chain(new).collect())  // Collects into new BTreeMap
}
```

Every scope entry (correlated subquery boundary) clones the entire `BTreeMap<ColumnRef, usize>`, increments all levels, adds new columns, and collects into a fresh BTreeMap. With C columns accumulated across D nesting levels, this is **O(C × D)** BTreeMap operations total across all scope entries.

Called from:
- `branch()` line 1846 (simple path)
- Line 682 (right side of joins)

### No Short-Circuit in Traversal 1

The `visit` method (hir.rs:1929-1940) delegates to `visit_fallible` which always does a complete post-order traversal:

```rust
pub fn visit_fallible<'a, F, E>(&'a self, depth: usize, f: &mut F) -> Result<(), E> {
    self.visit1(depth, |e, depth| e.visit_fallible(depth, f))?;  // recurse children
    f(self, depth)  // then visit self — ALWAYS called
}
```

There is no mechanism to abort early when `is_simple` becomes `false`. For a non-simple subquery with N nodes, N-1 of the visits in traversal 1 are pure waste.

### Scaling Impact

A query like:
```sql
SELECT *,
  (SELECT avg(x) FROM a WHERE a.id = t.id
   AND a.y > (SELECT max(z) FROM b WHERE b.id = a.id
              AND b.w IN (SELECT c.w FROM c WHERE c.id = b.id)))
FROM t
```

Has 3 levels of nesting. Each level triggers `branch()` with 3 full tree walks. With ~50 nodes per subquery level:
- Level 0 `branch()`: 3 × 150 = 450 visits
- Level 1 `branch()`: 3 × 100 = 300 visits
- Level 2 `branch()`: 3 × 50 = 150 visits
- Total: **900 visits** when ~150 would suffice with a single-pass approach

For ORM-generated queries that frequently use correlated subqueries (especially EXISTS patterns), and queries with 5+ levels of view expansion, the quadratic amplification becomes significant.

Additionally, all three traversals collect overlapping information that could be gathered in a single pass:
- Traversal 1 checks node types → just needs a boolean flag
- Traversal 2 collects outer columns → needs column tracking
- Traversal 3 collects CTE references → needs CTE id tracking

### Root Cause

1. The three analyses were written as separate concerns without considering their combined cost
2. The deprecated `visit` API doesn't support early termination (no `ControlFlow` / short-circuit)
3. `ColumnMap` uses value semantics (BTreeMap) rather than structural sharing

### Suggested Fix

**Option A (single-pass analysis — moderate)**: Merge all three traversals into one `visit` call that collects simplicity, outer columns, and CTE references simultaneously:

```rust
struct BranchAnalysis {
    is_simple: bool,
    outer_cols: BTreeSet<ColumnRef>,
}

inner.visit(0, &mut |expr, depth| {
    // Simplicity check
    match expr {
        Constant{..} | Project{..} | Map{..} | Filter{..} | CallTable{..} => {},
        _ => analysis.is_simple = false,
    }
    // CTE reference check (replaces traversal 3)
    if let Get { id: Id::Local(id), .. } = expr {
        if let Some(cte_desc) = cte_map.get(id) {
            analysis.outer_cols.extend(...);
        }
    }
    // Column collection happens via scalars inside this node
});
// visit_columns still needed for scalar column refs, but only if !is_simple
```

This reduces 3 traversals to 1 relation traversal + 1 scalar traversal (for column refs), cutting work by ~40%.

**Option B (short-circuiting simplicity check — simple)**: Check simplicity with an early-exit traversal. If the expression is simple (common for table functions), skip traversals 2 and 3 entirely since the simple path doesn't use `outer_cols`. Use the newer `Visit::try_visit_post` which supports `ControlFlow` for early exit.

**Option C (structural sharing for ColumnMap — moderate)**: Replace `BTreeMap<ColumnRef, usize>` with an `im::OrdMap` or a persistent data structure that supports O(1) "enter scope" via structural sharing rather than clone-and-rebuild. This eliminates the O(C) clone per scope entry.

### Files Involved
- `src/sql/src/plan/lowering.rs:1790-1959` — `branch()` function with triple traversal
- `src/sql/src/plan/lowering.rs:95-117` — `ColumnMap::enter_scope()` clone-and-rebuild
- `src/sql/src/plan/lowering.rs:1961-2058` — `apply_scalar_subquery()` and `apply_existential_subquery()` calling `branch()`
- `src/sql/src/plan/lowering.rs:240-960` — `HirRelationExpr::applied_to()` recursive lowering that triggers nested `branch()` calls
- `src/sql/src/plan/lowering.rs:585` — lateral join decorrelation calling `branch()`
- `src/sql/src/plan/hir.rs:1929-1952` — `visit()` and `visit_fallible()` with no short-circuit
- `src/sql/src/plan/hir.rs:2220-2231` — `visit_columns()` full tree+scalar traversal

## Session 32: Redundant Analysis Framework Recomputation — O(T × A × N) Full Tree Traversals Across Optimizer Pipeline (2026-03-09)

### Location
`src/transform/src/analysis.rs:312-367` — `DerivedBuilder::visit()` (full tree analysis)
`src/transform/src/equivalence_propagation.rs:69-72` — EquivalencePropagation analysis
`src/transform/src/normalize_lets.rs:263-266` — NormalizeLets analysis
`src/transform/src/reduce_elision.rs:43-46` — ReduceElision analysis
`src/transform/src/threshold_elision.rs:43-46` — ThresholdElision analysis
`src/transform/src/will_distinct.rs:47-49` — WillDistinct analysis
`src/transform/src/canonicalization.rs:50-52` — Canonicalization analysis
`src/transform/src/join_implementation.rs:283-286` — JoinImplementation analysis
`src/transform/src/monotonic.rs:35-37` — Monotonic analysis

### Problem

Each transform that uses the analysis framework independently creates a fresh `DerivedBuilder`, registers its required analyses, and calls `builder.visit(relation)` — which does a **complete post-order tree traversal plus one full pass per analysis**. There is **no caching or sharing of analysis results** between transforms, even when:

1. Multiple transforms require the **same analysis** (e.g., SubtreeSize is required by every analysis-using transform)
2. The tree **hasn't changed** since the last analysis was computed
3. Transforms run **in sequence within the same Fixpoint iteration**

**How `DerivedBuilder::visit()` works (lines 312-367):**
1. Full pre-order traversal to build post-order list: **O(N)** where N = tree nodes
2. For each registered analysis, calls `bundle.analyse()` which iterates all N nodes: **O(N) per analysis**

So each `visit()` call costs **O(A × N)** where A = number of analyses required.

**Analysis dependency chains** make this worse. Each analysis declares dependencies:
- `Equivalences` requires `Arity` + `ReprRelationType` (which requires `Arity`)
- `UniqueKeys` requires `Arity` + `ReprRelationType`
- `Cardinality` requires `Arity` + `UniqueKeys`
- Every analysis implicitly requires `SubtreeSize`

So `EquivalencePropagation` (requiring Equivalences + ReprRelationType) triggers **4 analysis passes** per call: SubtreeSize + Arity + ReprRelationType + Equivalences = **4N** work.

### Scaling Impact: Counting Analysis Passes Across the Full Pipeline

**Logical optimizer (`fixpoint_logical_01`, limit=100):**
Per iteration:
- `PredicatePushdown`: no analysis
- `EquivalencePropagation`: SubtreeSize + Arity + ReprRelationType + Equivalences = **4 passes**
- `Demand`: no analysis
- `FuseAndCollapse`: no analysis

**Total for fixpoint_logical_01**: up to **100 × 4 = 400 full tree passes**

**Logical optimizer (`fixpoint_logical_02`, limit=100):**
Per iteration:
- `ReduceElision`: SubtreeSize + Arity + ReprRelationType + UniqueKeys = **4 passes**
- Other transforms in the loop don't use the analysis framework directly

**Total for fixpoint_logical_02**: up to **100 × 4 = 400 full tree passes**

**Physical optimizer:**
- `fixpoint_physical_01` (limit=100):
  - `EquivalencePropagation`: 4 passes per iteration
  - `fold_constants_fixpoint` (nested, limit=100):
    - `NormalizeLets`: SubtreeSize + Arity + ReprRelationType + UniqueKeys = 4 passes (inside `refresh_types`)
  - **Total**: up to 100 × (4 + 100 × 4) = **40,400 passes** (worst case with nested fixpoints)

- `fixpoint_join_impl` (limit=100):
  - `JoinImplementation`: SubtreeSize + Arity + UniqueKeys + Cardinality = **5 passes** per join input

- Additional standalone transforms: `ThresholdElision` (3 passes), `WillDistinct` (3 passes), `Canonicalization` (3 passes), final `fold_constants_fixpoint` (4 passes per NormalizeLets iteration)

**Conservative estimate for a single query optimization:**
Even if most fixpoints converge in ~5 iterations, that's still:
- fixpoint_logical_01: 5 × 4 = 20 passes
- fixpoint_logical_02: 5 × 4 = 20 passes
- fixpoint_physical_01: 5 × 4 = 20 passes (just EquivalencePropagation)
- fold_constants_fixpoint (nested): 5 × 4 = 20 passes
- fold_constants_fixpoint (final): 5 × 4 = 20 passes
- Standalone: ~15 passes
- **Total: ~115 full tree traversals per query optimization**

With a plan of 1,000 nodes, that's ~115,000 node visits. With 10,000 nodes (complex queries with many views inlined), that's ~1,150,000 node visits.

### Root Cause

The `DerivedBuilder` is **ephemeral by design** — it's created, runs, and its results are discarded. There is no mechanism to:
1. Cache analysis results in the `TransformCtx` and invalidate them when the tree changes
2. Share a `Derived` instance across multiple transforms within a Fixpoint iteration
3. Detect that the tree hasn't changed and skip re-analysis
4. Incrementally update analysis results after a local tree mutation

The comment in `equivalence_propagation.rs:94-95` acknowledges this is a problem: when EquivalencePropagation makes no changes (`prior == *relation` at line 90), it still ran the full analysis. The code then tries `ColumnKnowledge` as a fallback, which runs **yet another** set of transforms.

### Additional Waste: EquivalencePropagation Clones the Full Tree Even When Analyses Show No Changes

At line 75 of `equivalence_propagation.rs`:
```rust
let prior = relation.clone();  // Full tree clone
// ... apply ...
if prior == *relation {  // Full structural comparison
    let ck = crate::ColumnKnowledge::default();
    ck.transform(relation, ctx)?;  // Run ANOTHER transform
}
```

This means every EquivalencePropagation invocation does: 4N analysis + N clone + N comparison = **6N** work minimum, even when it changes nothing.

### Suggested Fix

**Option A (analysis caching in TransformCtx — moderate)**: Add a `cached_derived: Option<Derived>` field to `TransformCtx`. Each transform that needs analyses checks the cache first. When a transform modifies the relation, it sets the cache to `None`. This eliminates redundant analysis when consecutive transforms don't modify the tree.

**Option B (shared Derived within Fixpoint — moderate)**: Compute the `Derived` once at the start of each Fixpoint iteration and pass it to all transforms in that iteration. Transforms that modify the tree mark the cache as dirty. SubtreeSize, Arity, and ReprRelationType would only be computed once per iteration instead of per-transform.

**Option C (incremental analysis — complex)**: Implement an incremental analysis framework (similar to LLVM's AnalysisManager) that tracks which subtrees were modified and only recomputes analyses for affected nodes. This is the most efficient but requires significant redesign.

**Option D (skip analysis when tree unchanged — simple)**: In the Fixpoint loop, track whether the previous transform actually modified the tree. If not, skip analysis-dependent transforms that are known to be no-ops when nothing changed. This can be done with a simple `changed` flag.

### Files Involved
- `src/transform/src/analysis.rs:312-367` — `DerivedBuilder::visit()` — the core analysis dispatch loop
- `src/transform/src/analysis.rs:396-481` — `Bundle::analyse()` — per-analysis full tree traversal
- `src/transform/src/equivalence_propagation.rs:62-104` — creates DerivedBuilder + clones tree
- `src/transform/src/normalize_lets.rs:255-267` — `refresh_types()` creates DerivedBuilder
- `src/transform/src/reduce_elision.rs:38-47` — creates DerivedBuilder
- `src/transform/src/threshold_elision.rs:38-46` — creates DerivedBuilder
- `src/transform/src/will_distinct.rs:42-50` — creates DerivedBuilder
- `src/transform/src/canonicalization.rs:45-52` — creates DerivedBuilder
- `src/transform/src/join_implementation.rs:278-288` — creates DerivedBuilder per join input
- `src/transform/src/monotonic.rs:30-37` — creates DerivedBuilder
- `src/transform/src/lib.rs:686-700` — `fold_constants_fixpoint` nesting NormalizeLets inside Fixpoint
- `src/transform/src/lib.rs:737-889` — full logical + physical optimizer pipeline composition

## Session 33: Unconditional Builtin Table Update Generation — O(C × K) Row Packing Per DDL with No Subscriber Awareness (2026-03-09)

### Location
`src/adapter/src/catalog/apply.rs:1341-1420` — `generate_builtin_table_update()`
`src/adapter/src/catalog/builtin_table_updates.rs:569-916` — `pack_item_update()`
`src/adapter/src/catalog/apply.rs:205-258` — `apply_updates_inner()` (the calling loop)
`src/adapter/src/coord/appends.rs:328-572` — `group_commit()` (the consumer)

### Problem

Every catalog state change (DDL) eagerly generates and commits **builtin table updates** (rows for `mz_tables`, `mz_columns`, `mz_sources`, `mz_object_dependencies`, etc.) regardless of whether any session is reading those system tables. There are 66 builtin tables, and the system has **zero subscriber awareness** — no check is ever performed to determine whether any user or internal process is subscribed to or reading a given builtin table.

**The amplification per DDL operation:**

For a single `CREATE TABLE` with C columns and D dependencies, `pack_item_update()` generates:

1. **1 row** to `mz_tables` (lines 596-598)
2. **C rows** to `mz_columns` — one row per column (lines 843-906), each requiring:
   - A `Row::pack_slice` with 8 datums
   - Type resolution via `mz_pgrepr::Type::from()` per column
   - For custom types: a catalog `get_entry()` lookup per column (line 871)
   - For tables with defaults: `to_ast_string_stable()` AST serialization per default (line 852)
3. **D rows** to `mz_object_dependencies` — one per referenced object (lines 837-839)
4. **1 row** to `mz_history_retention_strategies` (lines 910-912)
5. **1+ rows** to `mz_object_global_ids` (line 914)

**Total: 4 + C + D rows per CREATE TABLE.**

For sources with subsource exports, the amplification is worse. A `CREATE SOURCE` from Postgres with N exported tables generates:
- Source-specific rows (mz_sources, mz_postgres_sources)
- N rows to `mz_postgres_source_tables` (lines 629-634, 759-764)
- Then each subsource table generates its own `pack_item_update()` with column expansions

**The per-DDL cloning tax in `apply_updates_inner()` (lines 205-258):**

For each state update, the `StateUpdateKind` is cloned up to **2 times**:

- **Retraction path** (lines 211-232): `state_update.clone()` for `parse_state_update` (line 216) + `state_update.kind.clone()` for `generate_builtin_table_update` (line 224) = 2 clones
- **Addition path** (lines 234-255): `state_update.kind.clone()` for `apply_update` (line 236) + `state_update.kind.clone()` for `generate_builtin_table_update` (line 245) + `state_update.clone()` for `parse_state_update` (line 252) = 3 clones

Each `StateUpdateKind::Item` contains a full `Item` struct with `create_sql` (the full SQL text of the DDL), which is cloned each time.

**The unconditional commit path (appends.rs:328-572):**

All generated builtin table updates are:
1. Pushed to `pending_writes` as `PendingWriteTxn::System` (line 751)
2. Always included in the next `group_commit()` (line 341: system writes always proceed)
3. Consolidated with `differential_dataflow::consolidation::consolidate()` (line 533)
4. Sent to `controller.storage.append_table()` (line 569) — persisted to storage

This means every DDL writes to storage for every affected builtin table, even if no session has ever queried `mz_columns` or `mz_object_dependencies`.

### Scaling Impact

**Scenario: Creating a Postgres source with 200 exported tables, each with 20 columns:**

Per-source overhead:
- 1 `mz_sources` row
- 1 `mz_postgres_sources` row
- 200 `mz_postgres_source_tables` rows

Per-table overhead (200 tables):
- 200 `mz_tables` rows
- 200 × 20 = 4,000 `mz_columns` rows (each packing 8 datums with type resolution)
- 200 × 1 = 200 `mz_object_dependencies` rows (at minimum)
- 200 `mz_history_retention_strategies` rows
- 200+ `mz_object_global_ids` rows

**Total: ~5,000+ builtin table update rows from ONE CREATE SOURCE.**

Each row requires:
- `Row::pack_slice()` allocation + datum packing
- Consolidation in `group_commit()`
- A storage `append_table()` write

All on the **single-threaded coordinator**, blocking all other operations.

For a catalog with 10,000 objects, even routine DDL like `ALTER CLUSTER` triggers `pack_cluster_update()` which generates 3+ rows. With frequent DDL (schema migrations, CI/CD pipelines), the coordinator spends significant time packing and committing rows that nobody reads.

### Root Cause

The builtin table system has **no subscriber tracking**. Materialize treats builtin tables identically to user tables — they are persist-backed collections that receive updates via `append_table()`. The assumption is that these tables must always be up-to-date because a query like `SELECT * FROM mz_columns` could arrive at any time.

However, many builtin tables are rarely or never queried in production. Tables like `mz_postgres_source_tables`, `mz_kafka_source_tables`, `mz_object_global_ids`, and `mz_history_retention_strategies` are niche. Even `mz_columns` is only queried occasionally (typically by BI tools during schema discovery, not continuously).

The design also lacks **batching of row construction**: each column generates its own `Row::pack_slice()` call with a fresh allocation, rather than reusing a RowPacker across the column loop.

### Suggested Fix

**Option A (subscriber-aware lazy generation — highest impact):** Track which builtin tables have active SUBSCRIBE cursors, indexes, or materialized views depending on them. Only generate and commit updates for tables that have at least one downstream consumer. For tables with zero consumers, mark them as "stale" and regenerate on first query (similar to a materialized view refresh). This eliminates the vast majority of builtin table writes in typical production deployments.

**Option B (deferred generation with dirty flags — moderate):** Instead of eagerly generating all builtin table rows during `apply_updates_inner()`, set dirty flags per builtin table type. When a query reads a builtin table, check the flag and generate the missing rows at that point. This is effectively lazy evaluation of builtin table state.

**Option C (reduce per-row allocation overhead — simple):** In `pack_item_update()`, reuse a single `RowPacker` across the column loop (lines 851-906) instead of calling `Row::pack_slice()` per column. Also, avoid the `to_ast_string_stable()` call for defaults when `defaults` is `None` (which is the common case for non-table items).

**Option D (batch StateUpdateKind consumption — moderate):** Refactor `apply_updates_inner()` to consume `state_update.kind` by value once and pass references to the three consumers (`apply_update`, `generate_builtin_table_update`, `parse_state_update`), eliminating the 2-3 clones per update.

### Files Involved
- `src/adapter/src/catalog/apply.rs:205-258` — `apply_updates_inner()` loop with 2-3 clones per update
- `src/adapter/src/catalog/apply.rs:1341-1420` — `generate_builtin_table_update()` dispatcher
- `src/adapter/src/catalog/builtin_table_updates.rs:569-916` — `pack_item_update()` with column/dependency expansion
- `src/adapter/src/catalog/builtin_table_updates.rs:1-568` — All other `pack_*_update()` methods
- `src/adapter/src/coord/appends.rs:328-572` — `group_commit()` unconditionally writes all updates to storage
- `src/adapter/src/coord/appends.rs:739-757` — `background()` always pushes `PendingWriteTxn::System`
- `src/catalog/src/builtin.rs` — 66 `BuiltinTable` definitions

## Session 34: Exponential `introspection_dependencies` Walk on Every SELECT — No Visited Set + Per-Dependency Recursive Traversal (2026-03-09)

### Location
`src/adapter/src/catalog/state.rs:416-448` — `introspection_dependencies()` / `introspection_dependencies_inner()`
`src/adapter/src/coord/catalog_serving.rs:170-178` — `auto_run_on_catalog_server()` hot path
`src/adapter/src/coord/sequencer.rs:158` — called from `sequence_plan()` for every statement

### Problem

Every SELECT, SUBSCRIBE, EXPLAIN, and ShowColumns query calls `auto_run_on_catalog_server()` to decide if the query should be routed to the `mz_catalog_server` cluster. This function iterates over **every dependency** of the query and for each dependency calls `introspection_dependencies(id)` to check if any transitive dependency is a log source (introspection collection).

**Issue 1 — No visited set (exponential blow-up):**

`introspection_dependencies_inner()` (line 422) recursively walks all `references()` of each item without maintaining a visited/seen set. In a diamond dependency graph:

```
     A
    / \
   B   C
    \ /
     D
      \
       E (log source)
```

Node D (and everything below it) is visited **twice** — once via B, once via C. For a dependency DAG of depth D with branching factor B, the worst case is O(B^D) visits instead of O(V + E) with memoization. Real catalog objects like `mz_show_materialized_views` have deep dependency chains through multiple shared views, making this exponential blow-up a real concern.

**Issue 2 — Per-dependency recursive traversal on every query:**

In `auto_run_on_catalog_server()` (lines 170-178):
```rust
let valid_dependencies = depends_on.all(|id| {
    let entry = catalog.state().get_entry(&id);
    let schema = entry.name().qualifiers.schema_spec;
    let system_only = catalog.state().is_system_schema_specifier(schema);
    let non_replica = catalog.state().introspection_dependencies(id).is_empty();
    system_only && non_replica
});
```

For a query like `SELECT * FROM mz_materialized_views` with D direct dependencies, this performs D independent recursive walks. If those dependencies share sub-dependencies (which system catalog views heavily do), the **same subtrees are traversed D times**.

**Issue 3 — Uses `get_entry_by_global_id` in the recursive walk:**

At lines 435 and 439, the recursive function calls `self.get_entry_by_global_id(&sink.from).id()` and `self.get_entry_by_global_id(&idx.on).id()`. As identified in Session 11, `get_entry_by_global_id` performs a full `CatalogEntry` clone plus an O(N) linear scan through all entries (or at best an O(log N) map lookup + clone). This clone is entirely unnecessary — only the `CatalogItemId` is needed.

**Issue 4 — Result is never cached:**

The introspection dependency set of a catalog item is **static** — it only changes when the item's definition changes (i.e., on DDL). Yet it's recomputed from scratch on every query. A simple `BTreeMap<CatalogItemId, bool>` cache (has_introspection_deps) invalidated on DDL would eliminate all recurring cost.

### Scaling Impact

**Scenario: `SELECT * FROM mz_materialized_views` in a catalog with 500 views/MVs:**

1. `depends_on()` returns ~10 direct dependencies (system views)
2. For each dependency, `introspection_dependencies()` walks its full transitive closure
3. System catalog views like `mz_show_materialized_views` depend on `mz_materialized_views`, `mz_schemas`, `mz_databases`, `mz_clusters`, etc. — each with their own dependency chains
4. Without a visited set, shared sub-dependencies are visited exponentially
5. Each visit does a `get_entry()` lookup + pattern match + reference extraction

Estimated cost per query: O(D × 2^depth) where D = direct dependencies (~10) and depth = transitive dependency depth (~5-8 for system views). This could mean **hundreds to thousands of catalog lookups** for a single `SELECT * FROM mz_columns`.

This runs on the **single-threaded coordinator** for every SELECT query, even simple ones like `SELECT 1` if they happen to have any system catalog dependencies resolved during planning.

### Root Cause

1. `introspection_dependencies_inner` was written as a simple recursive function without the standard "visited set" pattern used in graph traversals (contrast with `transitive_uses()` at line 384 which correctly uses a `BTreeSet<CatalogItemId>` as a seen set)
2. The `auto_run_on_catalog_server` function was not designed with the assumption that `introspection_dependencies` could be expensive — it treats it as a simple property lookup
3. No caching layer exists between the static catalog dependency graph and the per-query routing decision

### Suggested Fix

**Option A (add visited set — minimal fix):** Add a `BTreeSet<CatalogItemId>` seen parameter to `introspection_dependencies_inner`, mirroring the pattern used by `transitive_uses()`:

```rust
fn introspection_dependencies_inner(
    &self,
    id: CatalogItemId,
    out: &mut Vec<CatalogItemId>,
    seen: &mut BTreeSet<CatalogItemId>,
) {
    if !seen.insert(id) { return; }
    // ... rest unchanged
}
```

This changes the traversal from potentially-exponential to O(V + E).

**Option B (cache per-item result — eliminates recurring cost):** Maintain a `BTreeMap<CatalogItemId, bool>` in `CatalogState` that caches whether each item has introspection dependencies. Invalidate entries when items are created/dropped. The `auto_run_on_catalog_server` check becomes O(D) simple map lookups with no recursion.

**Option C (short-circuit in `auto_run_on_catalog_server`):** Since the function already checks `system_only` first (line 174), and introspection sources only exist in system schemas, we know that items in user schemas can never have introspection dependencies. Skip the `introspection_dependencies` call entirely for non-system items (which is the common case for user queries).

**Option D (avoid `get_entry_by_global_id` cloning):** In `introspection_dependencies_inner`, replace `self.get_entry_by_global_id(&sink.from).id()` with a direct `global_id_to_item_id` map lookup to avoid the full `CatalogEntry` clone.

### Files Involved
- `src/adapter/src/catalog/state.rs:416-448` — `introspection_dependencies()` and `introspection_dependencies_inner()` with no visited set
- `src/adapter/src/coord/catalog_serving.rs:39-196` — `auto_run_on_catalog_server()` calling `introspection_dependencies()` per dependency
- `src/adapter/src/coord/sequencer.rs:156-164` — `sequence_plan()` calling `auto_run_on_catalog_server()` for every statement
- `src/adapter/src/frontend_peek.rs:438` — frontend peek path also calling `auto_run_on_catalog_server()`

## Session 35: PubSub `remove_connection` — O(S) Write-Lock Scan of All Shards Blocking All Diff Pushes (2026-03-09)

### Location
`src/persist-client/src/rpc.rs:833-858` — `remove_connection()` full shard scan under write lock
`src/persist-client/src/rpc.rs:860-916` — `push_diff()` hot path blocked by write lock
`src/persist-client/src/rpc.rs:800-811` — `PubSubState` data structure with no reverse index

### Problem

The persist PubSub server maintains a `shard_subscribers` map of type `BTreeMap<ShardId, BTreeMap<usize, Sender<...>>>` — mapping each shard to its set of subscriber connections. When a connection disconnects, `remove_connection()` must find and remove that connection from every shard it was subscribed to.

**Issue 1 — No reverse index, O(S_total) scan on disconnect:**

```rust
fn remove_connection(&self, connection_id: usize) {
    // ...
    {
        let mut subscribers = self.shard_subscribers.write().expect("lock poisoned");
        subscribers.retain(|_shard, connections_for_shard| {
            connections_for_shard.remove(&connection_id);
            !connections_for_shard.is_empty()
        });
    }
    // ...
}
```

There is no reverse index from `connection_id → Set<ShardId>`. The only way to clean up a disconnected connection is to iterate through **every shard** in the subscriber map and attempt removal. With S shards in the system, this is O(S) even if the connection was only subscribed to 1 shard.

**Issue 2 — Write lock blocks ALL push_diff operations during the scan:**

The `retain` call at line 847-852 holds an **exclusive write lock** on `shard_subscribers` for the entire duration of the O(S) scan. Meanwhile, `push_diff()` (the hot path called for every state update on every shard) needs a **read lock** on the same `shard_subscribers` (line 873):

```rust
fn push_diff(&self, connection_id: usize, shard_id: &ShardId, data: &VersionedData) {
    // ...
    let subscribers = self.shard_subscribers.read().expect("lock poisoned");
    if let Some(subscribed_connections) = subscribers.get(shard_id) {
        for (subscribed_conn_id, tx) in subscribed_connections {
            // ... send to each subscriber
        }
    }
}
```

While `remove_connection` holds the write lock scanning all S shards, **every** `push_diff` call across **every** shard is blocked. This creates a global pause in state diff propagation.

**Issue 3 — Per-subscriber message construction in push_diff:**

Inside `push_diff`, for each subscriber of a shard, a new protobuf message is constructed (lines 890-894):

```rust
let req = create_request(proto_pub_sub_message::Message::PushDiff(ProtoPushDiff {
    seqno: data.seqno.into_proto(),
    shard_id: shard_id.to_string(),  // String allocation per subscriber
    diff: Bytes::clone(&data.data),   // Ref-counted clone (cheap)
}));
```

The `shard_id.to_string()` allocates a new String for every subscriber of every diff. If a shard has C subscribers, each diff produces C string allocations. The message structure is identical across subscribers — only the destination channel differs — yet the full protobuf wrapper is recreated each time.

**Issue 4 — `subscribe()` and `unsubscribe()` also take write locks:**

Both `subscribe()` (line 937) and `unsubscribe()` (line 963) acquire write locks on `shard_subscribers`. Combined with `push_diff` holding a read lock for the entire duration of message creation and sending (including `try_send` per subscriber), there is read-write contention on every operation.

### Scaling Impact

**Scenario: Deployment with 5,000 shards and 10 connected workers (typical for mid-size Materialize):**

When a single worker disconnects (e.g., cluster replica restart, network partition):
1. `remove_connection` acquires a write lock on `shard_subscribers`
2. Iterates through all 5,000 shard entries in the BTreeMap
3. For each shard, does a BTreeMap removal by connection_id = O(log C) per shard
4. Total: O(S × log C) = O(5,000 × log 10) ≈ 17,000 BTreeMap operations
5. **During this entire scan, ALL push_diff calls across ALL shards are blocked**

Meanwhile, with active shards producing diffs at high throughput (e.g., 1,000+ diffs/sec across all shards), every diff is stalled waiting for the write lock to release. This creates a visible latency spike in state propagation across all shards — not just the ones the disconnecting connection subscribed to.

The already-present `connection_cleanup_seconds` metric (line 855) confirms the team recognizes this is a slow operation worth monitoring.

For the `push_diff` hot path: with C subscribers per shard and D diffs per second, we get C × D string allocations per second per shard. At C=10 and D=100, that's 1,000 unnecessary string allocations/sec per hot shard.

### Root Cause

The `PubSubState` data structure uses a single-directional mapping (`ShardId → connections`) without a reverse mapping (`connection_id → shards`). This is a classic graph representation problem: when you need to traverse edges in both directions (subscribe/push by shard, cleanup by connection), you need either a bidirectional index or an adjacency list in both directions.

The design also uses a single coarse-grained `RwLock` over the entire `shard_subscribers` map rather than per-shard locking, meaning operations on unrelated shards contend with each other.

### Suggested Fix

**Option A (reverse index — eliminates O(S) scan):** Maintain a parallel `BTreeMap<usize, BTreeSet<ShardId>>` mapping each connection to its subscribed shards. Update it in `subscribe()` and `unsubscribe()`. In `remove_connection()`, look up the connection's subscribed shards (O(1)), then remove from only those shards:

```rust
fn remove_connection(&self, connection_id: usize) {
    let subscribed_shards = self.connection_shards.write().remove(&connection_id);
    let mut subscribers = self.shard_subscribers.write().expect("lock");
    for shard_id in subscribed_shards.unwrap_or_default() {
        if let Entry::Occupied(mut entry) = subscribers.entry(shard_id) {
            entry.get_mut().remove(&connection_id);
            if entry.get().is_empty() {
                entry.remove_entry();
            }
        }
    }
}
```

This changes cleanup from O(S_total) to O(S_connection) — only the shards that connection actually subscribed to.

**Option B (per-shard locking — reduces contention):** Replace `RwLock<BTreeMap<ShardId, BTreeMap<...>>>` with `BTreeMap<ShardId, RwLock<BTreeMap<...>>>` (or use a sharded lock like `dashmap`). This allows `push_diff` for shard A to proceed concurrently with `remove_connection` cleanup of shard B. Combined with Option A, this eliminates global blocking entirely.

**Option C (share message across subscribers in push_diff):** Construct the `ProtoPubSubMessage` once and wrap it in `Arc`, sending `Arc<ProtoPubSubMessage>` to each subscriber channel instead of cloning the message. This eliminates the per-subscriber `shard_id.to_string()` allocation. Alternatively, pre-compute `shard_id.to_string()` once outside the loop.

**Option D (defer connection cleanup):** Instead of synchronously scanning all shards on disconnect, mark the connection as "disconnected" in a separate set and lazily clean up stale entries during `push_diff` (when a `try_send` returns `Closed`). This eliminates the write lock during disconnect entirely, at the cost of slightly delayed cleanup.

### Files Involved
- `src/persist-client/src/rpc.rs:800-811` — `PubSubState` struct with single-direction `shard_subscribers` map
- `src/persist-client/src/rpc.rs:833-858` — `remove_connection()` with O(S) full-map scan under write lock
- `src/persist-client/src/rpc.rs:860-916` — `push_diff()` hot path needing read lock on same map
- `src/persist-client/src/rpc.rs:918-947` — `subscribe()` acquiring write lock for each subscription
- `src/persist-client/src/rpc.rs:949-977` — `unsubscribe()` acquiring write lock for each unsubscription

---

## Session 36: ComputeInstanceSnapshot Allocates Full Collection Set Per Query

### Problem
`ComputeInstanceSnapshot::new()` at `src/adapter/src/optimize/dataflows.rs:65-73` collects **every collection ID** in the compute instance into a new `BTreeSet<GlobalId>` on each invocation:

```rust
pub fn new(controller: &Controller, id: ComputeInstanceId) -> Result<Self, InstanceMissing> {
    controller
        .compute
        .collection_ids(id)
        .map(|collection_ids| Self {
            instance_id: id,
            collections: Some(collection_ids.collect()),  // O(C) allocation
        })
}
```

`collection_ids()` at `src/compute-client/src/controller.rs:364-371` iterates the keys of `instance.collections: BTreeMap<GlobalId, CollectionState<T>>`, and `.collect()` copies all of them into a new `BTreeSet<GlobalId>`.

### Why This Is on the Hottest Path

`instance_snapshot()` is called from **12 call sites** across the adapter, including the most performance-critical ones:

| Call site | When triggered |
|-----------|---------------|
| `peek_validate()` at `peek.rs:257` | **Every SELECT query** |
| `peek_optimize()` at `peek.rs:526` | Every EXPLAIN (for ALL user clusters!) |
| `create_materialized_view_optimize` at `create_materialized_view.rs:432` | Every CREATE MATERIALIZED VIEW |
| `create_index_optimize` at `create_index.rs:313` | Every CREATE INDEX |
| `subscribe_optimize` at `subscribe.rs:254` | Every SUBSCRIBE |
| `create_continual_task_optimize` at `create_continual_task.rs:186` | Every CREATE CONTINUAL TASK |
| `coord.rs:3109,3200,3298` | Catalog transaction bootstrap (indexes, MVs, CTs) |
| `introspection.rs:201` | Introspection updates |

The SELECT path (`peek_validate`) is by far the highest-frequency caller. Every single SELECT on the coordinator thread performs an O(C) BTreeSet allocation where C is the number of collections in the target cluster.

### What Makes This Scale Poorly

Each compute instance's `collections` map contains entries for:
- Every index installed on the cluster
- Every materialized view output
- Every continual task
- All introspection/logging sources (system indexes)
- Arrangement imports

For a production cluster with hundreds of MVs and indexes, C can easily be **1,000–5,000+**. Each `BTreeSet::collect()` call:
1. Iterates all C keys in the BTreeMap (pointer chasing through B-tree nodes)
2. Allocates a new BTreeSet and inserts each GlobalId
3. The snapshot is then used almost exclusively for `contains_collection()` checks — typically checking only **1–10 specific IDs** during dataflow building

This means we do O(C) work to answer what could be O(log C) point lookups directly against the controller's existing map.

### The EXPLAIN Case Is Even Worse

In `peek_optimize()` at `peek.rs:522-528`, when `explain_ctx.needs_plan_insights()` is true, it creates a snapshot for **every user cluster**:

```rust
for cluster in self.catalog().user_clusters() {
    let snapshot = self.instance_snapshot(cluster.id).expect("must exist");
    compute_instances.insert(cluster.name.clone(), snapshot);
}
```

If a deployment has K user clusters each with C collections, this is O(K × C) allocation for a single EXPLAIN query.

### Suggested Fix

**Option A (pass reference instead of snapshot):** Change `DataflowBuilder` and the optimizer pipeline to accept a reference or `Arc` to the controller's live `BTreeMap<GlobalId, CollectionState>` (or a read-only view of its key set) instead of copying. The `contains_collection()` check would become a direct `.contains_key()` on the existing map. This eliminates all allocation.

**Option B (shared/cached snapshot):** Since multiple queries targeting the same cluster could share a snapshot, cache the `BTreeSet` per cluster and invalidate it only when collections are added/removed. This amortizes the cost across queries.

**Option C (use the `new_without_collections` variant):** The code already has `new_without_collections()` which sets `collections: None` and makes `contains_collection()` always return `true`. For the common peek path, if the optimizer only needs to check a few IDs, pass those specific IDs to the controller for direct lookup rather than copying the entire set.

**Option D (lazy snapshot):** Make `ComputeInstanceSnapshot` hold a reference to the controller and lazily evaluate `contains_collection()` via direct lookup, only materializing the full set if iteration is actually needed (which it rarely is).

### Files Involved
- `src/adapter/src/optimize/dataflows.rs:65-73` — `ComputeInstanceSnapshot::new()` with `.collect()` creating O(C) BTreeSet
- `src/adapter/src/optimize/dataflows.rs:96-100` — `contains_collection()` that only does point lookups
- `src/compute-client/src/controller.rs:364-371` — `collection_ids()` returning iterator over all keys
- `src/adapter/src/coord/sequencer/inner/peek.rs:257` — SELECT hot path calling `instance_snapshot()`
- `src/adapter/src/coord/sequencer/inner/peek.rs:522-528` — EXPLAIN creating snapshot for ALL user clusters
- `src/adapter/src/coord.rs:3807-3812` — `instance_snapshot()` wrapper calling `ComputeInstanceSnapshot::new()`
