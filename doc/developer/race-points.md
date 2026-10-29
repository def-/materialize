# Race points

Race points let you record which concurrent actors touched which shared state
in a test, and then force a chosen interleaving of them. The idea follows
Project Zero's MAccConc for the Linux kernel. That tool instruments every
memory access. Most of our races are logical races between async tasks and
processes, not data races, so we annotate the interesting places by hand
instead.

The pieces:

* `mz_ore::race` and the `race_point!` / `race_point_async!` macros, in
  `src/ore/src/race.rs`. Points are inert unless the process starts with
  `MZ_RACE_DIR` set.
* `bin/race-viz`, which runs a test case against an environment and serves a
  web page to inspect traces and edit orderings.
* Test cases in `test/race/`.

## Quick start

```shell
# Terminal 1: environmentd with race points enabled.
MZ_RACE_DIR=$PWD/mzdata/race bin/environmentd

# Terminal 2: the viewer, on http://127.0.0.1:7878.
bin/race-viz test/race/replacement_seal.py
```

`bin/race-viz --start-environmentd test/race/... -- <bin/environmentd args>`
starts environmentd itself. clusterd processes inherit `MZ_RACE_DIR` from the
process orchestrator, so points in clusterd work too.

In the page:

1. Press **Run baseline** to record the test once. Each actor gets a lane, and
   events appear in global time order.
2. Enable **Communication points only** to see the hits where different actors
   touch the same key and at least one of them writes. Hover a hit to
   highlight every hit on the same key. Click it to see its source.
3. Click a hit, press **Order before…**, then click the hit that must wait.
   Right-clicking two hits does the same. The arrow and its number show the
   constraint.
4. Press **Run**. The trace shows `wait`, `wake`, `resume` and `TIMEOUT`
   markers, the summary shows each actor's result, and the constraint list
   shows whether each constraint was enforced.
5. **Explore A-B-A** runs every schedule where one actor pauses at a
   communication point while another runs, and groups the runs by outcome.
   `bin/race-viz --explore` does the same without the page and exits non-zero
   on an invariant violation.

**Download run** saves the run and its trace as a JSON file. Opening the page
without a server (or from another checkout) can load it with **Open run
file**, which is how to attach a forced interleaving to an issue.

## Adding race points

```rust
// Waits, records, and fires constraints when reached.
mz_ore::race_point!("mv_sealed_check", key = gid, kind = Access::Read);

// The guard extends a "post" constraint across the action that follows.
let _race = mz_ore::race_point_async!("execute", note = sql).await;
```

* `key` names the shared state, for example a `GlobalId` or shard ID. Hits on
  the same key are what the viewer correlates.
* `kind` is `Read`, `Write` or `Drop`.
* `note` is free-form context.

Rules for placement:

* Never hit a point while holding a lock that the actor you may be waiting
  for needs. The wait then deadlocks until the timeout.
* `race_point!` blocks the thread. In async code use `race_point_async!`,
  since blocking a tokio worker stalls every task on it.
* A hit is identified by actor, point name and hit count within the run. Keep
  the points of a test deterministic up to the interleaving you explore, for
  example by hitting a point only for the state transition you care about.

Actors are named after the task that hits the point (every task spawned
through `mz_ore::task`), else the thread name. A pgwire connection takes its
`application_name`, which is how `bin/race-viz` gives each SQL actor of a test
its name.

## Writing a test case

A test case is a Python file. See `test/race/replacement_seal.py`.

* `ACTORS`: a dict from actor name to a function taking a psycopg cursor. Each
  actor runs on its own connection and thread.
* `setup(cur)` and `teardown(cur)`, optional, run outside the race run.
* `check(outcomes)`, optional, returns a list of invariant violations given
  each actor's `{"ok": bool, "result" | "error": ...}`.
* `TIMEOUT_MS`, optional, bounds each wait. A wait that times out means the
  requested ordering could not happen.
* `SETTLE_S`, optional, how long background actors keep recording after the
  SQL actors finish.

## Protocol

`MZ_RACE_DIR` holds a `control` file that names the active run, its timeout
and its constraints. Every process re-reads it at most every 10 ms on a point
hit. Each process appends events to `runs/<run>/trace-<pid>.jsonl` with one
`write` per event, so a trace survives a crash. A satisfied constraint is a
file in `runs/<run>/flags/`, which is what makes constraints work across
processes. The module docs in `src/ore/src/race.rs` own the details.
