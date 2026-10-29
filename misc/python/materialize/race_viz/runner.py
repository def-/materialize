# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Runs a race test case under a set of ordering constraints.

The Rust side of the protocol lives in `src/ore/src/race.rs`. This module
writes its `control` file, drives the test's SQL actors, and collects the
per-process traces of a run.

A test case is a Python file defining:

* `ACTORS`: a dict from actor name to a function taking a psycopg cursor.
  Each actor runs on its own thread and connection, whose `application_name`
  is the actor name, which makes the connection's race points belong to that
  actor.
* optionally `setup(cur)` and `teardown(cur)`, which run outside any race
  run, so their statements neither record nor wait.
* optionally `check(outcomes) -> list[str]`, returning invariant violations
  given each actor's outcome.
* optionally `TIMEOUT_MS` (per waiting hit) and `SETTLE_S` (how long
  background actors keep recording after the SQL actors finish).
"""

import importlib.util
import json
import threading
import time
import traceback
from dataclasses import asdict, dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

import psycopg

# Each process re-reads the control file at most this often, see
# `mz_ore::race::CONTROL_POLL_INTERVAL`. Waiting a few multiples of it before
# the actors start makes every process see the new run first.
CONTROL_SETTLE_S = 0.1


@dataclass(frozen=True)
class HitId:
    actor: str
    point: str
    hit: int


@dataclass(frozen=True)
class Order:
    """`after` does not proceed past its point until `before` fired.

    `before` fires when its point is reached (`post=False`) or when the
    point's guard drops (`post=True`).
    """

    before: HitId
    after: HitId
    post: bool = True

    @staticmethod
    def from_json(o: dict[str, Any]) -> "Order":
        return Order(
            before=HitId(**o["before"]),
            after=HitId(**o["after"]),
            post=bool(o.get("post", True)),
        )


@dataclass
class TestCase:
    path: Path
    module: ModuleType

    @staticmethod
    def load(path: Path) -> "TestCase":
        spec = importlib.util.spec_from_file_location(f"race_test_{path.stem}", path)
        assert spec and spec.loader, f"cannot load {path}"
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if not isinstance(getattr(module, "ACTORS", None), dict):
            raise ValueError(f"{path} does not define an ACTORS dict")
        return TestCase(path, module)

    @property
    def name(self) -> str:
        return self.path.stem

    @property
    def actors(self) -> list[str]:
        return list(self.module.ACTORS)

    @property
    def timeout_ms(self) -> int:
        return int(getattr(self.module, "TIMEOUT_MS", 5000))

    @property
    def settle_s(self) -> float:
        return float(getattr(self.module, "SETTLE_S", 1.0))


@dataclass
class RunResult:
    id: str
    label: str
    test: str
    orders: list[Order]
    started: float
    duration_s: float
    outcomes: dict[str, dict[str, Any]]
    violations: list[str]
    constraints: list[dict[str, Any]] = field(default_factory=list)
    status: str = ""

    def to_json(self) -> dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_json(o: dict[str, Any]) -> "RunResult":
        o = dict(o)
        o["orders"] = [Order.from_json(x) for x in o["orders"]]
        return RunResult(**o)


class RaceDir:
    """The directory shared with every process through `MZ_RACE_DIR`."""

    def __init__(self, root: Path):
        self.root = root
        (root / "runs").mkdir(parents=True, exist_ok=True)
        self._seq = 0

    def run_dir(self, run_id: str) -> Path:
        return self.root / "runs" / run_id

    def new_run_id(self) -> str:
        self._seq += 1
        return time.strftime("%Y%m%d-%H%M%S") + f"-{self._seq:03d}"

    def write_control(self, run_id: str, orders: list[Order], timeout_ms: int) -> None:
        lines = [f"run\t{run_id}", f"timeout_ms\t{timeout_ms}"]
        for o in orders:
            for part in (o.before.actor, o.before.point, o.after.actor, o.after.point):
                if "\t" in part or "\n" in part:
                    raise ValueError(f"invalid name {part!r}")
            mode = "post" if o.post else "pre"
            lines.append(
                "\t".join(
                    [
                        "order",
                        o.before.actor,
                        o.before.point,
                        str(o.before.hit),
                        mode,
                        o.after.actor,
                        o.after.point,
                        str(o.after.hit),
                    ]
                )
            )
        self._replace_control("\n".join(lines) + "\n")

    def clear_control(self) -> None:
        self._replace_control("")

    def _replace_control(self, contents: str) -> None:
        # Processes detect changes by modification time and length, so an
        # atomic rename keeps them from reading a half-written file.
        tmp = self.root / "control.tmp"
        tmp.write_text(contents)
        tmp.replace(self.root / "control")

    def events(self, run_id: str) -> list[dict[str, Any]]:
        events = []
        for trace in sorted(self.run_dir(run_id).glob("trace-*.jsonl")):
            for line in trace.read_text().splitlines():
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    # A process that died mid-write leaves a partial last line.
                    pass
        events.sort(key=lambda e: e["ts_ns"])
        return events

    def save(self, result: RunResult) -> None:
        path = self.run_dir(result.id) / "run.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result.to_json(), indent=2))

    def load(self, run_id: str) -> RunResult:
        return RunResult.from_json(
            json.loads((self.run_dir(run_id) / "run.json").read_text())
        )

    def runs(self) -> list[RunResult]:
        results = []
        for path in (self.root / "runs").glob("*/run.json"):
            try:
                results.append(RunResult.from_json(json.loads(path.read_text())))
            except (json.JSONDecodeError, KeyError, TypeError):
                pass
        results.sort(key=lambda r: r.started, reverse=True)
        return results


class Runner:
    def __init__(self, test: TestCase, race_dir: RaceDir, url: str):
        self.test = test
        self.race_dir = race_dir
        self.url = url
        # The control file is global to the environment, so runs must not
        # overlap.
        self.lock = threading.Lock()

    def connect(self, application_name: str) -> psycopg.Connection:
        return psycopg.connect(
            self.url, autocommit=True, application_name=application_name
        )

    def run(self, orders: list[Order], label: str = "") -> RunResult:
        with self.lock:
            return self._run(orders, label)

    def _run(self, orders: list[Order], label: str) -> RunResult:
        module = self.test.module
        run_id = self.race_dir.new_run_id()

        with self.connect("race-setup") as conn:
            if hasattr(module, "setup"):
                module.setup(conn.cursor())

        self.race_dir.write_control(run_id, orders, self.test.timeout_ms)
        time.sleep(CONTROL_SETTLE_S)
        started = time.time()

        outcomes: dict[str, dict[str, Any]] = {}

        def run_actor(name: str, fn: Any) -> None:
            try:
                with self.connect(name) as conn:
                    result = fn(conn.cursor())
                outcomes[name] = {"ok": True, "result": _jsonable(result)}
            except Exception as e:
                outcomes[name] = {
                    "ok": False,
                    "error": f"{type(e).__name__}: {e}".strip(),
                    "traceback": traceback.format_exc(),
                }

        threads = [
            threading.Thread(target=run_actor, args=(name, fn), daemon=True)
            for name, fn in module.ACTORS.items()
        ]
        for t in threads:
            t.start()
        # Every constraint can stall the actors for up to one timeout.
        deadline = time.time() + 30 + len(orders) * self.test.timeout_ms / 1000
        for t in threads:
            t.join(max(0.0, deadline - time.time()))
        for name in module.ACTORS:
            outcomes.setdefault(name, {"ok": False, "error": "actor did not finish"})

        time.sleep(self.test.settle_s)
        duration = time.time() - started
        self.race_dir.clear_control()

        with self.connect("race-teardown") as conn:
            if hasattr(module, "teardown"):
                module.teardown(conn.cursor())

        violations = []
        if hasattr(module, "check"):
            try:
                violations = list(module.check(outcomes))
            except Exception as e:
                violations = [f"check raised {type(e).__name__}: {e}"]

        constraints = constraint_status(orders, self.race_dir.events(run_id))
        result = RunResult(
            id=run_id,
            label=label,
            test=self.test.name,
            orders=orders,
            started=started,
            duration_s=duration,
            outcomes=outcomes,
            violations=violations,
            constraints=constraints,
            status=run_status(constraints),
        )
        self.race_dir.save(result)
        return result


def constraint_status(
    orders: list[Order], events: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Reports for each constraint whether it fired and whether its waiter
    resumed because of it or timed out."""
    status = [
        {"woke": False, "waited": False, "resumed": False, "timed_out": False}
        for _ in orders
    ]
    key = {"wake": "woke", "wait": "waited", "resume": "resumed", "timeout": "timed_out"}
    for e in events:
        c = e.get("constraint")
        if c is not None and 0 <= c < len(status) and e["event"] in key:
            status[c][key[e["event"]]] = True
    return status


def run_status(constraints: list[dict[str, Any]]) -> str:
    """Summarizes a run like the MAccConc A-B-A tester does: `reordered` if
    every constraint was enforced, `wait-timeout` if an ordering proved
    impossible, `injection-failed` if a constrained hit never happened."""
    if any(c["timed_out"] for c in constraints):
        return "wait-timeout"
    if any(not (c["waited"] and c["resumed"]) for c in constraints):
        return "injection-failed"
    return "reordered" if constraints else "baseline"


def outcome_signature(result: RunResult) -> str:
    """Groups runs whose actors observed the same results."""
    parts = []
    for name in sorted(result.outcomes):
        o = result.outcomes[name]
        parts.append(f"{name}: " + ("ok" if o["ok"] else o.get("error", "error")))
    parts.extend(f"violation: {v}" for v in result.violations)
    return "\n".join(parts)


def aba_schedules(events: list[dict[str, Any]], all_points: bool) -> list[tuple[str, list[Order]]]:
    """Builds A-B-A schedules from a baseline trace.

    For each ordered pair of actors (A, B) and each hit `h` of A, A pauses at
    `h` while B runs from its first to its last baseline hit, then A resumes.
    Without `all_points`, `h` is restricted to communication points: hits on
    a key that B also touches, at least one of the two not a read.
    """
    hits: dict[str, list[dict[str, Any]]] = {}
    for e in events:
        if e["event"] == "hit":
            hits.setdefault(e["actor"], []).append(e)

    schedules = []
    for a, a_hits in hits.items():
        for b, b_hits in hits.items():
            if a == b:
                continue
            first, last = b_hits[0], b_hits[-1]
            b_keys: dict[str, set[str]] = {}
            for e in b_hits:
                if "key" in e:
                    b_keys.setdefault(e["key"], set()).add(e.get("access", "write"))
            for h in a_hits:
                if not all_points:
                    accesses = b_keys.get(h.get("key", ""), set())
                    if not accesses or (
                        h.get("access") == "read" and accesses == {"read"}
                    ):
                        continue
                hid = HitId(a, h["point"], h["hit"])
                orders = [
                    # A reaching `h` releases B.
                    Order(hid, HitId(b, first["point"], first["hit"]), post=False),
                    # A stays at `h` until B left its last point.
                    Order(HitId(b, last["point"], last["hit"]), hid, post=True),
                ]
                schedules.append((f"{a}@{h['point']}#{h['hit']} ⟂ {b}", orders))
    return schedules


def _jsonable(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return repr(value)
