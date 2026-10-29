# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Explore interleavings of race points in a running Materialize.

See doc/developer/race-points.md.
"""

import argparse
import collections
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg

from materialize import MZ_ROOT
from materialize.race_viz.runner import (
    RaceDir,
    Runner,
    TestCase,
    aba_schedules,
    outcome_signature,
)
from materialize.race_viz.server import serve

DEFAULT_RACE_DIR = MZ_ROOT / "mzdata" / "race"


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="race-viz",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("test", type=Path, help="race test case (a Python file)")
    parser.add_argument(
        "--url",
        default="postgres://materialize@127.0.0.1:6875/materialize",
        help="SQL URL of the environment under test",
    )
    parser.add_argument(
        "--race-dir",
        type=Path,
        default=Path(os.environ.get("MZ_RACE_DIR", DEFAULT_RACE_DIR)),
        help="the MZ_RACE_DIR environmentd runs with",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7878)
    parser.add_argument(
        "--start-environmentd",
        action="store_true",
        help="run bin/environmentd with MZ_RACE_DIR set, passing any arguments after --",
    )
    parser.add_argument(
        "--explore",
        action="store_true",
        help="run a baseline and every A-B-A schedule without the web UI, then exit",
    )
    parser.add_argument(
        "--all-points",
        action="store_true",
        help="with --explore, pause at every hit, not only communication points",
    )
    args, environmentd_args = parser.parse_known_args()
    if environmentd_args[:1] == ["--"]:
        environmentd_args = environmentd_args[1:]
    elif environmentd_args:
        parser.error(f"unrecognized arguments: {' '.join(environmentd_args)}")

    race_dir = RaceDir(args.race_dir.resolve())
    race_dir.clear_control()
    test = TestCase.load(args.test.resolve())
    runner = Runner(test, race_dir, args.url)

    environmentd = None
    if args.start_environmentd:
        environmentd = subprocess.Popen(
            [str(MZ_ROOT / "bin" / "environmentd"), *environmentd_args],
            env={**os.environ, "MZ_RACE_DIR": str(race_dir.root)},
        )
        wait_for_sql(args.url, environmentd)
    try:
        if args.explore:
            return explore(runner, args.all_points)
        server = serve(runner, args.host, args.port)
        print(f"race-viz: serving {test.name} on http://{args.host}:{args.port}", flush=True)
        print(
            f"race-viz: environmentd must run with MZ_RACE_DIR={race_dir.root}",
            flush=True,
        )
        server.serve_forever()
        return 0
    except KeyboardInterrupt:
        return 0
    finally:
        race_dir.clear_control()
        if environmentd is not None:
            environmentd.terminate()
            environmentd.wait()


def wait_for_sql(url: str, process: subprocess.Popen, timeout_s: float = 600) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if process.poll() is not None:
            raise SystemExit(f"environmentd exited with {process.returncode}")
        try:
            psycopg.connect(url, connect_timeout=2).close()
            return
        except psycopg.OperationalError:
            time.sleep(1)
    raise SystemExit("environmentd did not accept SQL connections in time")


def explore(runner: Runner, all_points: bool) -> int:
    """Runs every A-B-A schedule and prints a summary, like `kcov-autorace`."""
    baseline = runner.run([], label="baseline")
    print(f"baseline {baseline.id}: {outcome_signature(baseline)!r}")
    schedules = aba_schedules(runner.race_dir.events(baseline.id), all_points)
    print(f"{len(schedules)} A-B-A schedules")

    statuses: collections.Counter[str] = collections.Counter()
    signatures: dict[str, list[str]] = collections.defaultdict(list)
    signatures[outcome_signature(baseline)].append(baseline.id)
    for label, orders in schedules:
        result = runner.run(orders, label=f"aba: {label}")
        statuses[result.status] += 1
        signature = outcome_signature(result)
        if signature not in signatures:
            print(f"NEW OUTCOME in {result.id} ({label}, {result.status}):")
            print("  " + signature.replace("\n", "\n  "))
        signatures[signature].append(result.id)
        print(f"{result.id} {result.status:16} {label}")

    print("stats: " + " ".join(f"{s}:{n}" for s, n in sorted(statuses.items())))
    print(f"{len(signatures)} distinct outcomes:")
    for signature, runs in signatures.items():
        print(f"  {len(runs):4} runs, e.g. {runs[0]}:")
        print("       " + signature.replace("\n", "\n       "))
    return 1 if any("violation:" in s for s in signatures) else 0


if __name__ == "__main__":
    sys.exit(main())
