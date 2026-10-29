# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""CREATE REPLACEMENT against a target materialized view that seals concurrently.

A materialized view over a constant seals shortly after it is created. Whether
`CREATE REPLACEMENT` then succeeds depends on whether the coordinator's sealed
check (`mv_sealed_check`) runs before the controllers learn about the seal
(`compute_write_frontier_sealed`, `storage_write_frontier_sealed`). Both
outcomes are correct. Any other error is a violation.

Run with `bin/race-viz test/race/replacement_seal.py`.
"""


def setup(cur):
    cur.execute("DROP MATERIALIZED VIEW IF EXISTS race_mv CASCADE")


teardown = setup


def replace(cur):
    cur.execute("CREATE MATERIALIZED VIEW race_mv AS SELECT 1 AS a")
    cur.execute(
        "CREATE REPLACEMENT MATERIALIZED VIEW race_rp FOR race_mv AS SELECT 1 AS a"
    )
    return "replacement created"


ACTORS = {"replace": replace}


def check(outcomes):
    o = outcomes["replace"]
    if o["ok"] or "sealed" in o["error"]:
        return []
    return [f"unexpected error: {o['error']}"]
