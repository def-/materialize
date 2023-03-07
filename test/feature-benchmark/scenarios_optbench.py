# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Type

from parameterized import parameterized_class  # type: ignore

import materialize.optbench
import materialize.optbench.sql
from materialize.feature_benchmark.action import Action
from materialize.feature_benchmark.executor import Executor
from materialize.feature_benchmark.measurement_source import (
    MeasurementSource,
    Timestamp,
)
from materialize.feature_benchmark.scenario import Scenario


class OptbenchInit(Action):
    def __init__(self, no_indexes=False):
        self._executor: Optional[Executor] = None
        self.no_indexes = no_indexes

    def run(self, executor: Optional[Executor] = None) -> None:
        e = executor or self._executor
        statements = materialize.optbench.sql.parse_from_file(
            Path("misc/python/materialize/optbench/schema/tpch.sql")
        )
        if self.no_indexes:
            idx_re = re.compile(r"(create|create\s+default|drop)\s+index\s+")
            statements = [
                statement
                for statement in statements
                if not idx_re.match(statement.lower())
            ]
        e._composition.sql("\n".join(statements))


class OptbenchRun(MeasurementSource):
    def __init__(self, query: int):
        self._executor: Optional[Executor] = None
        self._query = query

    def run(self, executor: Optional[Executor] = None) -> List[Timestamp]:
        assert not (executor is None and self._executor is None)
        assert not (executor is not None and self._executor is not None)
        e = executor or self._executor

        queries = materialize.optbench.sql.parse_from_file(
            Path("misc/python/materialize/optbench/workload/tpch.sql")
        )
        assert 1 <= self._query <= len(queries)
        query = queries[self._query - 1]
        explain_query = materialize.optbench.sql.Query(query).explain(
            True, materialize.optbench.sql.Dialect.MZ
        )
        now = time.time()
        explain_output = materialize.optbench.sql.ExplainOutput(
            e._composition.sql_query(explain_query)[0][0]
        )
        timestamps = [now, now + float(explain_output.optimization_time())]
        return timestamps


class OptbenchTPCH(Scenario):
    @staticmethod
    def name_with_query(cls: Type["OptbenchTPCH"], num: int, params_dict: Dict):
        return f"OptbenchTPCHQ{params_dict['QUERY']:02d}"


@parameterized_class(
    [{"QUERY": i} for i in range(1, 23)], class_name_func=OptbenchTPCH.name_with_query
)
class OptbenchTPCHQ(OptbenchTPCH):
    """Run optbench TPCH for optimizer benchmarks"""

    def init(self) -> List[Action]:
        return [OptbenchInit()]

    def benchmark(self) -> MeasurementSource:
        return OptbenchRun(self.QUERY)
