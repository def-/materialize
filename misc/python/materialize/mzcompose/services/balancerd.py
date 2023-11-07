# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import random

from materialize.mzcompose import (
    DEFAULT_MZ_VOLUMES,
)
from materialize.mzcompose.service import (
    Service,
    ServiceDependency,
)


class Balancerd(Service):
    def __init__(
        self,
        name: str = "balancerd",
        mzbuild: str = "balancerd",
        entrypoint: str | None = None
    ) -> None:
        if entrypoint is None:
            entrypoint = [
                "balancerd",
                "service",
                "--pgwire-listen-addr=0.0.0.0:6875",
                "--https-listen-addr=0.0.0.0:6876",
                "--internal-http-listen-addr=0.0.0.0:6878",
                "--static-resolver-addr=materialized:6875",
                "--https-resolver-template='{}:6876'"
            ]

        super().__init__(
            name=name,
            config={
                "mzbuild": mzbuild,
                "entrypoint": entrypoint,
                "ports": [6875, 6876, 6877, 6878],
            },
        )
