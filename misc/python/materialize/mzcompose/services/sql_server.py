# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.service import Service, ServiceConfig


class SqlServer(Service):
    DEFAULT_USER = "SA"
    DEFAULT_SA_PASSWORD = "RPSsql12345"

    def __init__(
        self,
        # The password must be at least 8 characters including uppercase,
        # lowercase letters, base-10 digits and/or non-alphanumeric symbols.
        sa_password: str = DEFAULT_SA_PASSWORD,
        name: str = "sql-server",
        mzbuild: str = "sql-server",
        image: str | None = None,  # "mcr.microsoft.com/mssql/server",
        environment_extra: list[str] = [],
    ) -> None:
        config: ServiceConfig = {"image": image} if image else {"mzbuild": mzbuild}
        config.update(
            {
                # WARNING: The requested image's platform (linux/amd64) does not match the detected host platform (linux/arm64/v8) and no specific platform was requested
                # See See https://github.com/microsoft/mssql-docker/issues/802 for current status
                "platform": "linux/amd64",
                "ports": [1433],
                "environment": [
                    f"SA_PASSWORD={sa_password}",
                    *environment_extra,
                ],
            }
        )
        super().__init__(
            name=name,
            config=config,
        )
        self.sa_password = sa_password
