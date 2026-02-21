from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dagster import InitResourceContext, resource

try:
    import snowflake.connector

    HAS_SNOWFLAKE_CONNECTOR = True
except ImportError:
    snowflake = None
    HAS_SNOWFLAKE_CONNECTOR = False


@dataclass
class SnowflakeRunner:
    simulate: bool
    database: str
    schema: str
    warehouse: str | None
    role: str | None
    account: str | None
    user: str | None
    password: str | None

    def _connection_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "schema": self.schema,
        }
        if self.warehouse:
            kwargs["warehouse"] = self.warehouse
        if self.role:
            kwargs["role"] = self.role
        return kwargs

    @staticmethod
    def _render_sql(sql_text: str, params: dict[str, Any]) -> str:
        rendered = sql_text
        for key, value in params.items():
            safe = str(value).replace("'", "''")
            rendered = rendered.replace(f"{{{{ {key} }}}}", safe)
        return rendered

    def execute_sql_text(self, *, sql_text: str, params: dict[str, Any], step_name: str) -> list[str]:
        rendered = self._render_sql(sql_text, params)
        statements = [s.strip() for s in rendered.split(";") if s.strip()]

        if self.simulate or not HAS_SNOWFLAKE_CONNECTOR:
            print(f"[SIMULATE_SNOWFLAKE] step={step_name}")
            print(f"[SIMULATE_SNOWFLAKE] params={params}")
            for idx, statement in enumerate(statements, start=1):
                print(f"--- statement {idx} ---")
                print(statement + ";")
            return statements

        with snowflake.connector.connect(**self._connection_kwargs()) as conn:
            with conn.cursor() as cur:
                for statement in statements:
                    cur.execute(statement)
        return statements

    def execute_sql_file(self, *, sql_file: Path, params: dict[str, Any], step_name: str) -> list[str]:
        sql_text = sql_file.read_text(encoding="utf-8")
        return self.execute_sql_text(sql_text=sql_text, params=params, step_name=step_name)


@resource
def snowflake_runner_resource(context: InitResourceContext) -> SnowflakeRunner:
    cfg = context.resource_config
    simulate = bool(cfg["simulate"])

    return SnowflakeRunner(
        simulate=simulate,
        account=cfg.get("account"),
        user=cfg.get("user"),
        password=cfg.get("password"),
        warehouse=cfg.get("warehouse"),
        database=cfg["database"],
        schema=cfg["schema"],
        role=cfg.get("role"),
    )


def default_snowflake_resource_config() -> dict[str, Any]:
    simulate_default = os.getenv("SIMULATE_SNOWFLAKE", "false").lower() in {"1", "true", "yes"}
    return {
        "simulate": simulate_default,
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "DEMO_DB"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "DATA5035_WEEK07"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }
