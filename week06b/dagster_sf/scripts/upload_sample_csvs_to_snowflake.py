#!/usr/bin/env python3
"""Optional helper to load local sample CSVs into Snowflake tables using pandas write_pandas."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd

try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
except ImportError as exc:
    print("Missing dependencies for Snowflake upload helper.")
    print("Install: pip install snowflake-connector-python[pandas] pandas")
    print(exc)
    raise SystemExit(1)


def main() -> int:
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print(f"Missing required env vars: {', '.join(missing)}")
        return 1

    database = os.getenv("SNOWFLAKE_DATABASE", "DEMO_DB")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "DATA5035_WEEK07")
    role = os.getenv("SNOWFLAKE_ROLE")

    root = Path(__file__).resolve().parents[1]
    sample_dir = root / "data" / "sample_inputs"

    conn_kwargs = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": database,
        "schema": schema,
    }
    if role:
        conn_kwargs["role"] = role

    with snowflake.connector.connect(**conn_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")

        mapping = {
            "patient_screenings.csv": "PATIENT_SCREENINGS_CSV",
            "lab_results.csv": "LAB_RESULTS_CSV",
            "site_reference.csv": "SITE_REFERENCE_CSV",
        }

        for file_name, table_name in mapping.items():
            path = sample_dir / file_name
            df = pd.read_csv(path)
            ok, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                table_name,
                database=database,
                schema=schema,
                auto_create_table=True,
                overwrite=True,
            )
            print(f"{file_name} -> {database}.{schema}.{table_name} ok={ok} rows={nrows} chunks={nchunks}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
