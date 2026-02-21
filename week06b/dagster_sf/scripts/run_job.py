#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dagster_sf.jobs.clinical_trial_enrollment_sf import clinical_trial_enrollment_sf_job
from dagster_sf.resources import default_snowflake_resource_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the clinical trial Dagster + Snowflake job once.")
    parser.add_argument("--run-date", default="2026-02-21")
    parser.add_argument("--publish-enabled", action="store_true", default=True)
    parser.add_argument("--no-publish-enabled", dest="publish_enabled", action="store_false")
    parser.add_argument("--impact-flag", action="store_true", default=True)
    parser.add_argument("--no-impact-flag", dest="impact_flag", action="store_false")
    parser.add_argument("--min-rows", type=int, default=25)
    parser.add_argument("--patient-rows", type=int, default=100)
    parser.add_argument("--lab-rows", type=int, default=100)
    parser.add_argument(
        "--simulate-snowflake",
        action="store_true",
        default=os.getenv("SIMULATE_SNOWFLAKE", "false").lower() in {"1", "true", "yes"},
        help="Print SQL instead of executing against Snowflake.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    resource_cfg = default_snowflake_resource_config()
    resource_cfg["simulate"] = args.simulate_snowflake

    run_config = {
        "ops": {
            "load_runtime_params": {
                "config": {
                    "run_date": args.run_date,
                    "publish_enabled": args.publish_enabled,
                    "impact_flag": args.impact_flag,
                    "min_rows": args.min_rows,
                    "patient_rows": args.patient_rows,
                    "lab_rows": args.lab_rows,
                }
            }
        },
        "resources": {
            "snowflake": {
                "config": resource_cfg,
            }
        },
    }

    result = clinical_trial_enrollment_sf_job.execute_in_process(run_config=run_config)
    if not result.success:
        print("Job execution failed.", file=sys.stderr)
        return 1

    print("Job execution succeeded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
