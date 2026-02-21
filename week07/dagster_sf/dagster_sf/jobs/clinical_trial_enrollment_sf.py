from __future__ import annotations

from pathlib import Path
from typing import Any

from dagster import Out, Output, graph, op

from dagster_sf.resources import default_snowflake_resource_config, snowflake_runner_resource

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def _run_sql_step(context, step: str, params: dict[str, Any]) -> dict[str, Any]:
    sql_file = SQL_DIR / f"{step}.sql"
    snowflake_runner = context.resources.snowflake
    merged_params = {
        **params,
        "database": snowflake_runner.database,
        "schema": snowflake_runner.schema,
    }
    statements = context.resources.snowflake.execute_sql_file(
        sql_file=sql_file,
        params=merged_params,
        step_name=step,
    )
    context.log.info(f"Executed {len(statements)} statement(s) for {step}")
    return {"step": step, "statements_executed": len(statements)}


@op(
    config_schema={
        "run_date": str,
        "publish_enabled": bool,
        "impact_flag": bool,
        "min_rows": int,
        "patient_rows": int,
        "lab_rows": int,
    }
)
def load_runtime_params(context) -> dict[str, Any]:
    params = dict(context.op_config)
    context.log.info(f"Runtime params: {params}")
    return params


@op(required_resource_keys={"snowflake"})
def ingest_patient_screenings(context, params: dict[str, Any]) -> dict[str, Any]:
    return _run_sql_step(context, "01_ingest_patient_screenings", params)


@op(required_resource_keys={"snowflake"})
def ingest_lab_results(context, params: dict[str, Any]) -> dict[str, Any]:
    return _run_sql_step(context, "02_ingest_lab_results", params)


@op(required_resource_keys={"snowflake"})
def load_site_reference(context, params: dict[str, Any]) -> dict[str, Any]:
    return _run_sql_step(context, "03_load_site_reference", params)


@op(required_resource_keys={"snowflake"})
def validate_and_clean(
    context,
    params: dict[str, Any],
    _screenings_done: dict[str, Any],
    _labs_done: dict[str, Any],
    _sites_done: dict[str, Any],
) -> dict[str, Any]:
    return _run_sql_step(context, "04_validate_and_clean", params)


@op(out={"continue_compute": Out(bool, is_required=False)})
def dq_gate(context, params: dict[str, Any], _validate_done: dict[str, Any]):
    patient_rows = int(params["patient_rows"])
    lab_rows = int(params["lab_rows"])
    min_rows = int(params["min_rows"])
    passed = patient_rows >= min_rows and lab_rows >= min_rows
    context.log.info(
        f"DQ decision patient_rows={patient_rows} lab_rows={lab_rows} min_rows={min_rows} passed={passed}"
    )
    if passed:
        yield Output(True, output_name="continue_compute")


@op(required_resource_keys={"snowflake"})
def eligibility_check(context, params: dict[str, Any], gate_passed: bool) -> dict[str, Any]:
    del gate_passed
    return _run_sql_step(context, "05_eligibility_check", params)


@op(required_resource_keys={"snowflake"})
def risk_scoring(context, params: dict[str, Any], gate_passed: bool) -> dict[str, Any]:
    del gate_passed
    return _run_sql_step(context, "06_risk_scoring", params)


@op(required_resource_keys={"snowflake"})
def consolidate_metrics(
    context,
    params: dict[str, Any],
    _eligibility_done: dict[str, Any],
    _risk_done: dict[str, Any],
) -> dict[str, Any]:
    return _run_sql_step(context, "07_consolidate_metrics", params)


@op(out={"publish": Out(str, is_required=False), "quarantine": Out(str, is_required=False)})
def choose_publish_or_quarantine(context, params: dict[str, Any], _consolidated: dict[str, Any]):
    publish_enabled = bool(params["publish_enabled"])
    impact_flag = bool(params["impact_flag"])
    publish = publish_enabled and impact_flag
    context.log.info(
        f"Branch decision publish_enabled={publish_enabled} impact_flag={impact_flag} publish={publish}"
    )
    if publish:
        yield Output("publish", output_name="publish")
    else:
        yield Output("quarantine", output_name="quarantine")


@op(required_resource_keys={"snowflake"})
def publish_trial_summary(context, params: dict[str, Any], publish_token: str) -> dict[str, Any]:
    del publish_token
    return _run_sql_step(context, "08_publish_trial_summary", params)


@op(required_resource_keys={"snowflake"})
def quarantine_run(context, params: dict[str, Any], quarantine_token: str) -> dict[str, Any]:
    del quarantine_token
    return _run_sql_step(context, "09_quarantine_run", params)


@graph
def clinical_trial_enrollment_sf_graph():
    params = load_runtime_params()

    screenings_done = ingest_patient_screenings(params)
    labs_done = ingest_lab_results(params)
    sites_done = load_site_reference(params)

    validated = validate_and_clean(params, screenings_done, labs_done, sites_done)
    gate_passed = dq_gate(params, validated)

    eligible_done = eligibility_check(params, gate_passed)
    risk_done = risk_scoring(params, gate_passed)
    consolidated = consolidate_metrics(params, eligible_done, risk_done)

    publish_token, quarantine_token = choose_publish_or_quarantine(params, consolidated)

    publish_trial_summary(params, publish_token)
    quarantine_run(params, quarantine_token)


clinical_trial_enrollment_sf_job = clinical_trial_enrollment_sf_graph.to_job(
    name="clinical_trial_enrollment_dagster_sf",
    resource_defs={"snowflake": snowflake_runner_resource},
    config={
        "ops": {
            "load_runtime_params": {
                "config": {
                    "run_date": "2026-02-21",
                    "publish_enabled": True,
                    "impact_flag": True,
                    "min_rows": 25,
                    "patient_rows": 100,
                    "lab_rows": 100,
                }
            }
        },
        "resources": {"snowflake": {"config": default_snowflake_resource_config()}},
    },
)
