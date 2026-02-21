"""Airflow DAG for a Clinical Trial Enrollment Pipeline orchestrated with Databricks."""

from __future__ import annotations

import json
import os
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

try:
    from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

    HAS_DATABRICKS_PROVIDER = True
except ImportError:  # pragma: no cover - handles local environments without provider installed
    DatabricksSubmitRunOperator = None
    HAS_DATABRICKS_PROVIDER = False

DATABRICKS_CONN_ID = "databricks_default"
SIMULATE_DATABRICKS = False
DBX_NOTEBOOK_BASE = "/Shared/data-5035/week07/clinical_trial_demo"
SIMULATE_DATABRICKS = os.getenv("SIMULATE_DATABRICKS", str(SIMULATE_DATABRICKS)).lower() in {
    "1",
    "true",
    "yes",
}

# IMPORTANT: update spark_version and node_type_id to values valid for your cloud (AWS/Azure/GCP).
JOB_CLUSTER = {
    "job_cluster_key": "demo_cluster",
    "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 1,
    },
}


def _build_submit_run_payload(run_name: str, notebook_rel: str) -> dict:
    return {
        "run_name": run_name,
        "job_clusters": [JOB_CLUSTER],
        "tasks": [
            {
                "task_key": "main",
                "job_cluster_key": "demo_cluster",
                "notebook_task": {
                    "notebook_path": f"{DBX_NOTEBOOK_BASE}/{notebook_rel}",
                    "base_parameters": {"run_date": "{{ params.run_date }}"},
                },
            }
        ],
    }


def _simulate_submit_run(task_id: str, payload: dict, **_: dict) -> dict:
    print(f"[SIMULATE_DATABRICKS] task_id={task_id}")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return payload


def _build_dbx_or_simulated_task(task_id: str, notebook_rel: str):
    payload = _build_submit_run_payload(run_name=f"clinical_trial_{task_id}", notebook_rel=notebook_rel)
    simulate_mode = SIMULATE_DATABRICKS or not HAS_DATABRICKS_PROVIDER

    if simulate_mode:
        return PythonOperator(
            task_id=task_id,
            python_callable=_simulate_submit_run,
            op_kwargs={"task_id": task_id, "payload": payload},
        )

    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=payload,
    )


def _decide_dq_gate(**context) -> bool:
    params = context["params"]
    patient_rows = int(params["patient_rows"])
    lab_rows = int(params["lab_rows"])
    min_rows = int(params["min_rows"])
    passed = patient_rows >= min_rows and lab_rows >= min_rows
    print(
        f"DQ decision: patient_rows={patient_rows}, lab_rows={lab_rows}, min_rows={min_rows}, passed={passed}"
    )
    return passed


def _choose_publish_or_quarantine(**context) -> str:
    params = context["params"]
    publish_enabled = bool(params["publish_enabled"])
    impact_flag = bool(params["impact_flag"])
    publish = publish_enabled and impact_flag
    selected = "publish.publish_trial_summary" if publish else "publish.quarantine_run"
    print(
        f"Branch decision: publish_enabled={publish_enabled}, impact_flag={impact_flag}, selected={selected}"
    )
    return selected


with DAG(
    dag_id="clinical_trial_enrollment_airflow_dbx",
    description="Clinical trial enrollment pipeline with Databricks execution and simulate mode",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "run_date": Param("2026-02-21", type="string"),
        "publish_enabled": Param(True, type="boolean"),
        "impact_flag": Param(True, type="boolean"),
        "min_rows": Param(25, type="integer"),
        "patient_rows": Param(100, type="integer"),
        "lab_rows": Param(100, type="integer"),
    },
    tags=["week07", "databricks", "clinical-trial"],
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="ingest") as ingest_group:
        ingest_patient_screenings = _build_dbx_or_simulated_task(
            task_id="ingest_patient_screenings",
            notebook_rel="01_ingest_patient_screenings",
        )
        ingest_lab_results = _build_dbx_or_simulated_task(
            task_id="ingest_lab_results",
            notebook_rel="02_ingest_lab_results",
        )
        load_site_reference = _build_dbx_or_simulated_task(
            task_id="load_site_reference",
            notebook_rel="03_load_site_reference",
        )

    validate_and_clean = _build_dbx_or_simulated_task(
        task_id="validate_and_clean",
        notebook_rel="04_validate_and_clean",
    )

    dq_gate = ShortCircuitOperator(task_id="dq_gate", python_callable=_decide_dq_gate)

    with TaskGroup(group_id="compute") as compute_group:
        eligibility_check = _build_dbx_or_simulated_task(
            task_id="eligibility_check",
            notebook_rel="05_eligibility_check",
        )
        risk_scoring = _build_dbx_or_simulated_task(
            task_id="risk_scoring",
            notebook_rel="06_risk_scoring",
        )
        consolidate_metrics = _build_dbx_or_simulated_task(
            task_id="consolidate_metrics",
            notebook_rel="07_consolidate_metrics",
        )

        [eligibility_check, risk_scoring] >> consolidate_metrics

    with TaskGroup(group_id="publish") as publish_group:
        choose_publish_or_quarantine = BranchPythonOperator(
            task_id="choose_publish_or_quarantine",
            python_callable=_choose_publish_or_quarantine,
        )

        publish_trial_summary = _build_dbx_or_simulated_task(
            task_id="publish_trial_summary",
            notebook_rel="08_publish_trial_summary",
        )
        quarantine_run = _build_dbx_or_simulated_task(
            task_id="quarantine_run",
            notebook_rel="09_quarantine_run",
        )

        publish_done = EmptyOperator(
            task_id="publish_done",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        choose_publish_or_quarantine >> [publish_trial_summary, quarantine_run]
        [publish_trial_summary, quarantine_run] >> publish_done

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    start >> ingest_group >> validate_and_clean >> dq_gate >> compute_group >> publish_group >> end
