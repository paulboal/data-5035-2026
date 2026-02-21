# Airflow + Databricks Demo: Clinical Trial Enrollment Pipeline

This demo shows an Apache Airflow DAG orchestrating a clinical trial enrollment workflow where compute steps run on Databricks notebooks (or in local simulation mode).

## What this demo demonstrates

- Parallel fan-out/fan-in orchestration with `TaskGroup`
  - Parallel ingest: patient screenings, lab results, and site reference
  - Parallel compute: eligibility + risk scoring, then fan-in to consolidate metrics
- Decision logic in Airflow
  - `ShortCircuitOperator` data-quality gate (`dq_gate`)
  - `BranchPythonOperator` publish-vs-quarantine branch
- External Databricks execution via `DatabricksSubmitRunOperator` (`/api/2.1/jobs/runs/submit` semantics)
- A local `SIMULATE_DATABRICKS` mode that prints exact submitRun JSON payloads without any Databricks API calls

## Folder structure

- `dags/clinical_trial_enrollment_airflow_dbx.py`: Airflow DAG
- `DAG_DIAGRAM.md`: Mermaid visualization of the DAG flow
- `DAG_DIAGRAM.png`: Rendered DAG diagram image
- `databricks/notebooks/*.py`: Databricks notebook sources
- `databricks/job_definitions/*.json`: submitRun payload templates (one per notebook)
- `data/sample_inputs/*.csv`: sample data files
- `scripts/upload_notebooks_to_databricks.py`: optional uploader to Databricks Workspace
- `scripts/validate_local_inputs.py`: optional local CSV validator
- `scripts/start_airflow_local.sh`: local startup helper with consistent Airflow env vars

## Databricks connection required by Airflow

Create Airflow connection ID `databricks_default` with:

- Host: your Databricks workspace URL (for example `https://adb-<workspace-id>.<region>.azuredatabricks.net`)
- Password: Databricks Personal Access Token (PAT)

You can configure this in Airflow UI (`Admin -> Connections`) or via env vars/CLI.

## Databricks notebook workspace path

Upload notebooks to this workspace base path:

- `/Shared/data-5035/week07/clinical_trial_demo`

Expected notebook paths are:

- `/Shared/data-5035/week07/clinical_trial_demo/01_ingest_patient_screenings`
- `/Shared/data-5035/week07/clinical_trial_demo/02_ingest_lab_results`
- `/Shared/data-5035/week07/clinical_trial_demo/03_load_site_reference`
- `/Shared/data-5035/week07/clinical_trial_demo/04_validate_and_clean`
- `/Shared/data-5035/week07/clinical_trial_demo/05_eligibility_check`
- `/Shared/data-5035/week07/clinical_trial_demo/06_risk_scoring`
- `/Shared/data-5035/week07/clinical_trial_demo/07_consolidate_metrics`
- `/Shared/data-5035/week07/clinical_trial_demo/08_publish_trial_summary`
- `/Shared/data-5035/week07/clinical_trial_demo/09_quarantine_run`

Use the helper script if you want automated upload:

```bash
cd week07/airflow_dbx
export DATABRICKS_HOST="https://<your-workspace-url>"
export DATABRICKS_TOKEN="<your-pat>"
python scripts/upload_notebooks_to_databricks.py
```

## Simulate mode (no Databricks API calls)

The DAG has `SIMULATE_DATABRICKS = False` by default. To enable simulation without editing code:

```bash
export SIMULATE_DATABRICKS=true
```

In simulate mode, each Databricks task becomes a `PythonOperator` that pretty-prints and returns the exact submitRun payload it would send.

The DAG also auto-falls back to simulation if `apache-airflow-providers-databricks` is not installed, so the DAG can still import.

## Local Airflow run (dev)

1. Activate your Python environment (example using your existing `data5035` env):

```bash
pyenv activate data5035
```

2. Install dependencies in your Airflow environment:

```bash
cd week07/airflow_dbx
pip install -r requirements.txt
```

3. Initialize Airflow metadata with the local helper script:

```bash
./scripts/start_airflow_local.sh init
./scripts/start_airflow_local.sh reserialize
./scripts/start_airflow_local.sh list-local | rg clinical_trial_enrollment_airflow_dbx
```

4. Start Airflow services in separate terminals:

Terminal 1:

```bash
cd week07/airflow_dbx
./scripts/start_airflow_local.sh api-server
```

Terminal 2:

```bash
cd week07/airflow_dbx
./scripts/start_airflow_local.sh scheduler
```

The helper script defaults to:

- `AIRFLOW_HOME=week07/airflow_dbx/.airflow`
- `AIRFLOW__CORE__DAGS_FOLDER=week07/airflow_dbx/dags`
- `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin`
- Airflow binary from your `data5035` env: `~/.pyenv/versions/data5035/bin/airflow`

5. Trigger DAG `clinical_trial_enrollment_airflow_dbx` manually with params such as:

```json
{
  "run_date": "2026-02-21",
  "publish_enabled": true,
  "impact_flag": true,
  "min_rows": 25,
  "patient_rows": 100,
  "lab_rows": 100
}
```

To force DQ failure, set `patient_rows` or `lab_rows` below `min_rows`.

To force quarantine branch, set either `publish_enabled=false` or `impact_flag=false`.

Manual fallback (without helper script):

```bash
export AIRFLOW_HOME="/absolute/path/to/week07/airflow_dbx/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="/absolute/path/to/week07/airflow_dbx/dags"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="admin:admin"
airflow db reset -y
airflow db migrate
airflow dags reserialize
airflow api-server --port 8080
airflow scheduler
```

## Notes

- Update cluster placeholders in DAG/JSON templates (`spark_version`, `node_type_id`) for your Databricks cloud.
- Notebooks create/write Delta tables in schema `data5035_week07`:
  - `bronze_patient_screenings`
  - `bronze_lab_results`
  - `dim_sites`
  - `silver_patient_screenings`
  - `silver_lab_results`
  - `eligible_patients`
  - `patient_risk_scores`
  - `trial_metrics`
  - `trial_daily_status`
  - `quarantine_trial_run`
