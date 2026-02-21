# Dagster + Snowflake Demo: Clinical Trial Enrollment Pipeline

This project recreates the week07 clinical trial orchestration demo using **Dagster** and **Snowflake**.

## What this demonstrates

- Parallel fan-out/fan-in orchestration:
  - Ingest fan-out: screenings, labs, site reference
  - Compute fan-in: eligibility + risk scoring -> consolidate
- Decision logic:
  - `dq_gate` skips compute/publish if DQ fails
  - `choose_publish_or_quarantine` routes to publish or quarantine
- External warehouse execution:
  - Each pipeline step executes Snowflake SQL in `dagster_sf/sql/*.sql`
- Local grading/testing mode:
  - `SIMULATE_SNOWFLAKE=true` prints SQL statements instead of executing

## Project structure

- `dagster_sf/definitions.py`: Dagster definitions entrypoint
- `dagster_sf/jobs/clinical_trial_enrollment_sf.py`: job graph and ops
- `dagster_sf/resources.py`: Snowflake execution resource + simulate fallback
- `dagster_sf/sql/*.sql`: 9 Snowflake SQL step scripts
- `run_config/example_run_config.yaml`: launchpad run-config template
- `scripts/run_job.py`: run the job from CLI
- `scripts/start_dagster_dev.sh`: starts Dagster UI/API locally using module loading (`-m dagster_sf.definitions`)
- `scripts/upload_sample_csvs_to_snowflake.py`: optional CSV loader helper
- `data/sample_inputs/*.csv`: sample input files
- `DAG_DIAGRAM.md`: graph visualization

## Use your existing Python environment

```bash
pyenv activate data5035
cd week07/dagster_sf
pip install -r requirements.txt
```

## Snowflake credentials and settings

Set env vars before real Snowflake runs:

```bash
export SNOWFLAKE_ACCOUNT="<account_identifier>"
export SNOWFLAKE_USER="<username>"
export SNOWFLAKE_PASSWORD="<password>"
export SNOWFLAKE_WAREHOUSE="<warehouse>"
export SNOWFLAKE_DATABASE="DEMO_DB"
export SNOWFLAKE_SCHEMA="DATA5035_WEEK07"
export SNOWFLAKE_ROLE="<optional_role>"
```

## Simulate mode (recommended for local validation)

```bash
export SIMULATE_SNOWFLAKE=true
python scripts/run_job.py --run-date 2026-02-21
```

This will print every SQL statement the pipeline would execute.

## Real Snowflake run

```bash
export SIMULATE_SNOWFLAKE=false
python scripts/run_job.py --run-date 2026-02-21
```

## Run with Dagster UI

Terminal 1:

```bash
pyenv activate data5035
cd week07/dagster_sf
./scripts/start_dagster_dev.sh
```

Open [http://localhost:3000](http://localhost:3000), select job `clinical_trial_enrollment_dagster_sf`, and launch runs.

You can paste `run_config/example_run_config.yaml` into Launchpad config and adjust values.

## Branch/DQ test examples

- Force DQ fail (skip compute/publish):

```bash
python scripts/run_job.py --patient-rows 10 --lab-rows 10 --min-rows 25 --simulate-snowflake
```

- Force quarantine branch:

```bash
python scripts/run_job.py --no-impact-flag --simulate-snowflake
```

## Snowflake tables created

Schema: `DEMO_DB.DATA5035_WEEK07` (or your configured DB/schema)

- `bronze_patient_screenings`
- `bronze_lab_results`
- `dim_sites`
- `silver_patient_screenings`
- `silver_lab_results`
- `eligible_patients`
- `patient_risk_scores`
- `trial_metrics`
- `trial_daily_status`
- `trial_publish_manifest`
- `quarantine_trial_run`

## Notes

- The SQL ingest steps are deterministic synthetic inserts for teaching consistency.
- Optional helper `scripts/upload_sample_csvs_to_snowflake.py` can load provided CSVs into raw tables if you want to extend the ingest SQL to read those tables.
