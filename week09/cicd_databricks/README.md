# Databricks CI/CD Demo

## Overview

This project demonstrates a CI/CD pipeline for deploying Databricks notebooks
using GitHub Actions. It deploys and runs a `monthly_summary` notebook that
creates a `fact_monthly_summary` view aggregating encounter and unique patient
counts by month from the `patient` and `encounter` tables.

This is the Databricks equivalent of the Snowflake CI/CD demo in
`../cicd_snowflake/`.

## Project Structure

```
cicd_databricks/
├── .github/workflows/
│   └── databricks_cicd.yml           # GitHub Actions pipeline
├── notebooks/
│   └── monthly_summary.py            # Deployment notebook (creates view)
├── tests/
│   └── test_monthly_summary.py       # Validation notebook (assert-based)
└── README.md
```

Notebooks use Databricks source format (`.py` with `# COMMAND ----------`
cell separators) for clean git diffs. Databricks imports these natively.

## Environments

| Branch | Environment | Schema   | Workspace Path      | Tests            | Approval |
|--------|-------------|----------|---------------------|------------------|----------|
| dev    | dev         | dev      | /Repos/cicd/dev     | Full test suite  | None     |
| test   | test        | test     | /Repos/cicd/test    | Full test suite  | None     |
| main   | prod        | spring26 | /Repos/cicd/prod    | Basic validation | Required |

## Branching Strategy

1. Develop on a feature branch, merge to `dev` for initial deployment
2. After dev tests pass, merge `dev` into `test` for integration testing
3. After test passes, create a PR from `test` to `main`
4. Prod deployment requires manual approval via GitHub environment protection

## Pipeline Steps

Each environment follows this sequence:

1. **Import notebooks** -- `databricks workspace import` uploads notebooks
   from the repo to the target workspace path
2. **Run deployment notebook** -- `databricks jobs submit` executes
   `monthly_summary` as a one-time run, passing the target schema as a
   notebook widget parameter
3. **Run tests** -- The test notebook runs the same way; any `AssertionError`
   causes the job to fail with a non-zero exit, which the pipeline detects

Prod skips the full test suite and re-runs the deployment notebook as a basic
validation (confirms it can execute without error).

## Required GitHub Secrets

Configure these in **Settings > Secrets and variables > Actions**:

| Secret                 | Description                                |
|------------------------|--------------------------------------------|
| DATABRICKS_HOST        | Workspace URL (e.g., https://adb-xxx.azuredatabricks.net) |
| DATABRICKS_TOKEN       | Personal access token or service principal token |
| DATABRICKS_CLUSTER_ID  | ID of the cluster to run notebooks on      |

## Required GitHub Environments

Configure these in **Settings > Environments**:

- **dev** -- No protection rules
- **test** -- No protection rules
- **prod** -- Add required reviewers for manual approval

## Key Differences from Snowflake Version

| Aspect           | Snowflake (`cicd_snowflake`)      | Databricks (`cicd_databricks`)       |
|------------------|-----------------------------------|--------------------------------------|
| Deployment unit  | SQL migration files               | Python notebooks                     |
| CLI tool         | Snow CLI (`snow sql`)             | Databricks CLI (`databricks`)        |
| Execution        | Direct SQL execution              | Notebook job runs via Jobs API       |
| Environment      | Schema per environment            | Schema + workspace path              |
| Testing          | SQL fail-on-rows queries          | Python `assert` statements           |
| Auth             | Account + user + password + role  | Host + token                         |

## Notebook Parameters

Both notebooks accept a `schema` widget parameter that controls which
schema the view is created in. This is how the same notebook code works
across all three environments:

- dev: `schema=dev`
- test: `schema=test`
- prod: `schema=spring26`
