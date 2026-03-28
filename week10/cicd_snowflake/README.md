# Snowflake CI/CD Demo

## Overview

This project demonstrates a simple CI/CD pipeline for deploying Snowflake
objects using GitHub Actions. It deploys a `FACT_MONTHLY_SUMMARY` view that
aggregates encounter and unique patient counts by month from the `PATIENT`
and `ENCOUNTER` tables.

## Project Structure

```
cicd_snowflake/
├── .github/workflows/
│   └── snowflake_cicd.yml        # GitHub Actions pipeline
├── migrations/
│   └── V1__create_fact_monthly_summary.sql  # View DDL
├── tests/
│   └── test_fact_monthly_summary.sql        # Validation queries
└── README.md
```

## Environments

| Branch | Environment | Schema           | Tests            | Approval |
|--------|-------------|------------------|------------------|----------|
| dev    | dev         | DATA5035.DEV     | Full test suite  | None     |
| test   | test        | DATA5035.TEST    | Full test suite  | None     |
| main   | prod        | DATA5035.SPRING26| Basic validation | Required |

## Branching Strategy

1. Develop on a feature branch, merge to `dev` for initial deployment
2. After dev tests pass, merge `dev` into `test` for integration testing
3. After test passes, create a PR from `test` to `main`
4. Prod deployment requires manual approval via GitHub environment protection

## Required GitHub Secrets

Configure these in **Settings > Secrets and variables > Actions**:

| Secret                | Description                        |
|-----------------------|------------------------------------|
| SNOWFLAKE_ACCOUNT     | Snowflake account identifier       |
| SNOWFLAKE_USER        | Service account username           |
| SNOWFLAKE_PASSWORD    | Service account password           |
| SNOWFLAKE_WAREHOUSE   | Warehouse for running migrations   |
| SNOWFLAKE_ROLE        | Role with schema permissions       |

## Required GitHub Environments

Configure these in **Settings > Environments**:

- **dev** -- No protection rules
- **test** -- No protection rules
- **prod** -- Add required reviewers for manual approval

## Test Strategy

**Dev and Test** run four validation checks:
1. View exists and returns data
2. No NULL encounter months
3. All counts are positive
4. Unique patient count does not exceed encounter count

**Prod** runs basic validation:
1. View exists in the schema
2. View returns at least one row

Tests are designed as "fail-only" queries: they return rows only when
something is wrong. Zero rows returned means the test passed.

## Adding New Migrations

Add new SQL files to `migrations/` using the naming convention:

```
V{number}__{description}.sql
```

Migrations run in filename order. Each migration should be idempotent
(use `CREATE OR REPLACE` or `CREATE IF NOT EXISTS`).
