CREATE DATABASE IF NOT EXISTS {{ database }};
CREATE SCHEMA IF NOT EXISTS {{ database }}.{{ schema }};

CREATE TABLE IF NOT EXISTS {{ database }}.{{ schema }}.quarantine_trial_run (
  run_date DATE,
  reason STRING,
  status STRING,
  recorded_ts TIMESTAMP_NTZ
);

INSERT INTO {{ database }}.{{ schema }}.quarantine_trial_run
SELECT
  '{{ run_date }}'::DATE,
  'Publish branch not selected or DQ gate failed',
  'QUARANTINED',
  CURRENT_TIMESTAMP();
