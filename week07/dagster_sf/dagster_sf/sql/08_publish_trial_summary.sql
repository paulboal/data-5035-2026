CREATE DATABASE IF NOT EXISTS {{ database }};
CREATE SCHEMA IF NOT EXISTS {{ database }}.{{ schema }};

CREATE TABLE IF NOT EXISTS {{ database }}.{{ schema }}.trial_daily_status (
  site_id STRING,
  run_date DATE,
  eligible_count NUMBER,
  avg_risk_score FLOAT,
  pipeline_status STRING,
  published_ts TIMESTAMP_NTZ
);

DELETE FROM {{ database }}.{{ schema }}.trial_daily_status WHERE run_date = '{{ run_date }}';

INSERT INTO {{ database }}.{{ schema }}.trial_daily_status
SELECT
  site_id,
  run_date,
  eligible_count,
  avg_risk_score,
  'PUBLISHED' AS pipeline_status,
  CURRENT_TIMESTAMP() AS published_ts
FROM {{ database }}.{{ schema }}.trial_metrics
WHERE run_date = '{{ run_date }}';

CREATE TABLE IF NOT EXISTS {{ database }}.{{ schema }}.trial_publish_manifest (
  run_date DATE,
  target_table STRING,
  status STRING,
  created_ts TIMESTAMP_NTZ
);

INSERT INTO {{ database }}.{{ schema }}.trial_publish_manifest
SELECT
  '{{ run_date }}'::DATE,
  'trial_daily_status',
  'PUBLISHED',
  CURRENT_TIMESTAMP();
