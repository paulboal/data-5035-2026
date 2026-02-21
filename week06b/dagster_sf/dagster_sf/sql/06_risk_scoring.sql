CREATE DATABASE IF NOT EXISTS {{ database }};
CREATE SCHEMA IF NOT EXISTS {{ database }}.{{ schema }};

CREATE OR REPLACE TABLE {{ database }}.{{ schema }}.patient_risk_scores AS
SELECT
  patient_id,
  site_id,
  lab_date,
  biomarker_a,
  biomarker_b,
  LEAST(100.0, GREATEST(0.0, ROUND((biomarker_a * 20.0) + (biomarker_b * 8.0), 2))) AS risk_score,
  run_date
FROM {{ database }}.{{ schema }}.silver_lab_results;
