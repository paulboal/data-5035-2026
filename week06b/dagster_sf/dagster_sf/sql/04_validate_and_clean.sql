CREATE DATABASE IF NOT EXISTS {{ database }};
CREATE SCHEMA IF NOT EXISTS {{ database }}.{{ schema }};

CREATE OR REPLACE TABLE {{ database }}.{{ schema }}.silver_patient_screenings AS
SELECT
  patient_id,
  site_id,
  screening_date,
  TRY_TO_NUMBER(age) AS age,
  COALESCE(sex, 'U') AS sex,
  UPPER(COALESCE(consent_flag, 'N')) AS consent_flag,
  run_date
FROM {{ database }}.{{ schema }}.bronze_patient_screenings
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY patient_id, site_id, screening_date
  ORDER BY screening_date DESC
) = 1;

CREATE OR REPLACE TABLE {{ database }}.{{ schema }}.silver_lab_results AS
SELECT
  patient_id,
  site_id,
  lab_date,
  COALESCE(biomarker_a, 0.0) AS biomarker_a,
  COALESCE(biomarker_b, 0.0) AS biomarker_b,
  run_date
FROM {{ database }}.{{ schema }}.bronze_lab_results
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY patient_id, site_id, lab_date
  ORDER BY lab_date DESC
) = 1;
