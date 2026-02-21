CREATE DATABASE IF NOT EXISTS {{ database }};
CREATE SCHEMA IF NOT EXISTS {{ database }}.{{ schema }};

CREATE OR REPLACE TABLE {{ database }}.{{ schema }}.eligible_patients AS
SELECT
  patient_id,
  site_id,
  screening_date,
  age,
  sex,
  consent_flag,
  'eligible' AS eligibility_status,
  run_date
FROM {{ database }}.{{ schema }}.silver_patient_screenings
WHERE age BETWEEN 18 AND 75
  AND consent_flag = 'Y';
