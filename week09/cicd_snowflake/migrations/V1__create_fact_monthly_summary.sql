-- Migration: V1
-- Description: Create FACT_MONTHLY_SUMMARY view
-- Dependencies: PATIENT, ENCOUNTER tables must exist in target schema

CREATE OR REPLACE VIEW FACT_MONTHLY_SUMMARY AS
SELECT
  DATE_TRUNC('MONTH', e.encounter_date) AS encounter_month,
  COUNT(e.encounter_id) AS encounter_count,
  COUNT(DISTINCT p.patient_id) AS unique_patient_count
FROM
  encounter e INNER JOIN
  patient p ON e.patient_sk = p.patient_sk
GROUP BY
  encounter_month
ORDER BY
  encounter_month;
