CREATE DATABASE IF NOT EXISTS {{ database }};
CREATE SCHEMA IF NOT EXISTS {{ database }}.{{ schema }};

CREATE OR REPLACE TABLE {{ database }}.{{ schema }}.trial_metrics AS
SELECT
  e.site_id,
  e.run_date,
  COUNT(DISTINCT e.patient_id) AS eligible_count,
  ROUND(AVG(r.risk_score), 2) AS avg_risk_score
FROM {{ database }}.{{ schema }}.eligible_patients e
LEFT JOIN {{ database }}.{{ schema }}.patient_risk_scores r
  ON e.patient_id = r.patient_id
 AND e.site_id = r.site_id
GROUP BY e.site_id, e.run_date;
