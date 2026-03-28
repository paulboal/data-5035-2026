-- Test Suite: FACT_MONTHLY_SUMMARY
-- Each query returns rows ONLY on failure. Zero rows = pass.
-- SnowSQL exits non-zero if a query errors, and the pipeline
-- checks row counts to detect logical failures.

-- Test 1: View must exist and return at least one row
-- Fails if the view is missing or empty
SELECT
  'FAIL: View is empty or missing' AS test_result
WHERE
  (SELECT COUNT(*) FROM fact_monthly_summary) = 0;

-- Test 2: No NULL encounter months
-- Fails if any row has a NULL month (bad data or broken aggregation)
SELECT
  'FAIL: NULL encounter_month found' AS test_result,
  encounter_month,
  encounter_count,
  unique_patient_count
FROM
  fact_monthly_summary
WHERE
  encounter_month IS NULL;

-- Test 3: All counts must be positive
-- Fails if any month has zero or negative counts
SELECT
  'FAIL: Non-positive counts found' AS test_result,
  encounter_month,
  encounter_count,
  unique_patient_count
FROM
  fact_monthly_summary
WHERE
  encounter_count <= 0
  OR unique_patient_count <= 0;

-- Test 4: Unique patients must not exceed encounter count
-- Fails if a month somehow has more unique patients than encounters
SELECT
  'FAIL: Unique patients exceeds encounter count' AS test_result,
  encounter_month,
  encounter_count,
  unique_patient_count
FROM
  fact_monthly_summary
WHERE
  unique_patient_count > encounter_count;
