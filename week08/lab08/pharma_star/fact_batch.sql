-- =============================================================================
-- PHARMA_STAR.FACT_BATCH
-- Purpose: Fact table at the grain of one row per manufacturing batch.
--          Contains measures derived from the normalized model:
--            yield_pct       = good_quantity / actual_quantity * 100
--            completion_hours = hours between start and end timestamps
--            failure_count   = count of failed test results
--            total_tests     = count of all test results
--            passed_tests    = count of passed test results
--            quality_score   = 100 - (rejected_quantity * 0.5) - (failure_count * 5)
--                              clamped to 0-100
--          Dynamic table derived from PHARMA_3NF.BATCH + PHARMA_3NF.TEST_RESULT.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_STAR;

CREATE OR REPLACE DYNAMIC TABLE FACT_BATCH
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
AS
SELECT
    b.batch_id,
    b.production_order_id,
    b.formulation_id,
    b.process_version_id,
    b.facility_id,
    TO_NUMBER(TO_CHAR(b.order_date, 'YYYYMMDD'))              AS order_date_id,
    TO_NUMBER(TO_CHAR(b.start_timestamp::DATE, 'YYYYMMDD'))   AS start_date_id,
    TO_NUMBER(TO_CHAR(b.end_timestamp::DATE, 'YYYYMMDD'))     AS end_date_id,
    TO_NUMBER(TO_CHAR(b.shipping_complete_date, 'YYYYMMDD'))  AS shipping_complete_date_id,
    b.planned_quantity,
    b.actual_quantity,
    b.good_quantity,
    b.rejected_quantity,
    ROUND(b.good_quantity / NULLIF(b.actual_quantity, 0) * 100, 2) AS yield_pct,
    GREATEST(0, LEAST(100,
        100 - (b.rejected_quantity * 0.5)
            - (SUM(CASE WHEN tr.pass_fail_flag = 'FAIL' THEN 1 ELSE 0 END) * 5)
    )) AS quality_score,
    ROUND(TIMEDIFF('MINUTE', b.start_timestamp, b.end_timestamp) / 60.0, 1) AS completion_hours,
    SUM(CASE WHEN tr.pass_fail_flag = 'FAIL' THEN 1 ELSE 0 END) AS failure_count,
    COUNT(tr.test_result_id) AS total_tests,
    SUM(CASE WHEN tr.pass_fail_flag = 'PASS' THEN 1 ELSE 0 END) AS passed_tests
FROM DATA5035.PHARMA_3NF.BATCH b
LEFT JOIN DATA5035.PHARMA_3NF.TEST_RESULT tr ON b.batch_id = tr.batch_id
GROUP BY
    b.batch_id, b.production_order_id, b.formulation_id, b.process_version_id,
    b.facility_id, b.order_date, b.start_timestamp, b.end_timestamp,
    b.shipping_complete_date, b.planned_quantity, b.actual_quantity,
    b.good_quantity, b.rejected_quantity;
