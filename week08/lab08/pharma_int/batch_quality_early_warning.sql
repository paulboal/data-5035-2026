-- =============================================================================
-- PHARMA_INT.BATCH_QUALITY_EARLY_WARNING
-- Purpose: Transactional alert table for reverse ETL / operational alerting.
--          Each row is a quality alert triggered by an automated model or rule.
--          alert_message is stored as VARIANT (valid JSON).
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_INT;

CREATE OR REPLACE TABLE BATCH_QUALITY_EARLY_WARNING (
    batch_id            STRING,
    alert_timestamp     TIMESTAMP_NTZ,
    alert_severity      STRING,
    alert_message       VARIANT
);

INSERT INTO BATCH_QUALITY_EARLY_WARNING
SELECT 'B-10454', '2024-04-13 07:23:47'::TIMESTAMP_NTZ, 'CRITICAL', PARSE_JSON('{"reason":"Non-conforming in-process test result","test":"fill_volume_check","observed_value":0.80,"expected_range":"0.95-1.05","recommended_action":"Hold batch for QA review","triggered_by_model":"batch_quality_risk_v1"}')
UNION ALL
SELECT 'B-10454', '2024-04-13 09:15:00'::TIMESTAMP_NTZ, 'WARNING', PARSE_JSON('{"reason":"Assay result below lower specification limit","test":"assay","observed_value":94.2,"expected_range":"95.0-105.0","recommended_action":"Initiate deviation investigation","triggered_by_model":"batch_quality_risk_v1"}')
UNION ALL
SELECT 'B-10459', '2024-04-14 16:45:00'::TIMESTAMP_NTZ, 'WARNING', PARSE_JSON('{"reason":"Dissolution test result below target","test":"dissolution","observed_value":88.0,"expected_range":"90.0-110.0","recommended_action":"Review granulation parameters","triggered_by_model":"batch_quality_risk_v1"}')
UNION ALL
SELECT 'B-10450', '2024-04-02 15:00:00'::TIMESTAMP_NTZ, 'INFO', PARSE_JSON('{"reason":"Yield slightly below planned quantity","test":"yield_check","observed_value":9800,"expected_range":"9900-10100","recommended_action":"Log deviation; no hold required","triggered_by_model":"yield_monitor_v2"}')
UNION ALL
SELECT 'B-10460', '2024-04-16 19:10:00'::TIMESTAMP_NTZ, 'INFO', PARSE_JSON('{"reason":"Minor rejected unit count above baseline","test":"reject_rate_check","observed_value":30,"expected_range":"0-20","recommended_action":"Monitor next batch closely","triggered_by_model":"yield_monitor_v2"}');
