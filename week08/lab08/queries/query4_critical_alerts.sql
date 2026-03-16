-- =============================================================================
-- Query 4: Alert Table
-- Purpose: Show all critical alerts from the early warning table.
-- =============================================================================

SELECT
    batch_id,
    alert_timestamp,
    alert_severity,
    alert_message
FROM DATA5035.PHARMA_INT.BATCH_QUALITY_EARLY_WARNING
WHERE alert_severity = 'CRITICAL'
ORDER BY alert_timestamp;
