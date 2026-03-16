-- =============================================================================
-- PHARMA_INT.VW_BATCH_ALERTS_ENRICHED
-- Purpose: Joins alerts with the flattened batch table (cross-schema join
--          to PHARMA_ML) so analysts can see facility and process context
--          alongside each alert.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_INT;

CREATE OR REPLACE VIEW VW_BATCH_ALERTS_ENRICHED AS
SELECT
    w.batch_id,
    w.alert_timestamp,
    w.alert_severity,
    w.alert_message,
    bf.formulation_id,
    bf.start_date,
    bf.facility_site,
    bf.facility_region,
    bf.facility_type,
    bf.process_version,
    bf.process_type,
    bf.time_to_complete
FROM BATCH_QUALITY_EARLY_WARNING w
JOIN DATA5035.PHARMA_ML.BATCH_FLATTENED bf ON w.batch_id = bf.batch_id;
