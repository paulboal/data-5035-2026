-- =============================================================================
-- PHARMA_ML.BATCH_FLATTENED
-- Purpose: Denormalized wide table for ML / file-based serving.
--          One row per batch with all relevant features flattened from the
--          star schema. Dynamic table sourced from PHARMA_STAR.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_ML;

CREATE OR REPLACE DYNAMIC TABLE BATCH_FLATTENED
    TARGET_LAG = '1 hour'
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
AS
SELECT
    f.batch_id,
    f.formulation_id,
    d.full_date         AS start_date,
    f.facility_id,
    fac.facility_site,
    fac.facility_region,
    fac.facility_type,
    fac.facility_age,
    pv.process_version,
    pv.process_type,
    pv.process_designer,
    f.completion_hours  AS time_to_complete,
    f.quality_score,
    f.yield_pct
FROM DATA5035.PHARMA_STAR.FACT_BATCH f
JOIN DATA5035.PHARMA_STAR.DIM_DATE d            ON f.start_date_id     = d.date_id
JOIN DATA5035.PHARMA_STAR.DIM_FACILITY fac      ON f.facility_id       = fac.facility_id
JOIN DATA5035.PHARMA_STAR.DIM_PROCESS_VERSION pv ON f.process_version_id = pv.process_version_id;
