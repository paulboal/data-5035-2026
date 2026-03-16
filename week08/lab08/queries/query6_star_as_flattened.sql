-- =============================================================================
-- Query 6: Same Information as Flattened, Sourced from Star Schema
-- Purpose: Reproduce the BATCH_FLATTENED output by joining FACT_BATCH with
--          dimension tables. Demonstrates that dimensional models can serve
--          the same queries as flattened tables.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_STAR;

SELECT
    f.batch_id,
    f.formulation_id,
    d.full_date             AS start_date,
    f.facility_id,
    fac.facility_site,
    fac.facility_region,
    fac.facility_type,
    fac.facility_age,
    pv.process_version,
    pv.process_type,
    pv.process_designer,
    f.completion_hours      AS time_to_complete
FROM FACT_BATCH f
JOIN DIM_FACILITY fac        ON f.facility_id        = fac.facility_id
JOIN DIM_PROCESS_VERSION pv  ON f.process_version_id = pv.process_version_id
JOIN DIM_DATE d              ON f.start_date_id      = d.date_id
ORDER BY f.batch_id;
