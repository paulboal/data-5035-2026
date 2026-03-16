-- =============================================================================
-- PHARMA_STAR.VW_BATCH_STAR_JOIN
-- Purpose: Fully joined star-schema view for BI analytics. Uses role-playing
--          joins to DIM_DATE for order, start, end, and shipping dates.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_STAR;

CREATE OR REPLACE VIEW VW_BATCH_STAR_JOIN AS
SELECT
    f.batch_id,
    df.product_name,
    df.dosage_form,
    df.formulation_version,
    fac.facility_site,
    fac.facility_region,
    fac.facility_type,
    pv.process_version,
    pv.process_type,
    pv.process_designer,
    d_order.full_date       AS order_date,
    d_start.full_date       AS start_date,
    d_end.full_date         AS end_date,
    d_ship.full_date        AS shipping_complete_date,
    f.planned_quantity,
    f.actual_quantity,
    f.good_quantity,
    f.rejected_quantity,
    f.yield_pct,
    f.quality_score,
    f.completion_hours,
    f.failure_count,
    f.total_tests,
    f.passed_tests
FROM FACT_BATCH f
JOIN DIM_FORMULATION df      ON f.formulation_id      = df.formulation_id
JOIN DIM_FACILITY fac        ON f.facility_id          = fac.facility_id
JOIN DIM_PROCESS_VERSION pv  ON f.process_version_id   = pv.process_version_id
JOIN DIM_DATE d_order        ON f.order_date_id        = d_order.date_id
JOIN DIM_DATE d_start        ON f.start_date_id        = d_start.date_id
JOIN DIM_DATE d_end          ON f.end_date_id          = d_end.date_id
JOIN DIM_DATE d_ship         ON f.shipping_complete_date_id = d_ship.date_id;
