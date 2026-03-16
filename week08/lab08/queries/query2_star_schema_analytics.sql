-- =============================================================================
-- Query 2: Star-Schema Analytics
-- Purpose: Aggregate batch performance by facility_region and process_type
--          using the star schema view.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_STAR;

SELECT
    v.facility_region,
    v.process_type,
    COUNT(*)                        AS batch_count,
    ROUND(AVG(v.yield_pct), 2)     AS avg_yield_pct,
    ROUND(AVG(v.quality_score), 2) AS avg_quality_score,
    ROUND(AVG(v.completion_hours), 2) AS avg_completion_hours
FROM VW_BATCH_STAR_JOIN v
GROUP BY v.facility_region, v.process_type
ORDER BY v.facility_region, v.process_type;
