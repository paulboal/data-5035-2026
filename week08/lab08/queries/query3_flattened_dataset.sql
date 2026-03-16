-- =============================================================================
-- Query 3: Flattened Dataset
-- Purpose: Select all columns from the ML-ready flattened table.
-- =============================================================================

SELECT *
FROM DATA5035.PHARMA_ML.BATCH_FLATTENED
ORDER BY batch_id;
