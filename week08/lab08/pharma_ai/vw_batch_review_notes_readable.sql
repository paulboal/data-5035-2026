-- =============================================================================
-- PHARMA_AI.VW_BATCH_REVIEW_NOTES_READABLE
-- Purpose: Simple view that returns batch notes without the vector column,
--          making it easy to read and query note text.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_AI;

CREATE OR REPLACE VIEW VW_BATCH_REVIEW_NOTES_READABLE AS
SELECT
    batch_id,
    note_type,
    note_text
FROM BATCH_REVIEW_NOTES;
