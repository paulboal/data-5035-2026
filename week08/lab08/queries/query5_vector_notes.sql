-- =============================================================================
-- Query 5: Vector Notes
-- Purpose: Select all review notes for batch B-10454.
-- =============================================================================

SELECT
    batch_id,
    note_type,
    note_text,
    vector_embedding
FROM DATA5035.PHARMA_AI.BATCH_REVIEW_NOTES
WHERE batch_id = 'B-10454'
ORDER BY note_type;
