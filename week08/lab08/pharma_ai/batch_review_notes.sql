-- =============================================================================
-- PHARMA_AI.BATCH_REVIEW_NOTES
-- Purpose: Free-text notes about each batch with vector embeddings generated
--          by snowflake-arctic-embed-l-v2.0 for semantic similarity search.
--          VECTOR(FLOAT, 1024) stores the real embedding output.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_AI;

CREATE OR REPLACE TABLE BATCH_REVIEW_NOTES (
    batch_id            STRING,
    note_type           STRING,
    note_text           STRING,
    vector_embedding    VECTOR(FLOAT, 1024)
);

INSERT INTO BATCH_REVIEW_NOTES (batch_id, note_type, note_text)
SELECT 'B-10450', 'batch_summary', 'Batch completed on schedule. Yield slightly below plan due to startup losses. All QC tests passed.'
UNION ALL SELECT 'B-10451', 'release_note', 'Batch released to distribution. All tests passed with results well within specification.'
UNION ALL SELECT 'B-10453', 'operations_comment', 'Sterile fill line ran smoothly. Fill volume and sterility tests passed on first attempt.'
UNION ALL SELECT 'B-10454', 'quality_comment', 'Pressure drop observed during sterile fill. Fill volume out of spec at 0.80 mL. Batch held for QA review.'
UNION ALL SELECT 'B-10454', 'batch_summary', 'Problem batch. Two test failures (fill volume and assay). High reject count. Rework required before conditional release.'
UNION ALL SELECT 'B-10455', 'release_note', 'QA approved for release. All tests passed. Yield at target. No deviations noted.'
UNION ALL SELECT 'B-10458', 'batch_summary', 'Perfect batch — zero rejects, 100% yield, all tests passed. Used as reference for process validation.'
UNION ALL SELECT 'B-10459', 'quality_comment', 'Dissolution test failed at 88%. Granulation parameters under review. Yield otherwise acceptable.'
UNION ALL SELECT 'B-10454', 'operations_comment', 'Delayed shipping due to QA hold. Batch held 9 days past normal shipping window pending investigation.'
UNION ALL SELECT 'B-10461', 'release_note', 'Ointment batch released. Viscosity and assay within spec. Sterility confirmed. Shipped on time.';

UPDATE BATCH_REVIEW_NOTES
SET vector_embedding = SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', note_text)
WHERE vector_embedding IS NULL;
