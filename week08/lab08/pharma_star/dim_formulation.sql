-- =============================================================================
-- PHARMA_STAR.DIM_FORMULATION
-- Purpose: Denormalized formulation dimension — combines product and
--          formulation attributes into a single lookup table.
--          Dynamic table derived from PHARMA_3NF.PRODUCT + PHARMA_3NF.FORMULATION.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_STAR;

CREATE OR REPLACE DYNAMIC TABLE DIM_FORMULATION
    TARGET_LAG = '1 hour'
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
AS
SELECT
    f.formulation_id,
    p.product_id,
    p.product_name,
    p.dosage_form,
    p.strength,
    f.formulation_version
FROM DATA5035.PHARMA_3NF.FORMULATION f
JOIN DATA5035.PHARMA_3NF.PRODUCT p ON f.product_id = p.product_id;
