-- =============================================================================
-- PHARMA_3NF.FORMULATION
-- Purpose: Each formulation is a specific recipe version for a product.
--          A product can have multiple formulation versions over time.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_3NF;

CREATE OR REPLACE TABLE FORMULATION (
    formulation_id      NUMBER,
    product_id          NUMBER,
    formulation_version STRING,
    target_batch_size   NUMBER,
    status              STRING
);

-- 4 formulations across 3 products (Cardiolex has 2 versions)
INSERT INTO FORMULATION (formulation_id, product_id, formulation_version, target_batch_size, status)
VALUES
    (1, 1, 'v2.1', 10000, 'Active'),
    (2, 1, 'v2.2', 10000, 'Active'),
    (3, 2, 'v1.0',  5000, 'Active'),
    (4, 3, 'v1.3',  8000, 'Active');
