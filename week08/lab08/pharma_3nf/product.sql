-- =============================================================================
-- PHARMA_3NF.PRODUCT
-- Purpose: Master list of pharmaceutical products manufactured at the company.
--          Each product has a unique NDC (National Drug Code).
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_3NF;

CREATE OR REPLACE TABLE PRODUCT (
    product_id      NUMBER,
    product_name    STRING,
    dosage_form     STRING,
    strength        STRING,
    ndc_code        STRING
);

-- 3 products covering tablet, sterile drops, and ointment dosage forms
INSERT INTO PRODUCT (product_id, product_name, dosage_form, strength, ndc_code)
VALUES
    (1, 'Cardiolex',    'Tablet',        '50 mg',   '12345-0101-01'),
    (2, 'OptiClear',    'Sterile Drops', '0.5%',    '12345-0202-01'),
    (3, 'DermaSmooth',  'Ointment',      '1% w/w',  '12345-0303-01');
