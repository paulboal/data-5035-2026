-- =============================================================================
-- PHARMA_3NF.BATCH_MATERIAL_CONSUMPTION
-- Purpose: Many-to-many relationship between batches and material lots.
--          Tracks how much of each material lot was consumed per batch.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_3NF;

CREATE OR REPLACE TABLE BATCH_MATERIAL_CONSUMPTION (
    batch_material_consumption_id   NUMBER,
    batch_id                        STRING,
    material_lot_id                 NUMBER,
    quantity_used                   NUMBER
);

-- Tablet batches (B-10450, B-10451, B-10452, B-10458, B-10459) use lots 1, 2, 3
-- Sterile drops batches (B-10453, B-10454, B-10455, B-10460) use lots 4, 5
-- Ointment batches (B-10456, B-10457, B-10461) use lots 6, 7, 8
INSERT INTO BATCH_MATERIAL_CONSUMPTION (batch_material_consumption_id, batch_id, material_lot_id, quantity_used)
VALUES
    ( 1, 'B-10450', 1,  4900),
    ( 2, 'B-10450', 2,  4500),
    ( 3, 'B-10450', 3,   400),
    ( 4, 'B-10451', 1,  5000),
    ( 5, 'B-10451', 2,  4600),
    ( 6, 'B-10451', 3,   400),
    ( 7, 'B-10452', 1,  4950),
    ( 8, 'B-10452', 2,  4550),
    ( 9, 'B-10452', 3,   400),
    (10, 'B-10453', 4,  2500),
    (11, 'B-10453', 5,  2450),
    (12, 'B-10454', 4,  2400),
    (13, 'B-10454', 5,  2400),
    (14, 'B-10455', 4,  2500),
    (15, 'B-10455', 5,  2500),
    (16, 'B-10456', 6,  3200),
    (17, 'B-10456', 7,  4000),
    (18, 'B-10456', 8,   700),
    (19, 'B-10457', 6,  3200),
    (20, 'B-10457', 7,  4100),
    (21, 'B-10457', 8,   700),
    (22, 'B-10458', 1,  5000),
    (23, 'B-10458', 2,  4600),
    (24, 'B-10458', 3,   400),
    (25, 'B-10459', 1,  4975),
    (26, 'B-10459', 2,  4575),
    (27, 'B-10459', 3,   400),
    (28, 'B-10460', 4,  2500),
    (29, 'B-10460', 5,  2500),
    (30, 'B-10461', 6,  3175),
    (31, 'B-10461', 7,  4075),
    (32, 'B-10461', 8,   700);
