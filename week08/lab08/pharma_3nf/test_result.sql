-- =============================================================================
-- PHARMA_3NF.TEST_RESULT
-- Purpose: Quality control test results for each batch. Each batch has
--          2-4 tests (assay, dissolution, sterility, fill volume, viscosity).
--          pass_fail_flag = 'PASS' or 'FAIL'.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_3NF;

CREATE OR REPLACE TABLE TEST_RESULT (
    test_result_id  NUMBER,
    batch_id        STRING,
    test_method     STRING,
    result_value    FLOAT,
    result_unit     STRING,
    pass_fail_flag  STRING
);

-- 2-4 test results per batch (34 total)
-- Tablet batches: assay + dissolution
-- Sterile drops batches: sterility + fill volume (+ assay for some)
-- Ointment batches: viscosity + assay (+ sterility for some)
INSERT INTO TEST_RESULT (test_result_id, batch_id, test_method, result_value, result_unit, pass_fail_flag)
VALUES
    -- B-10450: tablet, 2 tests, all pass
    ( 1, 'B-10450', 'Assay',        99.2,  '%',    'PASS'),
    ( 2, 'B-10450', 'Dissolution',   92.0,  '%',    'PASS'),

    -- B-10451: tablet, 2 tests, all pass
    ( 3, 'B-10451', 'Assay',       100.1,  '%',    'PASS'),
    ( 4, 'B-10451', 'Dissolution',   95.5,  '%',    'PASS'),

    -- B-10452: tablet, 3 tests, all pass
    ( 5, 'B-10452', 'Assay',        98.8,  '%',    'PASS'),
    ( 6, 'B-10452', 'Dissolution',   91.3,  '%',    'PASS'),
    ( 7, 'B-10452', 'Content Uniformity', 99.0, '%','PASS'),

    -- B-10453: sterile drops, 3 tests, all pass
    ( 8, 'B-10453', 'Sterility',      0.0,  'CFU/mL', 'PASS'),
    ( 9, 'B-10453', 'Fill Volume',    1.02, 'mL',     'PASS'),
    (10, 'B-10453', 'Assay',         99.5,  '%',      'PASS'),

    -- B-10454: sterile drops, 4 tests, 2 FAIL (this is the problem batch)
    (11, 'B-10454', 'Sterility',      0.0,  'CFU/mL', 'PASS'),
    (12, 'B-10454', 'Fill Volume',    0.80, 'mL',     'FAIL'),
    (13, 'B-10454', 'Assay',         94.2,  '%',      'FAIL'),
    (14, 'B-10454', 'Particulate',    52.0, 'particles/mL', 'PASS'),

    -- B-10455: sterile drops, 3 tests, all pass
    (15, 'B-10455', 'Sterility',      0.0,  'CFU/mL', 'PASS'),
    (16, 'B-10455', 'Fill Volume',    1.01, 'mL',     'PASS'),
    (17, 'B-10455', 'Assay',        100.0,  '%',      'PASS'),

    -- B-10456: ointment, 2 tests, all pass
    (18, 'B-10456', 'Viscosity',   4520.0,  'cP',   'PASS'),
    (19, 'B-10456', 'Assay',        99.7,   '%',    'PASS'),

    -- B-10457: ointment, 3 tests, all pass
    (20, 'B-10457', 'Viscosity',   4480.0,  'cP',   'PASS'),
    (21, 'B-10457', 'Assay',       100.3,   '%',    'PASS'),
    (22, 'B-10457', 'Sterility',      0.0,  'CFU/g','PASS'),

    -- B-10458: tablet, 2 tests, all pass (perfect batch)
    (23, 'B-10458', 'Assay',       100.0,   '%',    'PASS'),
    (24, 'B-10458', 'Dissolution',   96.1,  '%',    'PASS'),

    -- B-10459: tablet, 3 tests, 1 fail
    (25, 'B-10459', 'Assay',        99.4,   '%',    'PASS'),
    (26, 'B-10459', 'Dissolution',   88.0,  '%',    'FAIL'),
    (27, 'B-10459', 'Content Uniformity', 98.5, '%','PASS'),

    -- B-10460: sterile drops, 2 tests, all pass
    (28, 'B-10460', 'Sterility',      0.0,  'CFU/mL', 'PASS'),
    (29, 'B-10460', 'Fill Volume',    1.00, 'mL',     'PASS'),

    -- B-10461: ointment, 3 tests, all pass
    (30, 'B-10461', 'Viscosity',   4550.0,  'cP',   'PASS'),
    (31, 'B-10461', 'Assay',        99.9,   '%',    'PASS'),
    (32, 'B-10461', 'Sterility',      0.0,  'CFU/g','PASS');
