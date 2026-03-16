-- =============================================================================
-- PHARMA_STAR.DIM_PROCESS_VERSION
-- Purpose: Manufacturing process versions used across facilities.
--          Each process version defines the steps and cycle time for a
--          particular dosage form.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_STAR;

CREATE OR REPLACE TABLE DIM_PROCESS_VERSION (
    process_version_id    NUMBER,
    process_version       STRING,
    process_type          STRING,
    process_designer      STRING,
    standard_cycle_hours  FLOAT,
    revision_status       STRING
);

-- 3 process versions: one per dosage form / facility type
INSERT INTO DIM_PROCESS_VERSION (process_version_id, process_version, process_type, process_designer, standard_cycle_hours, revision_status)
VALUES
    (1, 'PV-Tablet-5.3',  'Compression & Coating', 'Process Engineering Team A',     8.0, 'Current'),
    (2, 'PV-Sterile-2.1', 'Aseptic Fill-Finish',   'Sterile Ops Center',            12.0, 'Current'),
    (3, 'PV-Ointment-1.4','Mixing & Filling',       'External Process Transfer Team', 8.0, 'Current');
