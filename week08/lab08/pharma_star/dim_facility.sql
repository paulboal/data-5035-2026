-- =============================================================================
-- PHARMA_STAR.DIM_FACILITY
-- Purpose: Manufacturing facility dimension with site, region, and type
--          attributes for slicing batch performance data.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_STAR;

CREATE OR REPLACE TABLE DIM_FACILITY (
    facility_id     NUMBER,
    facility_name   STRING,
    facility_site   STRING,
    facility_region STRING,
    facility_type   STRING,
    facility_age    NUMBER
);

-- 3 facilities across 2 regions
INSERT INTO DIM_FACILITY (facility_id, facility_name, facility_site, facility_region, facility_type, facility_age)
VALUES
    (1, 'STL Plant A',      'St. Louis', 'Midwest',   'Solid Dose',    15),
    (2, 'Columbus BioCenter','Columbus',  'Midwest',   'Sterile Fill',   8),
    (3, 'Raleigh South',    'Raleigh',   'Southeast', 'Ointment Line', 22);
