-- =============================================================================
-- PHARMA_3NF.MATERIAL_LOT
-- Purpose: Raw materials and ingredients received from suppliers. Each lot
--          has traceability back to the supplier and an expiration date.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_3NF;

CREATE OR REPLACE TABLE MATERIAL_LOT (
    material_lot_id     NUMBER,
    material_name       STRING,
    supplier_name       STRING,
    lot_number          STRING,
    expiration_date     DATE,
    quantity_received   NUMBER
);

-- 8 material lots from various suppliers
INSERT INTO MATERIAL_LOT (material_lot_id, material_name, supplier_name, lot_number, expiration_date, quantity_received)
VALUES
    (1, 'Cardiolex API',            'PharmaSource Inc.',    'LS-20240101', '2025-12-31', 50000),
    (2, 'Microcrystalline Cellulose','ExciPure Co.',        'LS-20240115', '2026-06-30', 80000),
    (3, 'Magnesium Stearate',       'ExciPure Co.',         'LS-20240120', '2026-03-31', 20000),
    (4, 'OptiClear API',            'BioActive Labs',       'LS-20240201', '2025-09-30', 15000),
    (5, 'Sterile Saline Solution',  'CleanFluid Corp.',     'LS-20240210', '2025-08-31', 30000),
    (6, 'DermaSmooth API',          'PharmaSource Inc.',    'LS-20240215', '2025-11-30', 25000),
    (7, 'White Petrolatum',         'PetroPharm Ltd.',      'LS-20240220', '2026-01-31', 40000),
    (8, 'Preservative Blend',       'ChemGuard Inc.',       'LS-20240225', '2025-10-31', 10000);
