-- =============================================================================
-- Query 1: Normalized Traceability
-- Purpose: Show which material lots were consumed by each batch,
--          demonstrating joins across the normalized 3NF model.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_3NF;

SELECT
    b.batch_id,
    b.production_order_id,
    p.product_name,
    f.formulation_version,
    ml.material_name,
    ml.supplier_name,
    ml.lot_number,
    bmc.quantity_used
FROM BATCH b
JOIN FORMULATION f          ON b.formulation_id = f.formulation_id
JOIN PRODUCT p              ON f.product_id     = p.product_id
JOIN BATCH_MATERIAL_CONSUMPTION bmc ON b.batch_id = bmc.batch_id
JOIN MATERIAL_LOT ml        ON bmc.material_lot_id = ml.material_lot_id
ORDER BY b.batch_id, ml.material_name;
