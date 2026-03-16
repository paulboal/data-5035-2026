-- =============================================================================
-- PHARMA_3NF.BATCH
-- Purpose: Each row represents one manufacturing batch — a single production
--          run of a formulation at a facility. Tracks quantities and timing.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_3NF;

CREATE OR REPLACE TABLE BATCH (
    batch_id                STRING,
    formulation_id          NUMBER,
    facility_id             NUMBER,
    process_version_id      NUMBER,
    production_order_id     STRING,
    order_date              DATE,
    start_timestamp         TIMESTAMP_NTZ,
    end_timestamp           TIMESTAMP_NTZ,
    shipping_complete_date  DATE,
    planned_quantity        NUMBER,
    actual_quantity         NUMBER,
    good_quantity           NUMBER,
    rejected_quantity       NUMBER,
    batch_status            STRING
);

-- 12 batches across 3 facilities, clustered around April 2024
-- facility_id: 1=St. Louis, 2=Columbus, 3=Raleigh
-- process_version_id: 1=PV-Tablet-5.3, 2=PV-Sterile-2.1, 3=PV-Ointment-1.4
INSERT INTO BATCH (batch_id, formulation_id, facility_id, process_version_id, production_order_id,
                   order_date, start_timestamp, end_timestamp, shipping_complete_date,
                   planned_quantity, actual_quantity, good_quantity, rejected_quantity, batch_status)
VALUES
    ('B-10450', 1, 1, 1, 'PO-9001', '2024-04-01', '2024-04-02 06:00:00', '2024-04-02 14:30:00', '2024-04-05', 10000,  9800,  9700,  100, 'Released'),
    ('B-10451', 1, 1, 1, 'PO-9002', '2024-04-02', '2024-04-03 06:00:00', '2024-04-03 15:00:00', '2024-04-06', 10000, 10000,  9950,   50, 'Released'),
    ('B-10452', 2, 1, 1, 'PO-9003', '2024-04-03', '2024-04-04 06:00:00', '2024-04-04 14:00:00', '2024-04-07', 10000,  9900,  9850,   50, 'Released'),
    ('B-10453', 3, 2, 2, 'PO-9004', '2024-04-04', '2024-04-05 07:00:00', '2024-04-05 19:00:00', '2024-04-09', 5000,   4950,  4900,   50, 'Released'),
    ('B-10454', 3, 2, 2, 'PO-9005', '2024-04-05', '2024-04-06 07:00:00', '2024-04-06 20:30:00', '2024-04-15', 5000,   4800,  4500,  300, 'Released'),
    ('B-10455', 3, 2, 2, 'PO-9006', '2024-04-08', '2024-04-09 07:00:00', '2024-04-09 18:00:00', '2024-04-12', 5000,   5000,  4980,   20, 'Released'),
    ('B-10456', 4, 3, 3, 'PO-9007', '2024-04-08', '2024-04-10 08:00:00', '2024-04-10 16:00:00', '2024-04-13', 8000,   7900,  7850,   50, 'Released'),
    ('B-10457', 4, 3, 3, 'PO-9008', '2024-04-09', '2024-04-11 08:00:00', '2024-04-11 17:30:00', '2024-04-14', 8000,   8000,  7950,   50, 'Released'),
    ('B-10458', 1, 1, 1, 'PO-9009', '2024-04-10', '2024-04-12 06:00:00', '2024-04-12 13:00:00', '2024-04-15', 10000, 10000, 10000,    0, 'Released'),
    ('B-10459', 2, 1, 1, 'PO-9010', '2024-04-12', '2024-04-14 06:00:00', '2024-04-14 15:30:00', '2024-04-17', 10000,  9950,  9900,   50, 'Released'),
    ('B-10460', 3, 2, 2, 'PO-9011', '2024-04-15', '2024-04-16 07:00:00', '2024-04-16 18:30:00', '2024-04-19', 5000,   5000,  4970,   30, 'Released'),
    ('B-10461', 4, 3, 3, 'PO-9012', '2024-04-16', '2024-04-18 08:00:00', '2024-04-18 15:00:00', '2024-04-21', 8000,   7950,  7900,   50, 'Released');
