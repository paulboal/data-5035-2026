# PHARMA_3NF — Normalized Transactional Data

## The Story

It's April 2024 at a mid-sized pharmaceutical manufacturer operating three production facilities across the eastern United States. The company produces three FDA-regulated products:

| Product | Dosage Form | Facility |
|---------|------------|----------|
| **Cardiolex** (50 mg) | Tablet | St. Louis Plant A |
| **OptiClear** (0.5%) | Sterile Drops | Columbus BioCenter |
| **DermaSmooth** (1% w/w) | Ointment | Raleigh South |

Over the first three weeks of April, the facilities run **12 production batches** (B-10450 through B-10461). Most batches proceed normally — materials are consumed, quality tests pass, and shipments go out within 3-4 days of production.

### The Problem Batch: B-10454

Batch **B-10454** is an OptiClear sterile drops run at Columbus BioCenter on April 6. It stands out in every metric:

- **300 units rejected** (out of 4,800 produced) — 6x worse than a typical batch
- **2 failed QC tests**: Fill Volume (0.80 mL, below spec) and Assay (94.2%, below potency threshold)
- **10 days to ship** (April 5 order, April 15 ship) — versus the normal 3-4 day turnaround
- Consumed OptiClear API from **BioActive Labs** (lot LS-20240201) and Sterile Saline from **CleanFluid Corp.** (lot LS-20240210)

A second batch, **B-10459** (Cardiolex tablet at St. Louis), also has one failed Dissolution test (88.0%) but is otherwise unremarkable.

## Schema Design

This schema follows **Third Normal Form (3NF)** — no redundant data, every non-key attribute depends on the whole key and nothing but the key.

```
PRODUCT ──< FORMULATION ──< BATCH >── MATERIAL_LOT
                              │          (via BATCH_MATERIAL_CONSUMPTION)
                              │
                          TEST_RESULT
```

### Tables

| Table | Rows | Description |
|-------|------|-------------|
| `PRODUCT` | 3 | Master product catalog (Cardiolex, OptiClear, DermaSmooth) |
| `FORMULATION` | 4 | Recipe versions per product; Cardiolex has v2.1 and v2.2 |
| `BATCH` | 12 | Production runs with quantities, timestamps, and status |
| `MATERIAL_LOT` | 8 | Raw material lots from 5 suppliers |
| `BATCH_MATERIAL_CONSUMPTION` | 32 | Many-to-many: which lots went into which batches |
| `TEST_RESULT` | 32 | QC results (2-4 tests per batch); 3 total FAILs across all data |

### Key Relationships

- **PRODUCT → FORMULATION**: One product can have multiple formulation versions
- **FORMULATION → BATCH**: Each batch is produced from exactly one formulation
- **BATCH ↔ MATERIAL_LOT**: Many-to-many through `BATCH_MATERIAL_CONSUMPTION`; tablet batches use 3 materials, sterile drops use 2, ointments use 3
- **BATCH → TEST_RESULT**: Each batch has 2-4 quality control tests

### Suppliers

| Supplier | Materials Provided |
|----------|--------------------|
| PharmaSource Inc. | Cardiolex API, DermaSmooth API |
| ExciPure Co. | Microcrystalline Cellulose, Magnesium Stearate |
| BioActive Labs | OptiClear API |
| CleanFluid Corp. | Sterile Saline Solution |
| PetroPharm Ltd. | White Petrolatum |
| ChemGuard Inc. | Preservative Blend |

## Why 3NF?

This schema is designed for **transactional integrity**. If a supplier's name changes, you update one row in `MATERIAL_LOT` — not 32 rows in a flattened table. The tradeoff is that answering analytical questions (e.g., "which supplier's materials were in the failed batch?") requires multi-table joins. That tradeoff is what motivates the other schemas in this lab: `PHARMA_STAR`, `PHARMA_ML`, `PHARMA_INT`, and `PHARMA_AI`.
