# PHARMA_STAR — Dimensional Star Schema

## The Story

The QA director needs a dashboard. She wants to compare batch performance across facilities, track yield trends over time, and slice rejection rates by product line — all without writing five-table joins every time.

The data engineering team builds a **star schema** that pre-joins and pre-computes the metrics analysts ask for most. The 3NF transactional tables are reshaped into four dimensions and one fact table, with a convenience view that wires everything together.

### What Changes from 3NF

The normalized `PRODUCT` and `FORMULATION` tables collapse into a single **DIM_FORMULATION** lookup. Dates become a proper **DIM_DATE** dimension so the same calendar attributes (day of week, quarter, month name) can be joined four different ways — once for each date role on a batch. Pre-computed measures like `yield_pct`, `quality_score`, and `failure_count` land directly on the fact table so dashboards don't need to recalculate them on every refresh.

### B-10454 in the Star Schema

In `FACT_BATCH`, B-10454 is immediately visible as an outlier:

- **yield_pct** = 93.75 (vs. 98–100 for other batches)
- **quality_score** = 0.0 (formula: `100 - (300 × 0.5) - (2 × 5)` = –160, clamped to 0)
- **completion_hours** = 13.5 (vs. 7–12 typical)
- **failure_count** = 2

No joins required to spot the problem — that's the point of a star schema.

## Schema Design

```
                  DIM_DATE (×4 role-playing joins)
                     │
DIM_FORMULATION ── FACT_BATCH ── DIM_FACILITY
                     │
              DIM_PROCESS_VERSION
                     │
              VW_BATCH_STAR_JOIN (fully joined view)
```

### Objects

| Object | Type | Rows | Source |
|--------|------|------|--------|
| `DIM_DATE` | **Dynamic table** | 20 | Derived from all distinct dates in `PHARMA_3NF.BATCH` |
| `DIM_FORMULATION` | **Dynamic table** | 4 | Derived from `PHARMA_3NF.PRODUCT` + `PHARMA_3NF.FORMULATION` |
| `FACT_BATCH` | **Dynamic table** | 12 | Derived from `PHARMA_3NF.BATCH` + `PHARMA_3NF.TEST_RESULT` |
| `DIM_FACILITY` | Static table | 3 | Hand-maintained (no 3NF source) |
| `DIM_PROCESS_VERSION` | Static table | 3 | Hand-maintained (no 3NF source) |
| `VW_BATCH_STAR_JOIN` | View | 12 | Joins all 5 tables with role-playing date joins |

### Dynamic Tables

Three of the five tables are Snowflake **dynamic tables** that automatically refresh from the `PHARMA_3NF` source. If a test result is updated in the normalized schema, `FACT_BATCH` will recompute its aggregated metrics within the target lag window. `DIM_FACILITY` and `DIM_PROCESS_VERSION` remain static because their attributes (facility region, process designer, etc.) don't exist in the 3NF schema.

### Role-Playing Date Joins

The view `VW_BATCH_STAR_JOIN` joins `DIM_DATE` four times under different aliases:

| Alias | Fact Column | Meaning |
|-------|------------|---------|
| `d_order` | `order_date_id` | When the production order was placed |
| `d_start` | `start_date_id` | When manufacturing began |
| `d_end` | `end_date_id` | When manufacturing finished |
| `d_ship` | `shipping_complete_date_id` | When the batch shipped |

This is a classic dimensional modeling pattern — one dimension, multiple roles.

## Why Star Schema?

The star schema trades storage efficiency for **query simplicity**. A BI tool can generate a single `SELECT ... GROUP BY` against `VW_BATCH_STAR_JOIN` without understanding the underlying normalization. The tradeoff: denormalized data means updates to product names or formulation versions must propagate (handled here by dynamic tables). For read-heavy analytics workloads, that tradeoff is almost always worth it.
