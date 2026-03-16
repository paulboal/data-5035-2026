# PHARMA_ML — Flattened ML-Ready Table

## The Story

The data science team wants to build a model that predicts batch completion time. They don't want to write SQL joins — they want a single flat CSV-like table they can pull into a notebook with one query. Every row is a training example. Every column is a feature.

The data engineers create **BATCH_FLATTENED**: one row per batch, with facility characteristics, process metadata, and outcome metrics already denormalized. The data scientist's query is just `SELECT * FROM BATCH_FLATTENED`.

### What Changes from the Star Schema

The star schema already pre-computed the measures. This table goes one step further — it **eliminates all joins** by folding dimension attributes directly onto each row. There are no foreign keys, no surrogate keys, no dimension tables. Just a wide, flat table ready for `pandas.read_sql()` or Snowpark ML.

### B-10454 as a Training Example

In the flattened table, B-10454 is a single row with all the context a model needs:

| Feature | Value |
|---------|-------|
| `facility_site` | Columbus |
| `facility_type` | Sterile Fill |
| `facility_age` | 8 years |
| `process_version` | PV-Sterile-2.1 |
| `time_to_complete` | 13.5 hours |
| `quality_score` | 0.0 |
| `yield_pct` | 93.75 |

A model trained on this data would learn that Columbus + Sterile Fill + long completion time correlates with poor quality outcomes.

## Schema Design

```
PHARMA_STAR.FACT_BATCH ──┐
PHARMA_STAR.DIM_DATE ────┤
PHARMA_STAR.DIM_FACILITY ┼──► BATCH_FLATTENED (dynamic table)
PHARMA_STAR.DIM_PROCESS_VERSION
```

### Objects

| Object | Type | Rows | Source |
|--------|------|------|--------|
| `BATCH_FLATTENED` | **Dynamic table** | 12 | Joins FACT_BATCH + DIM_DATE + DIM_FACILITY + DIM_PROCESS_VERSION |

This is a Snowflake dynamic table that refreshes automatically when the upstream star schema changes. The full pipeline is: `PHARMA_3NF` → `PHARMA_STAR` → `PHARMA_ML` — three layers of dynamic tables.

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `batch_id` | STRING | Batch identifier |
| `formulation_id` | NUMBER | Which recipe was used |
| `start_date` | DATE | When manufacturing began |
| `facility_id` | NUMBER | Facility numeric ID |
| `facility_site` | STRING | City name (St. Louis, Columbus, Raleigh) |
| `facility_region` | STRING | Geographic region (Midwest, Southeast) |
| `facility_type` | STRING | Line type (Solid Dose, Sterile Fill, Ointment Line) |
| `facility_age` | NUMBER | Years since facility commissioning |
| `process_version` | STRING | Manufacturing process version code |
| `process_type` | STRING | Process category |
| `process_designer` | STRING | Team that designed the process |
| `time_to_complete` | FLOAT | Hours from start to end of manufacturing |
| `quality_score` | FLOAT | Composite quality metric (0–100) |
| `yield_pct` | FLOAT | Good units / actual units × 100 |

## Why Flatten?

ML frameworks expect **rectangular data** — no joins, no nulls from missing dimension rows, no surrogate key lookups. The flattened table is the interface between the data warehouse and the data science workflow. It's intentionally redundant (facility_site appears on every row for that facility) because storage is cheap and feature engineering time is expensive.
