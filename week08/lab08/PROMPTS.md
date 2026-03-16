# Pharma Manufacturing — Data Serving Patterns Demo

You are building a Snowflake teaching demo about the **Serving stage of the data engineering lifecycle**. The domain is **pharmaceutical batch manufacturing**. Create all SQL scripts, execute them, and verify the results.

## Context

A mid-sized pharma manufacturer runs three facilities (St. Louis, Columbus, Raleigh) producing three FDA-regulated products (Cardiolex tablets, OptiClear sterile drops, DermaSmooth ointment). In April 2024, they produce 12 batches (B-10450 through B-10461). **Batch B-10454** is a problem batch — high rejects, two QC test failures, delayed shipping — and must appear consistently across all five schemas.

## Database and File Structure

- **Database**: `DATA5035`
- **Schemas**: `PHARMA_3NF`, `PHARMA_STAR`, `PHARMA_ML`, `PHARMA_INT`, `PHARMA_AI`
- **Files**: One folder per schema, one `.sql` file per object, plus `setup.sql` and a `queries/` folder
- **Grants**: PUBLIC gets USAGE on all schemas and SELECT on all current and future tables/views

## Schema 1: PHARMA_3NF — Normalized Transactional Model

Create six tables in Third Normal Form with static INSERT data:

| Table | Rows | Key Details |
|-------|------|-------------|
| PRODUCT | 3 | Cardiolex (Tablet/50mg), OptiClear (Sterile Drops/0.5%), DermaSmooth (Ointment/1% w/w) |
| FORMULATION | 4 | Recipes per product; Cardiolex has v2.1 and v2.2 |
| BATCH | 12 | B-10450–B-10461, April 2024; facility_id (1=STL, 2=Columbus, 3=Raleigh); process_version_id (1=Tablet, 2=Sterile, 3=Ointment) |
| MATERIAL_LOT | 8 | Raw materials from 5 suppliers |
| BATCH_MATERIAL_CONSUMPTION | ~32 | Many-to-many: tablets use 3 materials, sterile uses 2, ointments use 3 |
| TEST_RESULT | ~32 | 2–4 QC tests per batch (assay, dissolution, sterility, fill volume, viscosity) |

**B-10454 requirements**: 300 rejected units, 2 FAIL tests (Fill Volume 0.80 mL, Assay 94.2%), shipped 10 days after order (vs. 3–4 day norm).

## Schema 2: PHARMA_STAR — Dimensional Star Schema

Use **Snowflake dynamic tables** where the data can be derived from PHARMA_3NF. Use static tables only when attributes don't exist in the source.

| Object | Type | Derivation |
|--------|------|------------|
| DIM_DATE | Dynamic table, `TARGET_LAG = '1 hour'` | UNION of all date columns from PHARMA_3NF.BATCH; surrogate key = YYYYMMDD integer; use DECODE for full day/month names |
| DIM_FORMULATION | Dynamic table, `TARGET_LAG = '1 hour'` | JOIN of PHARMA_3NF.PRODUCT + PHARMA_3NF.FORMULATION |
| FACT_BATCH | Dynamic table, `TARGET_LAG = DOWNSTREAM` | PHARMA_3NF.BATCH + aggregated PHARMA_3NF.TEST_RESULT |
| DIM_FACILITY | Static table | 3 facilities with name, site, region, type, age (no 3NF source) |
| DIM_PROCESS_VERSION | Static table | 3 process versions with type, designer, standard_cycle_hours (no 3NF source) |
| VW_BATCH_STAR_JOIN | View | Joins all 5 tables with **role-playing date joins** (order, start, end, ship) |

**FACT_BATCH computed measures**:
- `yield_pct` = good_quantity / actual_quantity × 100
- `completion_hours` = TIMEDIFF(MINUTE, start, end) / 60
- `failure_count` / `total_tests` / `passed_tests` = aggregated from TEST_RESULT
- `quality_score` = GREATEST(0, LEAST(100, 100 − (rejected_quantity × 0.5) − (failure_count × 5)))

## Schema 3: PHARMA_ML — Flattened ML Table

| Object | Type | Derivation |
|--------|------|------------|
| BATCH_FLATTENED | Dynamic table, `TARGET_LAG = '1 hour'` | SELECT from PHARMA_STAR: FACT_BATCH + DIM_DATE + DIM_FACILITY + DIM_PROCESS_VERSION |

Columns: batch_id, formulation_id, start_date, facility_id, facility_site, facility_region, facility_type, facility_age, process_version, process_type, process_designer, time_to_complete, quality_score, yield_pct.

## Schema 4: PHARMA_INT — Reverse ETL Alerting

| Object | Type | Details |
|--------|------|---------|
| BATCH_QUALITY_EARLY_WARNING | Static table | 5 alerts with VARIANT JSON payloads (reason, test, observed_value, expected_range, recommended_action, triggered_by_model) |
| VW_BATCH_ALERTS_ENRICHED | View | Cross-schema join to PHARMA_ML.BATCH_FLATTENED |

**Required alert**: B-10454, CRITICAL severity, timestamp '2024-04-13 07:23:47'. Note: use `SELECT...UNION ALL` with `PARSE_JSON()` — not `VALUES` clause (Snowflake doesn't allow PARSE_JSON inside VALUES).

## Schema 5: PHARMA_AI — Vector-Enabled GenAI Retrieval

| Object | Type | Details |
|--------|------|---------|
| BATCH_REVIEW_NOTES | Static table | 10 free-text notes (batch_summary, quality_comment, operations_comment, release_note); B-10454 gets 3 notes |
| VW_BATCH_REVIEW_NOTES_READABLE | View | Excludes vector column for human readability |

**Embedding approach**: Insert notes with `vector_embedding VECTOR(FLOAT, 1024)` as NULL, then UPDATE using `SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', note_text)`.

## Example Queries (queries/ folder)

1. **Normalized traceability**: Batch → material lot join through BATCH_MATERIAL_CONSUMPTION
2. **Star schema analytics**: Aggregate by facility_region and process_type (count, avg yield, avg quality_score, avg hours)
3. **Flattened dataset**: SELECT * from BATCH_FLATTENED
4. **Critical alerts**: Filter BATCH_QUALITY_EARLY_WARNING where severity = CRITICAL, extract JSON fields
5. **Vector notes**: All notes for B-10454
6. **Star as flattened**: Reproduce BATCH_FLATTENED output directly from star schema joins

## Execution Order

1. `setup.sql` (database, schemas, grants)
2. `pharma_3nf/` (all 6 tables — these are the source of truth)
3. `pharma_star/` (static dims first, then dynamic tables, then view)
4. `pharma_ml/` (dynamic table depending on PHARMA_STAR)
5. `pharma_int/` (static alert table, then cross-schema view)
6. `pharma_ai/` (notes table, UPDATE embeddings, then view)
7. `queries/` (validate all 6)

After each schema, verify data coherence: row counts, join integrity, and spot-check B-10454.

## Constraints

- Snowflake SQL only. Idempotent (CREATE OR REPLACE, CREATE IF NOT EXISTS).
- Small enough to inspect manually, large enough for realistic joins.
- Readable pharma-oriented values — no random noise.
- Comments in SQL for teaching clarity.
- No placeholders. Fully populated inserts. Syntactically valid.
- Each folder gets a README.md telling the story of that schema's data from a specific persona's perspective (QA director, data scientist, plant manager, quality investigator).
- Warehouse for dynamic tables: `SNOWFLAKE_LEARNING_WH`.
