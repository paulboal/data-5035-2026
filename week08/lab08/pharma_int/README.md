# PHARMA_INT — Reverse ETL / Operational Alerting

## The Story

It's 7:23 AM on April 13, 2024. An automated quality risk model flags batch B-10454 with a **CRITICAL** alert: the fill volume check came back at 0.80 mL — well outside the 0.95–1.05 mL specification. Two hours later, a second alert fires: the assay result is 94.2%, below the 95.0% lower specification limit. The batch is placed on QA hold.

This is **reverse ETL** — instead of pulling data *into* the warehouse for analysis, the warehouse pushes structured alerts *out* to operational systems. The `BATCH_QUALITY_EARLY_WARNING` table is the staging area where alerts land before being routed to email, Slack, a ticketing system, or a plant floor display.

### The Alert Timeline

| Timestamp | Batch | Severity | What Happened |
|-----------|-------|----------|---------------|
| Apr 2, 3:00 PM | B-10450 | INFO | Yield slightly below plan (9,800 vs. 10,000) |
| Apr 13, 7:23 AM | **B-10454** | **CRITICAL** | Fill volume out of spec (0.80 mL) |
| Apr 13, 9:15 AM | **B-10454** | WARNING | Assay below lower limit (94.2%) |
| Apr 14, 4:45 PM | B-10459 | WARNING | Dissolution test failed (88.0%) |
| Apr 16, 7:10 PM | B-10460 | INFO | Minor reject count above baseline (30 units) |

### JSON Alert Payloads

Each alert carries a VARIANT column (`alert_message`) with structured JSON containing the reason, observed value, expected range, recommended action, and which model triggered the alert. This lets downstream consumers parse exactly what they need:

```sql
SELECT alert_message:reason::STRING, alert_message:recommended_action::STRING
FROM BATCH_QUALITY_EARLY_WARNING
WHERE alert_severity = 'CRITICAL';
```

### The Enriched View

Analysts don't just want to see the alert — they want context. `VW_BATCH_ALERTS_ENRICHED` performs a **cross-schema join** to `PHARMA_ML.BATCH_FLATTENED`, adding facility, process, and timing information alongside each alert. This is the view a plant manager would query to understand *where* and *why* an alert fired.

## Schema Design

```
BATCH_QUALITY_EARLY_WARNING ──┐
                               ├──► VW_BATCH_ALERTS_ENRICHED
PHARMA_ML.BATCH_FLATTENED ────┘    (cross-schema join)
```

### Objects

| Object | Type | Rows | Source |
|--------|------|------|--------|
| `BATCH_QUALITY_EARLY_WARNING` | Static table | 5 | Simulated alert data with JSON payloads |
| `VW_BATCH_ALERTS_ENRICHED` | View | 5 | Joins alerts to `PHARMA_ML.BATCH_FLATTENED` |

### Alert Message Schema

Every `alert_message` JSON document contains:

| Field | Example |
|-------|---------|
| `reason` | "Non-conforming in-process test result" |
| `test` | "fill_volume_check" |
| `observed_value` | 0.80 |
| `expected_range` | "0.95-1.05" |
| `recommended_action` | "Hold batch for QA review" |
| `triggered_by_model` | "batch_quality_risk_v1" |

## Why Reverse ETL?

Traditional ETL moves data from operational systems into a warehouse. Reverse ETL closes the loop — analytical insights flow *back* into operational systems as alerts, recommendations, or automated actions. In pharma manufacturing, a 2-hour delay in catching a fill volume problem can mean thousands of wasted units. The warehouse isn't just a reporting tool; it's part of the quality control feedback loop.
