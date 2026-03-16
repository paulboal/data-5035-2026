# PHARMA_AI — Vector-Enabled GenAI Retrieval

## The Story

A quality investigator is looking into what happened with B-10454. She doesn't know which table to query or what column to filter on. She types a natural language question into a search interface:

> *"quality failure fill volume problem"*

The system converts her question into a 1,024-dimensional vector using `snowflake-arctic-embed-l-v2.0`, then finds the most semantically similar batch review notes using cosine similarity. The top results:

| Rank | Batch | Note Type | Similarity |
|------|-------|-----------|-----------|
| 1 | B-10454 | batch_summary | 0.606 |
| 2 | B-10454 | quality_comment | 0.579 |
| 3 | B-10453 | operations_comment | 0.543 |

Without writing SQL filters or knowing that B-10454 is the problem batch, the investigator finds exactly the notes she needs. That's **semantic search** — matching on meaning, not keywords.

### The Notes

Quality engineers, operators, and QA reviewers write free-text notes after each batch. These notes capture institutional knowledge that doesn't fit neatly into structured columns:

| Batch | Note Type | Summary |
|-------|-----------|---------|
| B-10450 | batch_summary | Completed on schedule, slight yield loss |
| B-10451 | release_note | Released, all tests within spec |
| B-10453 | operations_comment | Sterile fill line ran smoothly |
| **B-10454** | **quality_comment** | **Pressure drop during fill, 0.80 mL out of spec** |
| **B-10454** | **batch_summary** | **Two failures, high rejects, rework required** |
| **B-10454** | **operations_comment** | **Delayed shipping, 9-day QA hold** |
| B-10455 | release_note | All tests passed, no deviations |
| B-10458 | batch_summary | Perfect batch, used as process validation reference |
| B-10459 | quality_comment | Dissolution failed at 88% |
| B-10461 | release_note | Ointment batch shipped on time |

B-10454 has three notes — more than any other batch — reflecting the depth of investigation it required.

## Schema Design

```
BATCH_REVIEW_NOTES ──► VW_BATCH_REVIEW_NOTES_READABLE
    (note_text + vector_embedding)       (note_text only, no vector)
```

### Objects

| Object | Type | Rows | Description |
|--------|------|------|-------------|
| `BATCH_REVIEW_NOTES` | Static table | 10 | Free-text notes with 1024-dim vector embeddings |
| `VW_BATCH_REVIEW_NOTES_READABLE` | View | 10 | Human-readable view (excludes the vector column) |

### How the Embeddings Work

1. Notes are inserted with `vector_embedding` as NULL
2. A separate UPDATE statement calls `SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', note_text)` to generate real embeddings
3. The `vector_embedding` column is `VECTOR(FLOAT, 1024)` — Snowflake's native vector type
4. Similarity search uses `VECTOR_COSINE_SIMILARITY()` to rank notes by semantic relevance

### Example: Semantic Search

```sql
SELECT batch_id, note_type, note_text,
       VECTOR_COSINE_SIMILARITY(
           vector_embedding,
           SNOWFLAKE.CORTEX.EMBED_TEXT_1024(
               'snowflake-arctic-embed-l-v2.0',
               'quality failure fill volume problem'
           )
       ) AS similarity
FROM PHARMA_AI.BATCH_REVIEW_NOTES
ORDER BY similarity DESC
LIMIT 3;
```

## Why Vector Embeddings?

Structured data can answer "which batches failed?" — that's a `WHERE pass_fail_flag = 'FAIL'` filter. But it can't answer "what went wrong and what did the team do about it?" Those answers live in free-text notes, investigation reports, and operator comments. Vector embeddings make unstructured text **queryable by meaning**, bridging the gap between what's in the database and what's in people's heads. In a GenAI pipeline, these embeddings are the retrieval layer — the "R" in RAG (Retrieval-Augmented Generation).
