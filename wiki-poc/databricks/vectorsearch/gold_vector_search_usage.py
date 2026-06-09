# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# =============================================================================
# gold_vector_search_usage.py
# Query patterns for the anomaly-summaries vector index (Phase 7)
#
# Three use cases, all against the same index:
#   1. Semantic search over past events
#   2. Related-event grouping (collapse pages spiking on the same real event)
#   3. Novelty detection (new breaking news vs ongoing/known event)
#
# Run gold_vector_search_setup.py first.
# =============================================================================

from pyspark.sql import functions as F
from databricks.vector_search.client import VectorSearchClient

CATALOG       = "wiki_poc"
SCHEMA        = "poc"
SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.gold_anomaly_summaries"
ENDPOINT_NAME = "wiki_poc_vs"
INDEX_NAME    = f"{CATALOG}.{SCHEMA}.gold_anomaly_summaries_index"
RETURN_COLS   = ["id", "title", "z_score", "summary", "generated_at"]

vsc = VectorSearchClient()
index = vsc.get_index(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME)


def search(query_text, num_results=10, columns=None):
    """Run a similarity search and return a list of dict rows (score included)."""
    columns = columns or RETURN_COLS
    resp = index.similarity_search(
        query_text=query_text,
        columns=columns,
        num_results=num_results,
    )
    col_names = [c["name"] for c in resp["manifest"]["columns"]]
    rows = resp["result"].get("data_array", []) or []
    return [dict(zip(col_names, r)) for r in rows]
    # NOTE: the trailing column is a similarity score (higher = more similar).
    # Its scale depends on the embedding model — calibrate thresholds empirically
    # (see use case 3) rather than assuming a fixed range.


# =============================================================================
# Use case 1 — Semantic search over past events
# =============================================================================
# Matches on meaning, not keywords: "unrest" finds protests/riots even if those
# exact words never appear in the summary.
print("=== Semantic search: 'election results being updated' ===")
for hit in search("election results being updated", num_results=8):
    score = list(hit.values())[-1]
    print(f"[{score:.3f}] {hit['title']} (z={hit['z_score']})")
    print(f"        {hit['summary'][:160]}...")


# =============================================================================
# Use case 2 — Related-event grouping
# =============================================================================
# When one real event drives many pages (e.g. a disaster spikes the event page
# plus affected cities and casualty pages), their summaries are semantically
# close. For a given anomaly, pull its neighbors to reconstruct the whole event.
print("\n=== Related events for the current top anomaly ===")
top = (
    spark.table(SUMMARY_TABLE)
    .orderBy(F.desc("generated_at"), F.desc("z_score"))
    .limit(1)
    .collect()
)
if top:
    seed = top[0]
    print(f"Seed: {seed['title']}  (z={seed['z_score']})")
    related = [h for h in search(seed["summary"], num_results=6)
               if h["id"] != seed["id"]]
    for h in related:
        score = list(h.values())[-1]
        print(f"  [{score:.3f}] {h['title']}")
else:
    print("No summaries yet.")


# =============================================================================
# Use case 3 — Novelty detection
# =============================================================================
# When a fresh anomaly fires, compare its summary to existing ones. A close
# neighbor means it's an ongoing/known event; no close neighbor means it's
# genuinely new and worth a priority flag.
def novelty(summary_text, exclude_id=None):
    """Return (is_novel, top_neighbor_score, top_neighbor_title)."""
    hits = search(summary_text, num_results=3)
    hits = [h for h in hits if h["id"] != exclude_id]
    if not hits:
        return True, None, None
    top = hits[0]
    score = list(top.values())[-1]
    # Threshold is a starting point — tune against a few known pairs first.
    NOVELTY_THRESHOLD = 0.75
    return (score < NOVELTY_THRESHOLD), score, top["title"]

print("\n=== Novelty check on the most recent anomaly ===")
recent = (
    spark.table(SUMMARY_TABLE)
    .orderBy(F.desc("generated_at"))
    .limit(1)
    .collect()
)
if recent:
    r = recent[0]
    is_new, score, neighbor = novelty(r["summary"], exclude_id=r["id"])
    label = "NOVEL (new event)" if is_new else f"known (similar to '{neighbor}')"
    print(f"{r['title']}: {label}  [top score={score}]")


# =============================================================================
# SQL alternative — query the index from a dashboard or SQL editor
# =============================================================================
# The vector_search() table function makes the index queryable in plain SQL,
# handy for an observability dashboard widget:
#
#   SELECT title, z_score, summary
#   FROM vector_search(
#     index => 'wiki_poc.poc.gold_anomaly_summaries_index',
#     query_text => 'protests and civil unrest',
#     num_results => 10
#   );
