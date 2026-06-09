# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# =============================================================================
# gold_vector_search_setup.py
# One-time setup — Databricks Vector Search over anomaly summaries
# Phase 7: vector index for event clustering, semantic search, novelty detection
#
# Run this notebook ONCE (interactively). After it completes, the index
# auto-embeds the `summary` column using a Databricks-hosted embedding model.
#
# PREREQUISITE — primary key on the source table
# ─────────────────────────────────────────────────────────────────────────────
# A Delta Sync index needs Change Data Feed + a single primary-key column on the
# source table. This notebook adds both to gold_anomaly_summaries. You must also
# add one line to the summarizer so NEW rows get an `id` (see end of notebook).
# =============================================================================

import time
from databricks.vector_search.client import VectorSearchClient

CATALOG            = "wiki_poc"
SCHEMA             = "poc"
SUMMARY_TABLE      = f"{CATALOG}.{SCHEMA}.gold_anomaly_summaries"
ENDPOINT_NAME      = "wiki_poc_vs"
INDEX_NAME         = f"{CATALOG}.{SCHEMA}.gold_anomaly_summaries_index"
EMBEDDING_ENDPOINT = "databricks-gte-large-en"   # Databricks-hosted, pay-per-token


# =============================================================================
# Step 1 — Prepare the source table: Change Data Feed + primary key
# =============================================================================
# CDF lets the Delta Sync index pick up new/changed rows incrementally.
spark.sql(f"ALTER TABLE {SUMMARY_TABLE} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")

# Add a deterministic primary key (md5 of the natural key) and backfill it.
# The SAME Spark expression must be used in the summarizer so new rows match —
# see the snippet at the bottom of this notebook.
existing_cols = [f.name for f in spark.table(SUMMARY_TABLE).schema.fields]
if "id" not in existing_cols:
    spark.sql(f"ALTER TABLE {SUMMARY_TABLE} ADD COLUMN id STRING")

spark.sql(f"""
    UPDATE {SUMMARY_TABLE}
    SET id = md5(concat_ws('|', title, cast(window_start AS string)))
    WHERE id IS NULL
""")
print("Source table prepared (CDF on, id backfilled).")


# =============================================================================
# Step 2 — Create the vector search endpoint (idempotent)
# =============================================================================
vsc = VectorSearchClient()

existing_endpoints = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]
if ENDPOINT_NAME not in existing_endpoints:
    vsc.create_endpoint(name=ENDPOINT_NAME, endpoint_type="STANDARD")
    print(f"Creating endpoint {ENDPOINT_NAME} (can take a few minutes)...")

# Wait for the endpoint to come online
while True:
    state = vsc.get_endpoint(ENDPOINT_NAME)["endpoint_status"]["state"]
    print(f"Endpoint state: {state}")
    if state == "ONLINE":
        break
    time.sleep(30)


# =============================================================================
# Step 3 — Create the Delta Sync index with Databricks-managed embeddings
# =============================================================================
# pipeline_type:
#   TRIGGERED  — sync on demand (call index.sync()); cheaper, fine for a POC
#   CONTINUOUS — auto-syncs within seconds; provisions a cluster, costs more
existing_indexes = [
    i["name"] for i in vsc.list_indexes(ENDPOINT_NAME).get("vector_indexes", [])
]
if INDEX_NAME not in existing_indexes:
    index = vsc.create_delta_sync_index(
        endpoint_name=ENDPOINT_NAME,
        source_table_name=SUMMARY_TABLE,
        index_name=INDEX_NAME,
        pipeline_type="TRIGGERED",
        primary_key="id",
        embedding_source_column="summary",
        embedding_model_endpoint_name=EMBEDDING_ENDPOINT,
    )
    print(f"Creating index {INDEX_NAME} (initial build takes several minutes)...")
else:
    index = vsc.get_index(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME)
    print("Index already exists.")

# Wait for the index to be ready
while not index.describe().get("status", {}).get("ready", False):
    print("Waiting for index to build...")
    time.sleep(30)

print("Index is ready.")
print(index.describe())


# =============================================================================
# ONE-LINE SUMMARIZER CHANGE (apply to whichever summarizer version you run)
# =============================================================================
# Right before `summary_df.write...saveAsTable(SUMMARY_TABLE)`, add the id column
# using the SAME expression as the backfill above so keys line up:
#
#   summary_df = summary_df.withColumn(
#       "id",
#       F.md5(F.concat_ws("|", F.col("title"), F.col("window_start").cast("string"))),
#   )
#
# With pipeline_type="TRIGGERED", also trigger a sync after the summarizer writes
# (add a final cell to the summarizer, or a downstream workflow task):
#
#   from databricks.vector_search.client import VectorSearchClient
#   VectorSearchClient().get_index(
#       endpoint_name="wiki_poc_vs",
#       index_name="wiki_poc.poc.gold_anomaly_summaries_index",
#   ).sync()
