# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# =============================================================================
# gold_vector_search_sync.py
# Databricks Workflow Job — NOT a DLT notebook
# Phase 7: Trigger an incremental sync of the anomaly-summaries vector index.
#
# HOW TO DEPLOY
# ─────────────────────────────────────────────────────────────────────────────
# Add as a downstream task after gold_anomaly_summarizer_job in the same
# Workflow, so each cycle is: detect → summarize → sync.
#   Workflows → your anomaly job → Add task → Notebook → this file
#   Cluster:    Serverless ; Depends on: gold_anomaly_summarizer_job task
#
# WHY A SEPARATE TASK
# ─────────────────────────────────────────────────────────────────────────────
# The index is a TRIGGERED Delta Sync index, so it only picks up new/changed
# rows when a sync runs. Change Data Feed makes each sync incremental (only the
# rows added since the last sync), so this is fast and cheap after the one-time
# initial build in gold_vector_search_setup.py. Keeping sync in its own task is
# also why the summarizer doesn't need the databricks-vectorsearch dependency.
#
# NOTE: the package was renamed databricks-vectorsearch → databricks-ai-search.
# The import below still works as a thin re-export; modernize the whole repo
# together if/when you switch.
# =============================================================================

from databricks.vector_search.client import VectorSearchClient

ENDPOINT_NAME = "wiki_poc_vs"
INDEX_NAME    = "wiki_poc.poc.gold_anomaly_summaries_index"

# disable_notice=True silences the dev-auth notice — fine for a scheduled job.
vsc = VectorSearchClient(disable_notice=True)

# If the index isn't set up yet, exit cleanly rather than failing the whole job.
try:
    index = vsc.get_index(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME)
except Exception as e:
    print(f"Could not get index {INDEX_NAME}: {e}")
    print("Run gold_vector_search_setup.py first to create the endpoint and index.")
    dbutils.notebook.exit("index_not_found")

# Kick off an incremental sync. This is asynchronous: it processes rows added
# since the last sync and returns; the embedding happens in the background.
index.sync()

status = index.describe().get("status", {})
print(f"Sync triggered for {INDEX_NAME}.")
print(f"  detailed_state:    {status.get('detailed_state')}")
print(f"  indexed_row_count: {status.get('indexed_row_count')}")
