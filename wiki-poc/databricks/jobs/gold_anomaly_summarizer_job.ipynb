# =============================================================================
# gold_anomaly_summarizer_job.py
# Databricks Workflow Job — NOT a DLT notebook
# Phase 6: Breaking news summarizer using Claude
#
# HOW TO DEPLOY
# ─────────────────────────────────────────────────────────────────────────────
# Run as a Databricks Workflow task, ideally as a downstream task in the same
# job as gold_anomaly_detection_job.py (runs after detection completes):
#   Workflows → your anomaly job → Add task → Notebook → this file
#   Cluster:    Serverless
#   Depends on: gold_anomaly_detection_job task
#
# PREREQUISITES
# ─────────────────────────────────────────────────────────────────────────────
# 1. Anthropic API key stored as a Databricks secret:
#      databricks secrets create-scope wiki_poc
#      databricks secrets put-secret wiki_poc anthropic_api_key
# 2. The anthropic SDK (installed via %pip below).
#
# OUTPUT
# ─────────────────────────────────────────────────────────────────────────────
# wiki_poc.poc.gold_anomaly_summaries — one row per summarized anomaly.
# Flags in gold_anomaly_flags are marked summarized = true once processed,
# so each anomaly is summarized exactly once.
# =============================================================================

# COMMAND ----------
# MAGIC %pip install anthropic
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime, timezone

from anthropic import Anthropic
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType, DoubleType,
)

CATALOG       = "wiki_poc"
SCHEMA        = "poc"
ANOMALY_TABLE = f"{CATALOG}.{SCHEMA}.gold_anomaly_flags"
SILVER_TABLE  = f"{CATALOG}.{SCHEMA}.silver_recentchange_enwiki"
SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.gold_anomaly_summaries"

MODEL              = "claude-haiku-4-5-20251001"  # fast + cheap for high-volume summarization
MAX_PAGES_PER_RUN  = 20   # cost control: only summarize the top N flags by z_score
MAX_COMMENTS       = 25   # cap edit summaries sent to Claude per page
MAX_TOKENS         = 300

SYSTEM_PROMPT = (
    "You are a news analyst monitoring Wikipedia edit activity. You are given "
    "metadata and edit summaries for a Wikipedia article that is being edited "
    "far more than usual. Your job is to explain, concisely and factually, what "
    "is most likely driving the surge. Distinguish between (a) a real-world event "
    "or breaking news, (b) an edit war or content dispute, (c) vandalism and "
    "reverts, and (d) routine maintenance. Do not speculate beyond what the edit "
    "summaries support. If the summaries are uninformative, say so plainly."
)

api_key = dbutils.secrets.get(scope="wiki_poc", key="anthropic_api_key")
client = Anthropic(api_key=api_key)


# =============================================================================
# Step 1 — Pull unsummarized flags, highest z_score first
# =============================================================================
flags = (
    spark.table(ANOMALY_TABLE)
    .filter(F.col("summarized") == False)
    .orderBy(F.col("z_score").desc())
    .limit(MAX_PAGES_PER_RUN)
    .collect()
)

if not flags:
    print("No unsummarized anomalies.")
    dbutils.notebook.exit("nothing_to_summarize")

print(f"Summarizing {len(flags)} anomalous page(s)...")


# =============================================================================
# Step 2 — For each flag, gather edit context from Silver and call Claude
# =============================================================================
def build_prompt(flag, comments_text: str) -> str:
    return (
        f"Page: {flag['title']}\n"
        f"Edits in this 5-minute window: {flag['edit_count']} "
        f"(typical average: {flag['baseline_mean']}, z-score: {flag['z_score']})\n"
        f"Unique editors: {flag['unique_editors']}\n"
        f"Net byte change: {flag['total_byte_delta']}\n\n"
        f"Edit summaries from this window:\n{comments_text}\n\n"
        f"In 2-3 sentences, explain what is most likely happening on this page "
        f"and why it is being edited so heavily."
    )


summaries = []
for flag in flags:
    comment_rows = (
        spark.table(SILVER_TABLE)
        .filter(
            (F.col("title") == flag["title"])
            & (F.col("event_timestamp") >= flag["window_start"])
            & (F.col("event_timestamp") < flag["window_end"])
            & F.col("comment").isNotNull()
            & (F.length("comment") > 0)
        )
        .select("comment", "byte_delta")
        .limit(MAX_COMMENTS)
        .collect()
    )

    if comment_rows:
        comments_text = "\n".join(
            f"- ({r['byte_delta']:+d} bytes) {r['comment']}"
            for r in comment_rows
            if r["comment"]
        )
    else:
        comments_text = "(no edit summaries were provided for these edits)"

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": build_prompt(flag, comments_text)}],
        )
        summary_text = "".join(
            block.text for block in response.content if block.type == "text"
        ).strip()
    except Exception as e:
        # Don't mark as summarized if the call failed — it'll be retried next run
        print(f"Claude call failed for '{flag['title']}': {e}")
        continue

    summaries.append({
        "title":         flag["title"],
        "window_start":  flag["window_start"],
        "z_score":       float(flag["z_score"]),
        "edit_count":    int(flag["edit_count"]),
        "summary":       summary_text,
        "model_used":    MODEL,
        "generated_at":  datetime.now(timezone.utc),
    })

if not summaries:
    print("No summaries produced (all Claude calls failed).")
    dbutils.notebook.exit("all_failed")


# =============================================================================
# Step 3 — Write summaries
# =============================================================================
summary_schema = StructType([
    StructField("title",        StringType()),
    StructField("window_start", TimestampType()),
    StructField("z_score",      DoubleType()),
    StructField("edit_count",   LongType()),
    StructField("summary",      StringType()),
    StructField("model_used",   StringType()),
    StructField("generated_at", TimestampType()),
])
summary_df = spark.createDataFrame(summaries, summary_schema)
summary_df.write.format("delta").mode("append").saveAsTable(SUMMARY_TABLE)


# =============================================================================
# Step 4 — Mark the corresponding flags as summarized
# =============================================================================
# MERGE on (title, window_start) flips summarized = true so each anomaly is
# summarized exactly once across runs.
summary_df.select("title", "window_start").createOrReplaceTempView("_just_summarized")
spark.sql(f"""
    MERGE INTO {ANOMALY_TABLE} AS a
    USING _just_summarized AS s
      ON a.title = s.title AND a.window_start = s.window_start
    WHEN MATCHED THEN UPDATE SET a.summarized = true
""")

print(f"Wrote {len(summaries)} summaries and marked flags as summarized.")
display(summary_df.orderBy(F.col("z_score").desc()))


# =============================================================================
# Validation queries
# =============================================================================
#
# -- Latest summaries
# SELECT title, z_score, edit_count, summary, generated_at
# FROM wiki_poc.poc.gold_anomaly_summaries
# ORDER BY generated_at DESC, z_score DESC
# LIMIT 20;
#
# -- Confirm no flags are stuck unsummarized for a long time
# SELECT COUNT(*) AS stuck
# FROM wiki_poc.poc.gold_anomaly_flags
# WHERE summarized = false AND detected_at < NOW() - INTERVAL 1 HOUR;
