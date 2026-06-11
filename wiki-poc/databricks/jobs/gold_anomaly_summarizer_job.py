# Databricks notebook source
# MAGIC %pip install anthropic

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# =============================================================================
# gold_anomaly_summarizer_job.py
# Databricks Workflow Job — NOT a DLT notebook
# Phase 6: Breaking news summarizer using Claude (Anthropic API) with web search
#
# CONTEXT SOURCES
# ─────────────────────────────────────────────────────────────────────────────
# 1. Edit summaries (comments) from Silver — why each edit was made.
# 2. Web search — external news context for genuine real-world events. Runs
#    server-side at Anthropic, so this compute only needs to reach
#    api.anthropic.com (no open-web egress required).
#
# HOW TO DEPLOY
# ─────────────────────────────────────────────────────────────────────────────
#   Workflows → your anomaly job → Add task → Notebook → this file
#   Cluster:    Serverless ; Depends on: gold_anomaly_detection_job task
#
# PREREQUISITES
# ─────────────────────────────────────────────────────────────────────────────
# 1. Anthropic API key secret:
#      databricks secrets put-secret wiki_poc anthropic_api_key
# 2. anthropic SDK (installed via %pip above).
# 3. Web search enabled for your org in the Claude Console (Settings → Privacy).
#
# ONE SUMMARY PER PAGE PER RUN
# ─────────────────────────────────────────────────────────────────────────────
# Pending flags are collapsed to one representative per page (peak-z window);
# all of a page's pending windows are marked summarized. Set COOLDOWN_MINS > 0
# to also skip pages summarized within that many minutes.
# =============================================================================

from datetime import datetime, timezone

from anthropic import Anthropic
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType, DoubleType,
)

CATALOG       = "wiki_poc"
SCHEMA        = "poc"
ANOMALY_TABLE = f"{CATALOG}.{SCHEMA}.gold_anomaly_flags"
SILVER_TABLE  = f"{CATALOG}.{SCHEMA}.silver_recentchange_enwiki"
SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.gold_anomaly_summaries"

MODEL              = "claude-haiku-4-5-20251001"
MAX_PAGES_PER_RUN  = 20
MAX_COMMENTS       = 25
MAX_TOKENS         = 700
COOLDOWN_MINS      = 0       # 0 = one summary per page per run

SYSTEM_PROMPT = (
    "You are a news analyst monitoring Wikipedia edit activity. You are given "
    "metadata and the edit summaries for a Wikipedia article that is being edited "
    "far more than usual. Explain, concisely and factually, what is most likely "
    "driving the surge.\n\n"
    "Classify the activity as one of: (a) a real-world event or breaking news, "
    "(b) an edit war or content dispute, (c) vandalism and reverts, or "
    "(d) routine maintenance.\n\n"
    "Use the web search tool ONLY when the edits point to a likely real-world "
    "event and you need current details to explain it; the edit summaries usually "
    "suffice. Never search for vandalism, edit wars, or routine maintenance.\n\n"
    "Write 2-4 sentences. Lead with what is actually happening in the real world "
    "when it's an event; otherwise explain the edit pattern plainly. Do not "
    "speculate beyond what the edit summaries and any search results support."
)

WEB_SEARCH_TOOL = {"type": "web_search_20250305", "name": "web_search", "max_uses": 3}

api_key = dbutils.secrets.get(scope="wiki_poc", key="anthropic_api_key")
client = Anthropic(api_key=api_key)


# =============================================================================
# Step 1 — Pull unsummarized flags, collapsed to one representative per page
# =============================================================================
pending = spark.table(ANOMALY_TABLE).filter(F.col("summarized") == False)

if COOLDOWN_MINS > 0 and spark.catalog.tableExists(SUMMARY_TABLE):
    recent_titles = (
        spark.table(SUMMARY_TABLE)
        .filter(F.col("generated_at")
                >= F.current_timestamp() - F.expr(f"INTERVAL {COOLDOWN_MINS} MINUTES"))
        .select("title").distinct()
    )
    pending = pending.join(recent_titles, on="title", how="left_anti")

_peak = Window.partitionBy("title").orderBy(
    F.col("z_score").desc(), F.col("window_start").desc()
)
flags = (
    pending
    .withColumn("_rn", F.row_number().over(_peak))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .orderBy(F.col("z_score").desc())
    .limit(MAX_PAGES_PER_RUN)
    .collect()
)

if not flags:
    print("No unsummarized anomalies.")
    dbutils.notebook.exit("nothing_to_summarize")

print(f"Summarizing {len(flags)} anomalous page(s)...")


# =============================================================================
# Step 2 — Gather edit comments, then call Claude (with web search)
# =============================================================================
def build_prompt(flag, comments_text):
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
    comments_text = "\n".join(
        f"- ({r['byte_delta']:+d} bytes) {r['comment']}"
        for r in comment_rows if r["comment"]
    ) or "(no edit summaries were provided for these edits)"

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            tools=[WEB_SEARCH_TOOL],
            messages=[{"role": "user", "content": build_prompt(flag, comments_text)}],
        )
        summary_text = "".join(
            b.text for b in response.content if b.type == "text"
        ).strip()
        searches_used = sum(
            1 for b in response.content if b.type == "server_tool_use"
        )
    except Exception as e:
        print(f"Claude call failed for '{flag['title']}': {e}")
        continue

    if not summary_text:
        print(f"Empty summary for '{flag['title']}', skipping.")
        continue

    summaries.append({
        "title":         flag["title"],
        "window_start":  flag["window_start"],
        "z_score":       float(flag["z_score"]),
        "edit_count":    int(flag["edit_count"]),
        "summary":       summary_text,
        "searches_used": int(searches_used),
        "model_used":    MODEL,
        "generated_at":  datetime.now(timezone.utc),
    })

if not summaries:
    print("No summaries produced (all Claude calls failed or returned empty).")
    dbutils.notebook.exit("all_failed")


# =============================================================================
# Step 3 — Write summaries (with md5 id as the vector-search primary key)
# =============================================================================
summary_schema = StructType([
    StructField("title",         StringType()),
    StructField("window_start",  TimestampType()),
    StructField("z_score",       DoubleType()),
    StructField("edit_count",    LongType()),
    StructField("summary",       StringType()),
    StructField("searches_used", LongType()),
    StructField("model_used",    StringType()),
    StructField("generated_at",  TimestampType()),
])
summary_df = spark.createDataFrame(summaries, summary_schema)
summary_df = summary_df.withColumn(
    "id",
    F.md5(F.concat_ws("|", F.col("title"), F.col("window_start").cast("string"))),
)
summary_df.write.format("delta").mode("append").saveAsTable(SUMMARY_TABLE)


# =============================================================================
# Step 4 — Mark ALL pending windows for the summarized pages
# =============================================================================
done_titles = [(s["title"],) for s in summaries]
spark.createDataFrame(done_titles, "title string").createOrReplaceTempView("_done_titles")
spark.sql(f"""
    MERGE INTO {ANOMALY_TABLE} AS a
    USING _done_titles AS d
      ON a.title = d.title
    WHEN MATCHED AND a.summarized = false THEN UPDATE SET a.summarized = true
""")

print(f"Wrote {len(summaries)} summaries and marked their pages summarized.")
display(summary_df.orderBy(F.col("z_score").desc()))


# =============================================================================
# Validation queries
# =============================================================================
#
# -- Latest summaries with search usage
# SELECT title, z_score, edit_count, searches_used, summary, generated_at
# FROM wiki_poc.poc.gold_anomaly_summaries
# ORDER BY generated_at DESC, z_score DESC
# LIMIT 20;
#
# -- Web search usage rate (cost tracking: $10 / 1,000 searches)
# SELECT
#   SUM(searches_used)            AS total_searches,
#   COUNT(*)                      AS pages_summarized,
#   ROUND(AVG(searches_used), 2)  AS avg_searches_per_page,
#   COUNT_IF(searches_used > 0)   AS pages_that_searched
# FROM wiki_poc.poc.gold_anomaly_summaries
# WHERE generated_at >= CURRENT_DATE();
#
# -- Confirm no pages are stuck unsummarized
# SELECT COUNT(*) AS stuck
# FROM wiki_poc.poc.gold_anomaly_flags
# WHERE summarized = false AND detected_at < NOW() - INTERVAL 1 HOUR;
