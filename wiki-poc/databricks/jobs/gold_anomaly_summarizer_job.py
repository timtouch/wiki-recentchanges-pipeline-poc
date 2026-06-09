# Databricks notebook source
# MAGIC %pip install anthropic

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# =============================================================================
# gold_anomaly_summarizer_job.py
# Databricks Workflow Job — NOT a DLT notebook
# Phase 6: Breaking news summarizer using Claude (Anthropic API)
#
# HOW TO DEPLOY
# ─────────────────────────────────────────────────────────────────────────────
# Run as a Databricks Workflow task, ideally downstream of
# gold_anomaly_detection_job.py in the same job:
#   Workflows → your anomaly job → Add task → Notebook → this file
#   Cluster:    Serverless
#   Depends on: gold_anomaly_detection_job task
#
# PREREQUISITES
# ─────────────────────────────────────────────────────────────────────────────
# 1. Anthropic API key stored as a Databricks secret:
#      databricks secrets create-scope wiki_poc
#      databricks secrets put-secret wiki_poc anthropic_api_key
# 2. The anthropic SDK (installed via %pip in the first cell above).
# 3. Web search enabled for your org in the Claude Console (Settings → Privacy).
#    Web search costs $10 / 1,000 searches + tokens; Claude only searches for
#    likely real-world events (not vandalism/maintenance), so cost stays bounded.
#
# ONE SUMMARY PER PAGE PER RUN
# ─────────────────────────────────────────────────────────────────────────────
# A sustained surge produces a separate anomaly flag for every 5-minute window.
# Rather than summarize each window, this job collapses pending flags to ONE
# representative per page (the peak-z window), summarizes that, and marks ALL of
# the page's pending windows summarized. Set COOLDOWN_MINS > 0 to also suppress
# re-summarizing a page that already has a summary within that many minutes.
#
# OUTPUT
# ─────────────────────────────────────────────────────────────────────────────
# wiki_poc.poc.gold_anomaly_summaries — one row per summarized page per run.
# Schema: title, window_start, z_score, edit_count, summary, searches_used,
#         model_used, generated_at, id (md5 PK for vector search).
# If your table currently has the `context_source` column (from the Databricks
# model version), drop it once so it recreates with this schema:
#   DROP TABLE IF EXISTS wiki_poc.poc.gold_anomaly_summaries;
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

MODEL              = "claude-haiku-4-5-20251001"  # fast + cheap for high-volume summarization
MAX_PAGES_PER_RUN  = 20   # cost control: only summarize the top N pages by z_score
MAX_COMMENTS       = 25   # cap edit summaries sent to Claude per page
MAX_TOKENS         = 700  # room for web search synthesis + summary
COOLDOWN_MINS      = 0    # 0 = one summary per page per run; >0 = skip pages
                          # already summarized within this many minutes

SYSTEM_PROMPT = (
    "You are a news analyst monitoring Wikipedia edit activity. You are given "
    "metadata and edit summaries for a Wikipedia article that is being edited "
    "far more than usual. Your job is to explain, concisely and factually, what "
    "is most likely driving the surge.\n\n"
    "First classify the activity as one of: (a) a real-world event or breaking "
    "news, (b) an edit war or content dispute, (c) vandalism and reverts, or "
    "(d) routine maintenance.\n\n"
    "If and only if the activity looks like a real-world event (a), use the web "
    "search tool to find current news about the topic, and incorporate the actual "
    "event details (what happened, when, who) into your summary. Do NOT search "
    "for vandalism, edit wars, or routine maintenance — the edit summaries already "
    "explain those, and searching wastes time and cost.\n\n"
    "Write 2-4 sentences. Lead with what is actually happening in the real world "
    "when it's an event; otherwise explain the edit pattern plainly. Do not "
    "speculate beyond what the edit summaries and any search results support."
)

WEB_SEARCH_TOOL = {
    "type": "web_search_20250305",
    "name": "web_search",
    "max_uses": 3,   # cap searches per page to control cost ($10 / 1,000 searches)
}

api_key = dbutils.secrets.get(scope="wiki_poc", key="anthropic_api_key")
client = Anthropic(api_key=api_key)


# =============================================================================
# Step 1 — Pull unsummarized flags, collapsed to one representative per page
# =============================================================================
pending = spark.table(ANOMALY_TABLE).filter(F.col("summarized") == False)

# Optional cooldown: drop pages already summarized within COOLDOWN_MINS.
if COOLDOWN_MINS > 0 and spark.catalog.tableExists(SUMMARY_TABLE):
    recent_titles = (
        spark.table(SUMMARY_TABLE)
        .filter(
            F.col("generated_at")
            >= F.current_timestamp() - F.expr(f"INTERVAL {COOLDOWN_MINS} MINUTES")
        )
        .select("title")
        .distinct()
    )
    pending = pending.join(recent_titles, on="title", how="left_anti")

# One representative per page: the highest-z window (ties broken by latest).
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
# Step 2 — For each page, gather edit context from Silver and call Claude
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
            tools=[WEB_SEARCH_TOOL],
            messages=[{"role": "user", "content": build_prompt(flag, comments_text)}],
        )
        # With web search the response has interleaved blocks (text, server_tool_use,
        # web_search_tool_result). Concatenate only the text blocks for the summary.
        summary_text = "".join(
            block.text for block in response.content if block.type == "text"
        ).strip()
        searches_used = sum(
            1 for block in response.content if block.type == "server_tool_use"
        )
    except Exception as e:
        # Don't mark as summarized if the call failed — it'll be retried next run
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

# Deterministic primary key — must match the backfill expression in
# gold_vector_search_setup.py so keys line up across the table and the index.
summary_df = summary_df.withColumn(
    "id",
    F.md5(F.concat_ws("|", F.col("title"), F.col("window_start").cast("string"))),
)

summary_df.write.format("delta").mode("append").saveAsTable(SUMMARY_TABLE)


# =============================================================================
# Step 4 — Mark ALL pending windows for the summarized pages
# =============================================================================
# We summarized one representative window per page, so mark every pending flag
# for those pages summarized — otherwise the page's other windows get picked up
# again next run.
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
# -- Latest summaries (one per page per run)
# SELECT title, z_score, edit_count, searches_used, summary, generated_at
# FROM wiki_poc.poc.gold_anomaly_summaries
# ORDER BY generated_at DESC, z_score DESC
# LIMIT 20;
#
# -- Confirm no page has runaway duplicate summaries in a short span
# SELECT title, COUNT(*) AS summary_rows
# FROM wiki_poc.poc.gold_anomaly_summaries
# WHERE generated_at >= NOW() - INTERVAL 1 HOUR
# GROUP BY title
# ORDER BY summary_rows DESC;
#
# -- Search usage rate (cost tracking)
# SELECT SUM(searches_used) AS total_searches, COUNT(*) AS pages,
#        ROUND(AVG(searches_used), 2) AS avg_per_page
# FROM wiki_poc.poc.gold_anomaly_summaries
# WHERE generated_at >= CURRENT_DATE();
