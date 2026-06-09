# Databricks notebook source
# MAGIC %pip install anthropic

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

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
# 2. The anthropic SDK (installed via %pip in the first cell above).
# 3. Web search enabled for your org in the Claude Console (Settings → Privacy →
#    enable web search). Without it, the tool call returns an error.
#    Web search costs $10 / 1,000 searches + tokens; Claude only searches for
#    likely real-world events (not vandalism/maintenance), so cost stays bounded.
#
# OUTPUT
# ─────────────────────────────────────────────────────────────────────────────
# wiki_poc.poc.gold_anomaly_summaries — one row per summarized anomaly.
# Flags in gold_anomaly_flags are marked summarized = true once processed,
# so each anomaly is summarized exactly once.
# =============================================================================

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
MAX_TOKENS         = 700  # higher to allow for web search synthesis + summary

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
            tools=[WEB_SEARCH_TOOL],
            messages=[{"role": "user", "content": build_prompt(flag, comments_text)}],
        )
        # With web search the response has interleaved blocks (text, server_tool_use,
        # web_search_tool_result). Concatenate only the text blocks for the summary.
        summary_text = "".join(
            block.text for block in response.content if block.type == "text"
        ).strip()
        # Count searches actually performed (0 for vandalism/maintenance cases)
        searches_used = sum(
            1 for block in response.content if block.type == "server_tool_use"
        )
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
        "searches_used": int(searches_used),
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
