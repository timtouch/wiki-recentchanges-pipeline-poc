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
# CONTEXT SOURCES (richest to leanest)
# ─────────────────────────────────────────────────────────────────────────────
# 1. Revision diff — the actual content added/changed on the page during the
#    anomaly window, pulled from the MediaWiki Revisions + Compare APIs. This is
#    the primary "what happened" signal: the literal text editors added.
# 2. Edit summaries (comments) from Silver — why each edit was made.
# 3. Web search — external news context for genuine real-world events.
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
# 4. Network egress from this compute to en.wikipedia.org for the revision
#    fetch. If blocked, the job degrades to comments + web search automatically.
#
# ONE SUMMARY PER PAGE PER RUN
# ─────────────────────────────────────────────────────────────────────────────
# Pending flags are collapsed to one representative per page (peak-z window);
# all of a page's pending windows are marked summarized. Set COOLDOWN_MINS > 0
# to also skip pages summarized within that many minutes.
# =============================================================================

import requests
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
MAX_DIFF_CHARS     = 2500    # cap diff text sent to Claude (token control)
REV_LIMIT          = 200     # max revisions to enumerate per window
WIKI_API           = "https://en.wikipedia.org/w/api.php"
WIKI_REST          = "https://en.wikipedia.org/w/rest.php/v1"
WIKI_UA            = "wiki-recentchanges-poc/1.0 (chaosbounder@gmail.com)"  # required by Wikimedia

SYSTEM_PROMPT = (
    "You are a news analyst monitoring Wikipedia edit activity. For a page being "
    "edited far more than usual, you are given: the actual content added or "
    "changed on the page during the window, the edit summaries, and edit "
    "metadata. Explain, concisely and factually, what is driving the surge.\n\n"
    "Use the added/changed CONTENT as your primary evidence — it is the literal "
    "text editors wrote. Classify the activity as one of: (a) a real-world event "
    "or breaking news, (b) an edit war or content dispute, (c) vandalism and "
    "reverts, or (d) routine maintenance.\n\n"
    "Use the web search tool ONLY when the content points to a real-world event "
    "you need to date or corroborate; the content and comments usually suffice. "
    "Never search for vandalism, edit wars, or maintenance.\n\n"
    "Write 2-4 sentences. Lead with what is actually happening. Do not speculate "
    "beyond what the content, comments, and any search results support."
)

WEB_SEARCH_TOOL = {"type": "web_search_20250305", "name": "web_search", "max_uses": 3}

api_key = dbutils.secrets.get(scope="wiki_poc", key="anthropic_api_key")
client = Anthropic(api_key=api_key)


# =============================================================================
# MediaWiki helpers — list revisions in the window, then diff the boundaries
# =============================================================================
def _mw_ts(dt) -> str:
    """Format a datetime as MediaWiki ISO 8601 (e.g. 2026-06-09T00:35:00Z)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_window_revisions(title, window_start, window_end):
    """Revisions of `title` within [window_start, window_end], oldest first.
    Uses API:Revisions. Returns a list of revision dicts (revid, parentid, ...)."""
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "titles": title,
        "rvprop": "ids|timestamp|comment|user|flags|size",
        "rvdir": "newer",                  # oldest first → rvstart < rvend
        "rvstart": _mw_ts(window_start),
        "rvend": _mw_ts(window_end),
        "rvlimit": REV_LIMIT,
    }
    try:
        r = requests.get(WIKI_API, params=params,
                         headers={"User-Agent": WIKI_UA}, timeout=15)
        r.raise_for_status()
        for page in r.json().get("query", {}).get("pages", {}).values():
            if page.get("revisions"):
                return page["revisions"]
    except Exception as e:
        print(f"  revisions fetch failed for '{title}': {e}")
    return []


def fetch_net_diff(from_rev, to_rev):
    """Net added/changed text between two revisions via the REST compare endpoint.
    wikidiff2 inline JSON: type 1 = added line, type 3 = changed line (text = new)."""
    url = f"{WIKI_REST}/revision/{from_rev}/compare/{to_rev}"
    try:
        r = requests.get(url, headers={"User-Agent": WIKI_UA}, timeout=15)
        r.raise_for_status()
        segments = r.json().get("diff", [])
        added = [s.get("text", "") for s in segments if s.get("type") in (1, 3)]
        text = "\n".join(t for t in added if t).strip()
        return text[:MAX_DIFF_CHARS] if text else None
    except Exception as e:
        print(f"  diff fetch failed ({from_rev}->{to_rev}): {e}")
    return None


def fetch_window_diff(title, window_start, window_end):
    """Net content added/changed on `title` across the anomaly window, or None."""
    revs = fetch_window_revisions(title, window_start, window_end)
    if not revs:
        return None
    oldest, newest = revs[0], revs[-1]
    from_rev = oldest.get("parentid") or oldest.get("revid")  # pre-window state
    to_rev = newest.get("revid")                              # post-window state
    if not from_rev or not to_rev or from_rev == to_rev:
        return None
    return fetch_net_diff(from_rev, to_rev)


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
# Step 2 — Gather diff + comments, then call Claude
# =============================================================================
def build_prompt(flag, comments_text, diff_text):
    if diff_text:
        content_block = f"Content added/changed on the page this window:\n{diff_text}\n\n"
    else:
        content_block = "(page content changes could not be retrieved)\n\n"
    return (
        f"Page: {flag['title']}\n"
        f"Edits in this 5-minute window: {flag['edit_count']} "
        f"(typical average: {flag['baseline_mean']}, z-score: {flag['z_score']})\n"
        f"Unique editors: {flag['unique_editors']}\n"
        f"Net byte change: {flag['total_byte_delta']}\n\n"
        f"{content_block}"
        f"Edit summaries from this window:\n{comments_text}\n\n"
        f"In 2-3 sentences, explain what is most likely happening on this page "
        f"and why it is being edited so heavily."
    )


summaries = []
pages_with_diff = 0
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

    diff_text = fetch_window_diff(flag["title"], flag["window_start"], flag["window_end"])
    if diff_text:
        pages_with_diff += 1

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            tools=[WEB_SEARCH_TOOL],
            messages=[{"role": "user",
                       "content": build_prompt(flag, comments_text, diff_text)}],
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

print(f"Pulled diff content for {pages_with_diff}/{len(flags)} page(s).")

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
#
# Note: diff-fetch coverage (how often the MediaWiki content pull succeeded) is
# printed at runtime as "Pulled diff content for N/M page(s)" — it is not stored
# per row. If it's consistently 0/M, this compute can't reach en.wikipedia.org.
