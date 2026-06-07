# =============================================================================
# silver_recentchange.py
# Delta Live Tables — Silver layer
# Phase 2, Steps 6–8: Cleaned, Conformed & Quality-Gated enwiki edits
#
# HOW TO DEPLOY
# ─────────────────────────────────────────────────────────────────────────────
# Add this notebook to the SAME DLT pipeline as bronze_recentchange.py:
#   Pipelines → <your pipeline> → Settings → Libraries → Add library
#
# All Bronze, Silver, and Gold tables share one pipeline and one schema
# (e.g. wiki_poc.poc). The layer is encoded in the table name prefix:
#   bronze_recentchange_raw
#   silver_recentchange_enwiki
#   silver_recentchange_quarantine
#
# dlt.read_stream() resolves table names within the same pipeline at runtime
# — no catalog/schema prefix needed.
# =============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# =============================================================================
# STEP 6 — Explicit Silver schema  (never use inferSchema in Silver)
# =============================================================================
#
# Design notes:
#
#  from_json is permissive by default: fields absent from a given event surface
#  as NULL; fields not listed in the schema are silently ignored.  Upstream
#  schema drift therefore does not break ingestion.
#
#  log_params is intentionally EXCLUDED from this schema.  Its structure varies
#  by log_type — for example, abusefilter/hit sends a StructType while other
#  log types send arrays, scalars, or nothing.  from_json requires a fixed type
#  per field, so any single choice silently NULLs out most log event payloads.
#  Instead, log_params is extracted via get_json_object() on raw_json in the
#  view below, which preserves it as a JSON string for all log_type variants.
#  (Silver filters log events to quarantine anyway, but the quarantine rows
#  retain a readable log_params string for future audit / multi-wiki work.)
#
#  LLM-phase hooks baked in per the plan:
#    - comment       (raw edit summary)   → breaking-news summarizer
#    - revision_old / revision_new        → diff fetcher / vandalism classifier
#    - title_url                          → diff URL construction
RECENTCHANGE_SCHEMA = StructType([
    StructField("schema", StringType()),
    StructField("meta", StructType([
        StructField("uri",        StringType()),
        StructField("request_id", StringType()),
        StructField("id",         StringType()),    # event UUID — dedup / join key
        StructField("dt",         StringType()),    # ISO-8601 event time (string backup)
        StructField("domain",     StringType()),
        StructField("stream",     StringType()),
        StructField("topic",      StringType()),
        StructField("partition",  IntegerType()),
        StructField("offset",     LongType()),
    ])),
    StructField("id",            LongType()),       # MediaWiki revision / log integer ID
    StructField("type",          StringType()),     # edit | new | log | categorize | ...
    StructField("namespace",     IntegerType()),
    StructField("title",         StringType()),
    StructField("title_url",     StringType()),     # kept: diff URL for LLM phase
    StructField("comment",       StringType()),     # kept: raw edit summary for LLM phase
    StructField("parsedcomment", StringType()),
    StructField("timestamp",     LongType()),       # Unix epoch seconds
    StructField("user",          StringType()),
    StructField("bot",           BooleanType()),
    StructField("minor",         BooleanType()),
    StructField("patrolled",     BooleanType()),
    StructField("notify_url",          StringType()),
    StructField("server_url",          StringType()),
    StructField("server_name",         StringType()),
    StructField("server_script_path",  StringType()),
    StructField("wiki",                StringType()),
    StructField("length", StructType([
        StructField("old", IntegerType()),
        StructField("new", IntegerType()),
    ])),
    StructField("revision", StructType([
        StructField("old", LongType()),
        StructField("new", LongType()),
    ])),
    StructField("log_id",             LongType()),
    StructField("log_type",           StringType()),
    StructField("log_action",         StringType()),
    # log_params intentionally omitted — see design note above.
    StructField("log_action_comment", StringType()),
])


# =============================================================================
# STEP 7, Part 1 — Intermediate parsed view
# =============================================================================
# Every Bronze row flows through here.  Parse failures produce all-NULL columns
# and are naturally routed to quarantine by the downstream filter logic.
#
# Intentionally a pure projection — no watermark, dedup, or filter.
# DLT inlines a streaming view into each downstream table at compile time, so
# silver_enwiki and silver_quarantine get fully independent stream checkpoints
# and state stores.  Applying a watermark here would create shared streaming
# state that makes the fan-out harder to reason about.
@dlt.view(name="silver_recentchange_parsed")
def _silver_parsed():
    return (
        dlt.read_stream("bronze_recentchange_raw")  # Bronze table in this pipeline
        .select(
            F.from_json(F.col("raw_json"), RECENTCHANGE_SCHEMA).alias("e"),
            # log_params excluded from RECENTCHANGE_SCHEMA (variable structure by log_type).
            # get_json_object captures it as a raw JSON string regardless of whether
            # the value is an object, array, or scalar — NULL for edit/new events.
            F.get_json_object(F.col("raw_json"), "$.log_params").alias("_log_params_raw"),
            F.col("ingest_timestamp"),
            F.col("ingest_date"),
            F.col("source_file"),
        )
        .select(
            # ── Identity ─────────────────────────────────────────────────────
            F.col("e.meta.id").alias("event_id"),     # UUID — dedup key
            F.col("e.id").alias("mw_id"),             # MediaWiki integer ID
            F.col("e.type").alias("event_type"),
            F.col("e.wiki"),
            F.col("e.namespace"),
            # ── Page ─────────────────────────────────────────────────────────
            F.col("e.title"),
            F.col("e.title_url"),
            F.col("e.server_name"),
            F.col("e.server_url"),
            # ── User / editorial flags ────────────────────────────────────────
            F.col("e.user"),
            F.col("e.bot"),
            F.col("e.minor"),
            F.col("e.patrolled"),
            # ── Timing ───────────────────────────────────────────────────────
            # Cast LongType (epoch-seconds) → TimestampType.
            # Watermark is applied inside silver_enwiki, not here, so this
            # view stays stateless and the fan-out is clean.
            F.col("e.timestamp").cast("timestamp").alias("event_timestamp"),
            F.col("e.meta.dt").alias("event_dt"),     # ISO-8601 fallback
            # ── Edit payload ─────────────────────────────────────────────────
            F.col("e.comment"),                       # raw summary for LLM summarizer
            F.col("e.parsedcomment"),
            F.col("e.length.old").alias("length_old"),
            F.col("e.length.new").alias("length_new"),
            (F.col("e.length.new") - F.col("e.length.old")).alias("byte_delta"),
            # ── Revision IDs — vandalism classifier + diff summarizer hooks ──
            F.col("e.revision.old").alias("revision_old"),
            F.col("e.revision.new").alias("revision_new"),
            # ── Log fields (NULL for edit / new events) ───────────────────────
            F.col("e.log_id"),
            F.col("e.log_type"),
            F.col("e.log_action"),
            F.col("_log_params_raw").alias("log_params"),  # JSON string; structure varies by log_type
            F.col("e.log_action_comment"),
            # ── Lineage ───────────────────────────────────────────────────────
            F.col("ingest_timestamp"),
            F.col("ingest_date"),
            F.col("source_file"),
        )
    )


# =============================================================================
# Filter predicate — shared between the main table and quarantine
# =============================================================================
# NULL handling note
# ──────────────────
# Spark's filter() keeps only rows where the predicate evaluates to TRUE
# (not FALSE, not NULL).  When any discriminator field is NULL (e.g. a parse
# failure), _PASS evaluates to NULL, so those rows are excluded from the main
# table automatically — correct behavior.
#
# The quarantine wraps _PASS in F.coalesce(_PASS, F.lit(False)) so NULL rows
# are captured there instead of silently vanishing from both tables.
_PASS = (
    (F.col("bot") == False)
    & (F.col("wiki") == "enwiki")
    & F.col("event_type").isin("edit", "new")
)


# =============================================================================
# STEP 7, Part 2  +  STEP 8 — Silver main table: recentchange_enwiki
# =============================================================================
@dlt.table(
    name="silver_recentchange_enwiki",
    comment=(
        "Filtered, deduped, conformed stream of human enwiki edits and "
        "new-page events.  One row per unique event_id (deduplicated within a "
        "10-minute watermark window).  Expected volume: ~5–15 % of Bronze."
    ),
    table_properties={
        "delta.enableChangeDataFeed":       "true",   # Gold + future phases read via CDF
        "delta.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.managed":   "true",
    },
    partition_cols=["ingest_date"],
)
# ── STEP 8: DLT data quality expectations ────────────────────────────────────
# expect_or_drop → hard failure: offending row is dropped and counted in the
#   DLT pipeline metrics / event log.
# expect          → soft warning: row is retained; violation count in metrics.
@dlt.expect_or_drop("title_not_null", "title IS NOT NULL")
@dlt.expect_or_drop("user_not_null",  "user  IS NOT NULL")
@dlt.expect(
    "byte_delta_plausible",
    # NULL is allowed — new-page events have no length.old.
    # ±10 MB covers even the largest legitimate mass-edits / page moves.
    "byte_delta IS NULL OR (byte_delta >= -10000000 AND byte_delta <= 10000000)",
)
def silver_enwiki():
    return (
        dlt.read_stream("silver_recentchange_parsed")
        .filter(_PASS)
        # ── Stateful streaming dedup ──────────────────────────────────────────
        # withWatermark MUST precede dropDuplicates in streaming mode.
        # 10 minutes:
        #   - Tolerates SSE at-least-once delivery and brief producer restarts
        #     (the Fargate task replays from Last-Event-ID on reconnect)
        #   - Keeps Spark's state store from growing unbounded
        # event_id is the Wikimedia-assigned UUID — globally unique per event.
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["event_id"])
        # event_date: handy partition-pruning column for Gold aggregation queries
        .withColumn("event_date", F.col("event_timestamp").cast("date"))
    )


# =============================================================================
# STEP 7, Part 2 — Quarantine table: recentchange_quarantine
# =============================================================================
@dlt.table(
    name="silver_recentchange_quarantine",
    comment=(
        "Rows excluded from silver.recentchange_enwiki: bot edits, non-enwiki "
        "wikis, non-edit/new event types (log, categorize, …), and parse "
        "failures.  Expected volume: ~85–95 % of Bronze (most of the all-wikis "
        "firehose is non-enwiki or bot-generated).  Retained for audit, "
        "filter-rate monitoring in the Step 12 dashboard, and future multi-wiki "
        "expansion."
    ),
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "pipelines.autoOptimize.managed":   "true",
    },
    partition_cols=["ingest_date"],
)
def silver_quarantine():
    return (
        dlt.read_stream("silver_recentchange_parsed")
        # F.coalesce(_PASS, F.lit(False)):
        #   _PASS = True  → coalesce returns True  → ~True  = False → NOT quarantined
        #   _PASS = False → coalesce returns False → ~False = True  → quarantined
        #   _PASS = NULL  → coalesce returns False → ~False = True  → quarantined ✓
        # The last case captures parse failures that would otherwise fall through
        # the cracks (neither filter would claim them).
        .filter(~F.coalesce(_PASS, F.lit(False)))
        .withColumn(
            "quarantine_reason",
            # First-match wins; ordered most-specific → most-general.
            # eqNullSafe handles the edge case where a field itself is NULL.
            F.when(
                F.col("event_id").isNull(),
                F.lit("parse_failure"),
            )
            .when(
                F.col("bot").eqNullSafe(True),
                F.lit("bot"),
            )
            .when(
                F.col("wiki").isNull() | (F.col("wiki") != "enwiki"),
                F.lit("non_enwiki"),
            )
            .when(
                F.col("event_type").isNull()
                | (~F.col("event_type").isin("edit", "new")),
                F.lit("non_edit_type"),
            )
            .otherwise(F.lit("unknown")),
        )
    )


# =============================================================================
# Validation queries — run in a Databricks SQL cell after ≥ 5 min of pipeline
# =============================================================================
#
# 1. Volume sanity check (Silver should be ~5–15 % of Bronze)
# ─────────────────────────────────────────────────────────
# SELECT
#   (SELECT COUNT(*) FROM wiki_poc.poc.silver_recentchange_enwiki)     AS silver_rows,
#   (SELECT COUNT(*) FROM wiki_poc.poc.bronze_recentchange_raw)        AS bronze_rows,
#   (SELECT COUNT(*) FROM wiki_poc.poc.silver_recentchange_quarantine) AS quarantine_rows;
#
# 2. Quarantine breakdown by reason
# ──────────────────────────────────
# SELECT quarantine_reason, COUNT(*) AS cnt,
#        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
# FROM wiki_poc.poc.silver_recentchange_quarantine
# GROUP BY quarantine_reason ORDER BY cnt DESC;
#
# 3. Filter-correctness check (should return exactly one row: false | enwiki)
# ──────────────────────────────────────────────────────────────────────────
# SELECT DISTINCT bot, wiki FROM wiki_poc.poc.silver_recentchange_enwiki;
#
# 4. Dedup check (should return 0 rows)
# ──────────────────────────────────────
# SELECT event_id, COUNT(*) AS dupes
# FROM wiki_poc.poc.silver_recentchange_enwiki
# GROUP BY event_id HAVING dupes > 1 LIMIT 10;
#
# 5. byte_delta distribution
# ────────────────────────────
# SELECT
#   MIN(byte_delta)              AS min_delta,
#   MAX(byte_delta)              AS max_delta,
#   ROUND(AVG(byte_delta), 1)    AS avg_delta,
#   PERCENTILE(byte_delta, 0.5)  AS p50_delta,
#   PERCENTILE(byte_delta, 0.95) AS p95_delta,
#   COUNT_IF(byte_delta IS NULL) AS null_delta_rows   -- expected for new-page events
# FROM wiki_poc.poc.silver_recentchange_enwiki;
#
# 6. Top 10 pages by edit count in the last hour
# ──────────────────────────────────────────────
# SELECT title, COUNT(*) AS edits, COUNT(DISTINCT user) AS unique_editors
# FROM wiki_poc.poc.silver_recentchange_enwiki
# WHERE event_timestamp >= NOW() - INTERVAL 1 HOUR
# GROUP BY title ORDER BY edits DESC LIMIT 10;
#
# 7. DLT expectation failures (run as a DLT event log query)
# ─────────────────────────────────────────────────────────
# SELECT details:flow_name, details:expectations
# FROM event_log('<pipeline_id>')
# WHERE event_type = 'flow_progress'
#   AND details:flow_progress:metrics:num_rows_dropped_by_expectations > 0
# ORDER BY timestamp DESC LIMIT 20;
