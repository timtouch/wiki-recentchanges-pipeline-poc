# =============================================================================
# gold_recentchange.py
# Delta Live Tables — Gold layer
# Phase 3, Steps 9–10: Rolling windowed aggregations over Silver enwiki edits
#
# HOW TO DEPLOY
# ─────────────────────────────────────────────────────────────────────────────
# Add this notebook to the SAME DLT pipeline as bronze_recentchange.py and
# silver_recentchange.py:
#   Pipelines → <your pipeline> → Settings → Libraries → Add library
#
# OUTPUT LATENCY
# ─────────────────────────────────────────────────────────────────────────────
# Gold applies a 10-minute watermark.  A window is only appended to the table
# after the watermark passes its end time, so results appear roughly 10–15
# minutes after each 5-minute window closes.  For the Step 12 dashboard this
# is acceptable; for lower-latency exploration, query silver_recentchange_enwiki
# directly.
# =============================================================================

import dlt
from pyspark.sql import functions as F


# =============================================================================
# STEP 9 / 10 — Tumbling 5-minute window: gold_page_edits_5min
# =============================================================================
# One row per (window, title, namespace) — appended once the window is
# finalized by the watermark.
#
# Consumers:
#   - Step 12 dashboard  → top edited pages, bytes written per hour
#   - gold_baseline_job  → reads this table to compute rolling baselines
#   - Future anomaly detection → compare edit_count to baseline mean ± 3σ
@dlt.table(
    name="gold_page_edits_5min",
    comment=(
        "Tumbling 5-minute windowed edit metrics per enwiki page.  "
        "Rows appended once the window is finalized (after the 10-minute "
        "watermark passes its end time).  Partitioned by window_date."
    ),
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["window_date"],
)
def gold_page_edits_5min():
    return (
        dlt.read_stream("silver_recentchange_enwiki")
        # Watermark bounds Spark's state store and determines when a window
        # is considered closed.  Must be applied before groupBy.
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            F.window(F.col("event_timestamp"), "5 minutes").alias("w"),
            F.col("title"),
            F.col("namespace"),
        )
        .agg(
            F.count("*").alias("edit_count"),
            # approx_count_distinct (HyperLogLog, ~5 % error) avoids storing
            # all distinct user strings in streaming state per window.
            # Exact countDistinct is too expensive at this cardinality.
            F.approx_count_distinct("user").alias("unique_editors"),
            F.sum("byte_delta").alias("total_byte_delta"),
            F.round(F.avg("byte_delta"), 1).alias("avg_byte_delta"),
            F.max("byte_delta").alias("max_byte_delta"),
            # Minor-edit ratio — editorial quality signal for future phases
            F.sum(
                F.when(F.col("minor") == True, 1).otherwise(0)
            ).alias("minor_edit_count"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("w.start").cast("date").alias("window_date"),
            F.col("title"),
            F.col("namespace"),
            F.col("edit_count"),
            F.col("unique_editors"),
            F.col("total_byte_delta"),
            F.col("avg_byte_delta"),
            F.col("max_byte_delta"),
            F.col("minor_edit_count"),
        )
    )


# =============================================================================
# STEP 9 (future hook) — Sliding 15-minute window: gold_page_edits_15min
# =============================================================================
# Uncomment when ready for smoother anomaly detection.
# Slide interval of 1 minute gives one aggregated row per minute per page,
# making edit-rate trends easier to detect than the coarser 5-minute tumble.
#
# @dlt.table(
#     name="gold_page_edits_15min",
#     comment=(
#         "Sliding 15-minute window (1-minute slide) over enwiki edits.  "
#         "Produces one row per (minute, title) for smoother anomaly detection.  "
#         "Higher state and storage cost than gold_page_edits_5min."
#     ),
#     table_properties={
#         "delta.autoOptimize.optimizeWrite": "true",
#     },
#     partition_cols=["window_date"],
# )
# def gold_page_edits_15min():
#     return (
#         dlt.read_stream("silver_recentchange_enwiki")
#         .withWatermark("event_timestamp", "10 minutes")
#         .groupBy(
#             F.window(F.col("event_timestamp"), "15 minutes", "1 minute").alias("w"),
#             F.col("title"),
#             F.col("namespace"),
#         )
#         .agg(
#             F.count("*").alias("edit_count"),
#             F.approx_count_distinct("user").alias("unique_editors"),
#             F.sum("byte_delta").alias("total_byte_delta"),
#         )
#         .select(
#             F.col("w.start").alias("window_start"),
#             F.col("w.end").alias("window_end"),
#             F.col("w.start").cast("date").alias("window_date"),
#             F.col("title"),
#             F.col("namespace"),
#             F.col("edit_count"),
#             F.col("unique_editors"),
#             F.col("total_byte_delta"),
#         )
#     )


# =============================================================================
# Validation queries — run after the pipeline has been up for ~20 min
# (windows need time to close through the watermark)
# =============================================================================
#
# 1. Confirm windows are landing
# ──────────────────────────────
# SELECT window_start, window_end, COUNT(*) AS page_count, SUM(edit_count) AS total_edits
# FROM wiki_poc.poc.gold_page_edits_5min
# ORDER BY window_start DESC LIMIT 10;
#
# 2. Top 20 pages by edit count in the last hour
# ────────────────────────────────────────────────
# SELECT title, SUM(edit_count) AS edits, SUM(unique_editors) AS editors
# FROM wiki_poc.poc.gold_page_edits_5min
# WHERE window_start >= NOW() - INTERVAL 1 HOUR
# GROUP BY title ORDER BY edits DESC LIMIT 20;
#
# 3. Cross-layer row count sanity check
# ──────────────────────────────────────
# SELECT
#   (SELECT COUNT(*) FROM wiki_poc.poc.bronze_recentchange_raw)       AS bronze,
#   (SELECT COUNT(*) FROM wiki_poc.poc.silver_recentchange_enwiki)    AS silver,
#   (SELECT SUM(edit_count) FROM wiki_poc.poc.gold_page_edits_5min)   AS gold_events;
# -- gold_events should be close to silver (slight diff from watermark lag)
#
# 4. Byte delta distribution across windows
# ──────────────────────────────────────────
# SELECT
#   ROUND(AVG(avg_byte_delta), 1) AS overall_avg_delta,
#   MAX(max_byte_delta)           AS largest_single_edit,
#   SUM(total_byte_delta)         AS net_bytes_added
# FROM wiki_poc.poc.gold_page_edits_5min
# WHERE window_date = CURRENT_DATE();
