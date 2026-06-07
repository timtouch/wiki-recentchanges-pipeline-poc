# =============================================================================
# gold_baseline_job.py
# Databricks Workflow Job — NOT a DLT notebook
# Phase 3, Step 10: Rolling 30-day edit baseline per page
#
# HOW TO DEPLOY
# ─────────────────────────────────────────────────────────────────────────────
# Create a Databricks Workflow that runs this notebook on a daily schedule:
#   Workflows → Create job
#   Task type:  Notebook
#   Cluster:    Serverless
#   Schedule:   Daily, 03:00 UTC
#               (late enough that the previous day's windows have closed
#               through the 10-minute DLT watermark and been committed)
#
# Each run writes one partition (baseline_date = today) to
# gold_page_edits_baseline, preserving prior days' baselines intact.
# The anomaly detector always joins on baseline_date = CURRENT_DATE() - 1
# (yesterday's computation, since the job runs after midnight).
# =============================================================================

from datetime import date
from pyspark.sql import functions as F

CATALOG        = "wiki_poc"
SCHEMA         = "poc"
SOURCE_TABLE   = f"{CATALOG}.{SCHEMA}.gold_page_edits_5min"
BASELINE_TABLE = f"{CATALOG}.{SCHEMA}.gold_page_edits_baseline"
LOOKBACK_DAYS  = 30
MIN_WINDOWS    = 10   # pages with fewer samples excluded — stats unreliable below this


# =============================================================================
# Compute rolling baseline
# =============================================================================
# For each (title, namespace), compute mean and stddev of edit_count across
# all 5-minute windows in the last LOOKBACK_DAYS days.
#
# The z-score comparison the anomaly detector will use:
#   z = (current_window.edit_count - mean_edit_count) / stddev_edit_count
#   z > 3  →  flag as a potential breaking-news surge
#
# Pages below MIN_WINDOWS are excluded: stddev is meaningless with too few
# samples and produces noisy false positives.
baseline_df = (
    spark.table(SOURCE_TABLE)
    .filter(
        F.col("window_start") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS")
    )
    .groupBy("title", "namespace")
    .agg(
        F.count("*").alias("window_count"),
        F.avg("edit_count").alias("mean_edit_count"),
        F.stddev("edit_count").alias("stddev_edit_count"),
        F.avg("unique_editors").alias("mean_unique_editors"),
        F.avg("total_byte_delta").alias("mean_total_byte_delta"),
        F.min("window_start").alias("data_from"),
        F.max("window_start").alias("data_through"),
    )
    .filter(F.col("window_count") >= MIN_WINDOWS)
    .withColumn("baseline_date",  F.current_date())
    .withColumn("computed_at",    F.current_timestamp())
    .withColumn("lookback_days",  F.lit(LOOKBACK_DAYS))
)


# =============================================================================
# Write — one partition per run, daily baseline history preserved
# =============================================================================
# dynamic partitionOverwriteMode replaces only today's partition, so prior
# days' baselines remain available for tracking baseline drift over time.
(
    baseline_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("baseline_date")
    .saveAsTable(BASELINE_TABLE)
)

today = date.today()
row_count = (
    spark.table(BASELINE_TABLE)
    .filter(F.col("baseline_date") == F.lit(today))
    .count()
)
print(f"Baseline written: {row_count:,} pages for baseline_date={today}")


# =============================================================================
# Anomaly detection preview (hook for future phase)
# =============================================================================
# Run this cell interactively after the job completes to preview what the
# anomaly detector would flag right now given yesterday's baseline.
#
# SELECT
#   g.title,
#   g.namespace,
#   g.window_start,
#   g.edit_count,
#   ROUND(b.mean_edit_count, 1)   AS baseline_mean,
#   ROUND(b.stddev_edit_count, 1) AS baseline_stddev,
#   ROUND(
#     (g.edit_count - b.mean_edit_count) / NULLIF(b.stddev_edit_count, 0),
#   2) AS z_score
# FROM wiki_poc.poc.gold_page_edits_5min  g
# JOIN wiki_poc.poc.gold_page_edits_baseline b
#   ON  g.title     = b.title
#   AND g.namespace = b.namespace
#   AND b.baseline_date = CURRENT_DATE() - 1
# WHERE g.window_start >= NOW() - INTERVAL 1 HOUR
#   AND (g.edit_count - b.mean_edit_count) / NULLIF(b.stddev_edit_count, 0) > 3
# ORDER BY z_score DESC
# LIMIT 20;
