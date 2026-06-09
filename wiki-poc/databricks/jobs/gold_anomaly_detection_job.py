# =============================================================================
# gold_anomaly_detection_job.py
# Databricks Workflow Job — NOT a DLT notebook
# Phase 5: Breaking news anomaly detection
#
# HOW TO DEPLOY
# ─────────────────────────────────────────────────────────────────────────────
# Create a Databricks Workflow that runs this notebook every 15 minutes:
#   Workflows → Create job
#   Task type:  Notebook
#   Cluster:    Serverless
#   Schedule:   Every 15 minutes  →  Quartz cron: 0 */15 * * * ?
#
# DEPENDENCIES
# ─────────────────────────────────────────────────────────────────────────────
# - wiki_poc.poc.gold_page_edits_5min     (written by DLT pipeline)
# - wiki_poc.poc.gold_page_edits_baseline (written by gold_baseline_job.py)
#   Baseline must have at least one run before this job produces output.
#   The job uses baseline_date = CURRENT_DATE() - 1 (yesterday's computation).
#
# OUTPUT
# ─────────────────────────────────────────────────────────────────────────────
# wiki_poc.poc.gold_anomaly_flags — one row per (title, window_start) that
# crossed the z-score threshold. Append-only; deduplicated across runs.
# The `summarized` column is a hook for the LLM summarizer (Phase 6):
# it picks up rows where summarized = false and sets it to true when done.
# =============================================================================

from delta.tables import DeltaTable
from pyspark.sql import functions as F

CATALOG        = "wiki_poc"
SCHEMA         = "poc"
GOLD_TABLE     = f"{CATALOG}.{SCHEMA}.gold_page_edits_5min"
BASELINE_TABLE = f"{CATALOG}.{SCHEMA}.gold_page_edits_baseline"
ANOMALY_TABLE  = f"{CATALOG}.{SCHEMA}.gold_anomaly_flags"

Z_THRESHOLD    = 3.0   # flag pages more than 3σ above their baseline mean
MIN_EDITS      = 3     # ignore windows with fewer edits — too noisy at low counts
LOOKBACK_MINS  = 60    # scan Gold windows from the last hour; covers watermark lag


# =============================================================================
# Step 1 — Compute z-scores for recent Gold windows
# =============================================================================
# Gold only contains finalized windows (DLT holds them until the watermark
# passes), so everything in the lookback window is a complete, stable count.
#
# Inner join excludes pages with no baseline entry (< 10 historical windows).
candidates = (
    spark.table(GOLD_TABLE).alias("g")
    .join(
        spark.table(BASELINE_TABLE)
            .filter(F.col("baseline_date") == F.current_date() - 1)
            .alias("b"),
        on=["title", "namespace"],
        how="inner",
    )
    .filter(
        F.col("g.window_start") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_MINS} MINUTES")
    )
    .filter(F.col("g.edit_count") >= MIN_EDITS)
    .withColumn(
        "z_score",
        F.when(
            F.col("b.stddev_edit_count").isNotNull() & (F.col("b.stddev_edit_count") > 0),
            F.round(
                (F.col("g.edit_count") - F.col("b.mean_edit_count"))
                / F.col("b.stddev_edit_count"),
                2,
            ),
        ).otherwise(F.lit(None).cast("double")),
    )
    .filter(F.col("z_score") >= Z_THRESHOLD)
    .select(
        F.col("g.title"),
        F.col("g.namespace"),
        F.col("g.window_start"),
        F.col("g.window_end"),
        F.col("g.edit_count"),
        F.col("g.unique_editors"),
        F.col("g.total_byte_delta"),
        F.round(F.col("b.mean_edit_count"),  2).alias("baseline_mean"),
        F.round(F.col("b.stddev_edit_count"), 2).alias("baseline_stddev"),
        F.col("z_score"),
        F.current_timestamp().alias("detected_at"),
        # Hook for Phase 6 (LLM summarizer): picks up rows where summarized = false
        F.lit(False).alias("summarized"),
    )
)


# =============================================================================
# Step 2 — Deduplicate: skip windows already in the anomaly table
# =============================================================================
# The job runs every 15 minutes but scans the last 60 minutes of Gold, so
# the same (title, window_start) pair will appear in multiple runs.
# Only insert rows that haven't been flagged before.
if DeltaTable.isDeltaTable(spark, ANOMALY_TABLE):
    already_flagged = spark.table(ANOMALY_TABLE).select("title", "window_start")
    new_anomalies = candidates.join(already_flagged, on=["title", "window_start"], how="left_anti")
else:
    # First run — table doesn't exist yet; all candidates are new
    new_anomalies = candidates


# =============================================================================
# Step 3 — Write new anomaly flags (single execution; 0-row write is a no-op)
# =============================================================================
new_anomalies.write.format("delta").mode("append").saveAsTable(ANOMALY_TABLE)


# =============================================================================
# Step 4 — Report results by reading back from Delta (no re-computation)
# =============================================================================
written = (
    spark.table(ANOMALY_TABLE)
    .filter(F.col("detected_at") >= F.current_timestamp() - F.expr("INTERVAL 2 MINUTES"))
)
new_count = written.count()

if new_count == 0:
    print(f"No new anomalies detected in the last {LOOKBACK_MINS} minutes.")
    dbutils.notebook.exit("no_anomalies")

print(f"Flagged {new_count} new anomalous window(s) (z_score >= {Z_THRESHOLD}):")
display(written.orderBy(F.col("z_score").desc()))


# =============================================================================
# Validation queries — run ad hoc to inspect the anomaly table
# =============================================================================
#
# -- Recent anomalies (last 2 hours)
# SELECT title, window_start, edit_count, baseline_mean, baseline_stddev, z_score, detected_at
# FROM wiki_poc.poc.gold_anomaly_flags
# WHERE detected_at >= NOW() - INTERVAL 2 HOURS
# ORDER BY z_score DESC;
#
# -- Pages with the most anomalous windows today (persistent surges)
# SELECT title, COUNT(*) AS anomalous_windows, MAX(z_score) AS peak_z_score, MAX(edit_count) AS peak_edits
# FROM wiki_poc.poc.gold_anomaly_flags
# WHERE detected_at >= CURRENT_DATE()
# GROUP BY title
# ORDER BY anomalous_windows DESC, peak_z_score DESC
# LIMIT 20;
#
# -- Unsummarized anomalies (queue for Phase 6 LLM summarizer)
# SELECT * FROM wiki_poc.poc.gold_anomaly_flags
# WHERE summarized = false
# ORDER BY z_score DESC;
