-- =============================================================================
-- wiki_poc_dashboard_queries.sql
-- Step 12: Observability — Databricks SQL dashboard queries
-- =============================================================================
--
-- HOW TO BUILD THE DASHBOARD
-- ─────────────────────────────────────────────────────────────────────────────
-- 1. SQL Editor → New query → paste query → save with the name in the header
-- 2. Add a visualization to the saved query (chart type noted per query)
-- 3. Dashboards → New dashboard → Add widget → pick the visualization
-- 4. Arrange into three sections: Producer Health / Pipeline Health / Content View
--
-- All queries run against a Serverless SQL Warehouse.
-- Refresh the dashboard on a 1-minute auto-refresh for live monitoring.
-- =============================================================================


-- =============================================================================
-- SECTION 1: PRODUCER HEALTH
-- =============================================================================

-- PH-1  Bronze ingest rate — last 24 hours
-- Widget: Line chart  |  x=hour  y=events_ingested (primary), mb_ingested (secondary)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  DATE_TRUNC('hour', ingest_timestamp)           AS hour,
  COUNT(*)                                        AS events_ingested,
  ROUND(SUM(LENGTH(raw_json)) / 1048576.0, 1)    AS mb_ingested
FROM wiki_poc.poc.bronze_recentchange_raw
WHERE ingest_timestamp >= NOW() - INTERVAL 24 HOURS
GROUP BY 1
ORDER BY 1;


-- PH-2  Staleness counter — minutes since last Bronze event
-- Widget: Counter  |  value=minutes_stale  |  alert visually if > 10
-- (Fargate stall alarm fires at 5 min via CloudWatch, but this gives
--  an at-a-glance read inside the dashboard)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  MAX(ingest_timestamp)                                                  AS latest_event,
  TIMESTAMPDIFF(MINUTE, MAX(ingest_timestamp), CURRENT_TIMESTAMP())     AS minutes_stale
FROM wiki_poc.poc.bronze_recentchange_raw;


-- PH-3  Hourly ingest rate — last 7 days (trend / anomaly baseline)
-- Widget: Bar chart  |  x=day_hour  y=events_ingested
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  DATE_TRUNC('hour', ingest_timestamp)   AS day_hour,
  COUNT(*)                               AS events_ingested
FROM wiki_poc.poc.bronze_recentchange_raw
WHERE ingest_timestamp >= NOW() - INTERVAL 7 DAYS
GROUP BY 1
ORDER BY 1;


-- =============================================================================
-- SECTION 2: PIPELINE HEALTH
-- =============================================================================

-- PL-1  Row counts per layer — all time
-- Widget: Table  (simple health snapshot; latest_event confirms each layer is live)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 'bronze'            AS layer, COUNT(*) AS total_rows, MAX(ingest_timestamp) AS latest_event
FROM wiki_poc.poc.bronze_recentchange_raw
UNION ALL
SELECT 'silver_enwiki',              COUNT(*), MAX(ingest_timestamp)
FROM wiki_poc.poc.silver_recentchange_enwiki
UNION ALL
SELECT 'silver_quarantine',          COUNT(*), MAX(ingest_timestamp)
FROM wiki_poc.poc.silver_recentchange_quarantine
UNION ALL
SELECT 'gold_5min_windows',          COUNT(*), MAX(window_end)
FROM wiki_poc.poc.gold_page_edits_5min;


-- PL-2  Silver pass rate and quarantine rate — last hour
-- Widget: Counter  |  values=pass_rate_pct, quarantine_rate_pct
-- Expected: pass_rate_pct ~5–15%, quarantine_rate_pct ~85–95%
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  b.bronze_1h,
  s.silver_1h,
  q.quarantine_1h,
  ROUND(100.0 * s.silver_1h    / NULLIF(b.bronze_1h, 0), 1) AS pass_rate_pct,
  ROUND(100.0 * q.quarantine_1h / NULLIF(b.bronze_1h, 0), 1) AS quarantine_rate_pct
FROM
  (SELECT COUNT(*) AS bronze_1h      FROM wiki_poc.poc.bronze_recentchange_raw       WHERE ingest_timestamp >= NOW() - INTERVAL 1 HOUR) b,
  (SELECT COUNT(*) AS silver_1h      FROM wiki_poc.poc.silver_recentchange_enwiki     WHERE ingest_timestamp >= NOW() - INTERVAL 1 HOUR) s,
  (SELECT COUNT(*) AS quarantine_1h  FROM wiki_poc.poc.silver_recentchange_quarantine WHERE ingest_timestamp >= NOW() - INTERVAL 1 HOUR) q;


-- PL-3  Quarantine breakdown by reason — last hour
-- Widget: Bar chart  |  x=quarantine_reason  y=events (sorted desc)
-- Expected dominant reason: non_enwiki (most of the firehose), then non_edit_type, then bot
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  quarantine_reason,
  COUNT(*)                                              AS events,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)  AS pct
FROM wiki_poc.poc.silver_recentchange_quarantine
WHERE ingest_timestamp >= NOW() - INTERVAL 1 HOUR
GROUP BY quarantine_reason
ORDER BY events DESC;


-- PL-4  Silver events per 5-minute bucket — last 2 hours
-- Widget: Line chart  |  x=bucket_start  y=silver_events
-- Gaps in this chart (buckets with 0 or no row) indicate pipeline stalls
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  from_unixtime(FLOOR(unix_timestamp(ingest_timestamp) / 300) * 300)  AS bucket_start,
  COUNT(*)                                                              AS silver_events
FROM wiki_poc.poc.silver_recentchange_enwiki
WHERE ingest_timestamp >= NOW() - INTERVAL 2 HOURS
GROUP BY 1
ORDER BY 1;


-- PL-5  Gold windows landing — last 2 hours
-- Widget: Line chart  |  x=window_start  y=total_edits
-- Windows arrive ~10-15 min after they close (watermark lag) — a flat line
-- means the watermark is advancing but no edits are landing (unusual)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  window_start,
  SUM(edit_count)        AS total_edits,
  COUNT(DISTINCT title)  AS pages_with_edits
FROM wiki_poc.poc.gold_page_edits_5min
WHERE window_start >= NOW() - INTERVAL 2 HOURS
GROUP BY window_start
ORDER BY window_start;


-- =============================================================================
-- SECTION 3: CONTENT VIEW
-- =============================================================================

-- CV-1  Top 20 article pages by edit count — last hour
-- Widget: Table  |  sort by total_edits desc
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  title,
  SUM(edit_count)             AS total_edits,
  SUM(unique_editors)         AS editors,
  SUM(total_byte_delta)       AS net_bytes,
  SUM(minor_edit_count)       AS minor_edits
FROM wiki_poc.poc.gold_page_edits_5min
WHERE window_start >= NOW() - INTERVAL 1 HOUR
GROUP BY title
ORDER BY total_edits DESC
LIMIT 20;


-- CV-2  Article edit volume over time — last 6 hours
-- Widget: Line chart  |  x=window_start  y=total_edits  y2=pages_edited
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  window_start,
  SUM(edit_count)          AS total_edits,
  COUNT(DISTINCT title)    AS pages_edited,
  SUM(total_byte_delta)    AS net_bytes_added
FROM wiki_poc.poc.gold_page_edits_5min
WHERE window_start >= NOW() - INTERVAL 6 HOURS
GROUP BY window_start
ORDER BY window_start;


-- CV-3  Largest single article edits — last hour
-- Widget: Table
-- Useful for spotting mass-reverts (large negative delta) or major additions
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  title,
  user,
  event_timestamp,
  byte_delta,
  SUBSTRING(comment, 1, 120)  AS comment_preview
FROM wiki_poc.poc.silver_recentchange_enwiki
WHERE event_timestamp >= NOW() - INTERVAL 1 HOUR
  AND byte_delta IS NOT NULL
ORDER BY ABS(byte_delta) DESC
LIMIT 10;


-- =============================================================================
-- ALERT QUERY
-- =============================================================================
--
-- ALT-1  Silver gap detection
-- ─────────────────────────────────────────────────────────────────────────────
-- HOW TO SET UP THE ALERT
--   1. Save this as a standalone query named "ALT-1 Silver gap detection"
--   2. Alerts → New alert → select this query → column: silver_events_15min
--   3. Condition: value IS EQUAL TO 0
--   4. Refresh schedule: every 5 minutes
--   5. Notification: email / Slack webhook
--
-- Fires when no Silver events have landed in 15 minutes — almost always
-- means the DLT pipeline has stalled or the Fargate producer has stopped.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT COUNT(*) AS silver_events_15min
FROM wiki_poc.poc.silver_recentchange_enwiki
WHERE ingest_timestamp >= NOW() - INTERVAL 15 MINUTES;


-- =============================================================================
-- SECTION 4: ANOMALY DETECTION
-- =============================================================================

-- AN-1  Recent anomalies — last 2 hours
-- Widget: Table  |  sort by z_score desc
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  title,
  window_start,
  window_end,
  edit_count,
  baseline_mean,
  baseline_stddev,
  z_score,
  unique_editors,
  total_byte_delta,
  detected_at
FROM wiki_poc.poc.gold_anomaly_flags
WHERE detected_at >= NOW() - INTERVAL 2 HOURS
ORDER BY z_score DESC
LIMIT 20;


-- AN-2  Anomaly count over time — today (line chart for surge patterns)
-- Widget: Line chart  |  x=hour  y=anomaly_count
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  DATE_TRUNC('hour', detected_at)  AS hour,
  COUNT(*)                          AS anomaly_count,
  MAX(z_score)                      AS peak_z_score
FROM wiki_poc.poc.gold_anomaly_flags
WHERE detected_at >= CURRENT_DATE()
GROUP BY 1
ORDER BY 1;


-- AN-3  Top pages by anomalous window count — today
-- Widget: Bar chart  |  x=title  y=anomalous_windows
-- Pages appearing multiple times are sustained surges, not one-off spikes
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  title,
  COUNT(*)          AS anomalous_windows,
  MAX(z_score)      AS peak_z_score,
  MAX(edit_count)   AS peak_edits,
  MIN(window_start) AS first_detected
FROM wiki_poc.poc.gold_anomaly_flags
WHERE detected_at >= CURRENT_DATE()
GROUP BY title
ORDER BY anomalous_windows DESC, peak_z_score DESC
LIMIT 15;


-- =============================================================================
-- ALERT QUERY — Anomaly detection
-- =============================================================================
--
-- ALT-2  New anomalies in the last 15 minutes
-- ─────────────────────────────────────────────────────────────────────────────
-- HOW TO SET UP THE ALERT
--   1. Save as "ALT-2 New anomaly flags"
--   2. Alerts → New alert → select this query → column: new_anomalies
--   3. Condition: value IS GREATER THAN 0
--   4. Refresh schedule: every 5 minutes
--   5. Notification: email / Slack webhook
-- ─────────────────────────────────────────────────────────────────────────────
SELECT COUNT(*) AS new_anomalies
FROM wiki_poc.poc.gold_anomaly_flags
WHERE detected_at >= NOW() - INTERVAL 15 MINUTES
  AND summarized = false;


-- =============================================================================
-- SUPPLEMENTARY: Anomaly preview (not a dashboard widget — run ad hoc)
-- =============================================================================
--
-- Shows pages currently spiking above 3σ of their 30-day baseline.
-- Requires at least one run of gold_baseline_job.py to have completed.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  g.title,
  g.namespace,
  g.window_start,
  g.edit_count,
  ROUND(b.mean_edit_count, 1)    AS baseline_mean,
  ROUND(b.stddev_edit_count, 1)  AS baseline_stddev,
  ROUND(
    (g.edit_count - b.mean_edit_count) / NULLIF(b.stddev_edit_count, 0),
  2)                             AS z_score
FROM wiki_poc.poc.gold_page_edits_5min g
JOIN wiki_poc.poc.gold_page_edits_baseline b
  ON  g.title     = b.title
  AND g.namespace = b.namespace
  AND b.baseline_date = CURRENT_DATE() - 1
WHERE g.window_start >= NOW() - INTERVAL 1 HOUR
  AND (g.edit_count - b.mean_edit_count) / NULLIF(b.stddev_edit_count, 0) > 3
ORDER BY z_score DESC
LIMIT 20;
