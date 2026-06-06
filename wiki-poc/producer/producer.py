"""
Wikimedia Recent Changes SSE Producer
Consumes the Wikimedia SSE stream and writes JSONL files to S3,
with DynamoDB checkpointing for resumable delivery.
"""

import io
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

import boto3
import requests
from sseclient import SSEClient

# ---------------------------------------------------------------------------
# Configuration (all overridable via environment variables)
# ---------------------------------------------------------------------------
SSE_URL = os.environ.get("SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
S3_BUCKET = os.environ.get("S3_BUCKET", "wiki-raw-poc")
S3_PREFIX = os.environ.get("S3_PREFIX", "recentchange")
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE", "wiki_producer_checkpoint")
CHECKPOINT_KEY = os.environ.get("CHECKPOINT_KEY", "recentchange")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
CW_NAMESPACE = os.environ.get("CW_NAMESPACE", "WikiProducer")

# Wikimedia's User-Agent policy requires a descriptive UA identifying the client
# (and ideally a contact). A generic library UA like "python-requests/x.y" is
# rejected with HTTP 403. Override CONTACT via env var with a real email/URL.
USER_AGENT = os.environ.get(
    "USER_AGENT",
    "wiki-recentchanges-poc/1.0 (https://github.com/your-org/wiki-poc; chaosbounder@gmail.com)",
)

FLUSH_INTERVAL_SECS = int(os.environ.get("FLUSH_INTERVAL_SECS", "60"))
FLUSH_SIZE_BYTES = int(os.environ.get("FLUSH_SIZE_BYTES", str(5 * 1024 * 1024)))  # 5 MB
BACKOFF_CAP_SECS = int(os.environ.get("BACKOFF_CAP_SECS", "30"))
# How often to publish CloudWatch metrics. One batched PutMetricData call per
# interval keeps API request volume low (well under the 1M/month free tier).
# Aligned to 60s to match the stream-stall alarm's 60s period.
METRIC_INTERVAL_SECS = int(os.environ.get("METRIC_INTERVAL_SECS", "60"))

# ---------------------------------------------------------------------------
# Logging — structured JSON to stdout so CloudWatch can parse it
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------
s3 = boto3.client("s3", region_name=AWS_REGION)
dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
cw = boto3.client("cloudwatch", region_name=AWS_REGION)
checkpoint_table = dynamo.Table(DYNAMO_TABLE)


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def read_checkpoint() -> str | None:
    """Return the last persisted Last-Event-ID, or None if no checkpoint exists."""
    try:
        resp = checkpoint_table.get_item(Key={"stream_id": CHECKPOINT_KEY})
        item = resp.get("Item")
        if item:
            last_id = item.get("last_event_id")
            log.info(f"Resuming from checkpoint last_event_id={last_id}")
            return last_id
    except Exception as e:
        log.warning(f"Could not read checkpoint, starting from head: {e}")
    return None


def write_checkpoint(last_event_id: str) -> None:
    """Persist the last successfully flushed Last-Event-ID to DynamoDB."""
    try:
        checkpoint_table.put_item(Item={
            "stream_id": CHECKPOINT_KEY,
            "last_event_id": last_event_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as e:
        log.error(f"Failed to write checkpoint: {e}")


# ---------------------------------------------------------------------------
# S3 writer
# ---------------------------------------------------------------------------

def s3_key(dt: datetime) -> str:
    """Build the hive-partitioned S3 key for a given UTC datetime."""
    return (
        f"{S3_PREFIX}/"
        f"year={dt.year:04d}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"hour={dt.hour:02d}/"
        f"events-{uuid.uuid4()}.jsonl"
    )


def flush_to_s3(buffer: list[str], last_event_id: str) -> int:
    """Write the in-memory buffer to S3 as a JSONL file, then checkpoint.

    Returns the number of bytes written (0 if the buffer was empty).
    """
    if not buffer:
        return 0

    now = datetime.now(timezone.utc)
    key = s3_key(now)
    body = "\n".join(buffer) + "\n"
    byte_count = len(body.encode("utf-8"))

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))
        log.info(f"Flushed {len(buffer)} events ({byte_count} bytes) to s3://{S3_BUCKET}/{key}")
        write_checkpoint(last_event_id)
        return byte_count
    except Exception as e:
        log.error(f"S3 flush failed: {e}")
        raise


# ---------------------------------------------------------------------------
# CloudWatch custom metrics
# ---------------------------------------------------------------------------

def emit_metrics_batch(metrics: list[dict]) -> None:
    """Send multiple metrics in a SINGLE PutMetricData request.

    One API call can carry many MetricData entries, so batching keeps request
    volume far below the CloudWatch free tier. Each dict is
    {"MetricName": str, "Value": float, "Unit": str}.
    """
    if not metrics:
        return
    try:
        cw.put_metric_data(Namespace=CW_NAMESPACE, MetricData=metrics)
    except Exception as e:
        log.warning(f"CloudWatch metric emission failed: {e}")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run() -> None:
    backoff = 1
    last_event_id = read_checkpoint()

    while True:
        try:
            headers = {"User-Agent": USER_AGENT}
            if last_event_id:
                headers["Last-Event-ID"] = last_event_id

            log.info(f"Connecting to SSE stream (Last-Event-ID={last_event_id})")
            response = requests.get(SSE_URL, headers=headers, stream=True, timeout=60)
            response.raise_for_status()

            client = SSEClient(response)
            buffer: list[str] = []
            buffer_bytes = 0
            last_flush_time = time.monotonic()
            last_event_time = time.monotonic()
            last_emit_time = time.monotonic()
            events_since_last_emit = 0
            bytes_since_last_emit = 0

            for event in client.events():
                if not event.data or event.data.strip() == "":
                    continue

                # Track liveness
                last_event_time = time.monotonic()
                events_since_last_emit += 1

                # Capture last event ID for checkpointing
                if event.id:
                    last_event_id = event.id

                buffer.append(event.data)
                buffer_bytes += len(event.data.encode("utf-8"))

                now = time.monotonic()

                # Flush on time or size threshold
                if (now - last_flush_time) >= FLUSH_INTERVAL_SECS or buffer_bytes >= FLUSH_SIZE_BYTES:
                    written = flush_to_s3(buffer, last_event_id)
                    bytes_since_last_emit += written
                    buffer = []
                    buffer_bytes = 0
                    last_flush_time = time.monotonic()

                # Publish metrics once per METRIC_INTERVAL_SECS in a SINGLE
                # batched API call. The independent last_emit_time timer ensures
                # this fires once per interval, NOT on every event.
                emit_elapsed = now - last_emit_time
                if emit_elapsed >= METRIC_INTERVAL_SECS:
                    emit_metrics_batch([
                        {
                            "MetricName": "events_received_per_second",
                            "Value": events_since_last_emit / emit_elapsed,
                            "Unit": "Count/Second",
                        },
                        {
                            "MetricName": "bytes_written_per_minute",
                            "Value": bytes_since_last_emit,
                            "Unit": "Bytes",
                        },
                        {
                            "MetricName": "seconds_since_last_event",
                            "Value": now - last_event_time,
                            "Unit": "Seconds",
                        },
                    ])
                    last_emit_time = now
                    events_since_last_emit = 0
                    bytes_since_last_emit = 0

            # Stream ended cleanly — flush remainder
            if buffer:
                flush_to_s3(buffer, last_event_id)

            # Reset backoff on clean exit
            backoff = 1

        except Exception as e:
            log.error(f"Stream error: {e}. Reconnecting in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_CAP_SECS)


if __name__ == "__main__":
    run()
