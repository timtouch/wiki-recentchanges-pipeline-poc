# Databricks notebook source
# Bronze Layer — Raw Ingestion via Auto Loader
# Delta Live Tables pipeline notebook
# Target table: wiki_poc.bronze.recentchange_raw

import dlt
from pyspark.sql import functions as F

RAW_S3_PATH = "s3://wiki-raw-poc/recentchange/"


@dlt.table(
    name="recentchange_raw",
    comment="Bronze: raw Wikimedia recentchange events ingested from S3 via Auto Loader.",
    table_properties={
        # Silver will read this table via Change Data Feed
        "delta.enableChangeDataFeed": "true",
        # Coalesce small files on write — important for a high-frequency streaming source
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["ingest_date"],
)
def recentchange_raw():
    return (
        spark.readStream.format("cloudFiles")
        # --- Auto Loader options ---
        .option("cloudFiles.format", "json")
        # Directory-listing mode: no SNS/SQS needed at POC volume (~2 events/sec)
        .option("cloudFiles.useNotifications", "false")
        # Don't infer schema — Bronze captures everything faithfully.
        # Schema drift in the upstream feed should never break ingestion.
        .option("inferSchema", "false")
        # Hive-partitioned source path; Auto Loader will discover new
        # year=/month=/day=/hour= directories automatically.
        .option("cloudFiles.includeExistingFiles", "true")
        .load(RAW_S3_PATH)
        # --- Columns ---
        # Store the entire event as raw JSON string for full fidelity.
        # The actual JSON is what Auto Loader read into a single-column df
        # when inferSchema=false and the source is JSONL.
        .select(
            F.to_json(F.struct("*")).alias("raw_json"),
            F.current_timestamp().alias("ingest_timestamp"),
            F.input_file_name().alias("source_file"),
            # Pre-extract event_id now so Silver can dedup without re-parsing everything.
            # meta.id is the canonical unique event identifier in the recentchange stream.
            F.get_json_object(F.to_json(F.struct("*")), "$.meta.id").alias("event_id"),
            # Derive ingest_date for partitioning — cast to date so partition
            # pruning works cleanly in downstream queries.
            F.current_timestamp().cast("date").alias("ingest_date"),
        )
    )
