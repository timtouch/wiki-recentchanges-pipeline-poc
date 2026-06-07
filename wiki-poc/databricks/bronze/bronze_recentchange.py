# Databricks notebook source
# Bronze Layer — Raw Ingestion via Auto Loader
# Delta Live Tables pipeline notebook
# Target table: wiki_poc.bronze.bronze_recentchange_raw

import dlt
from pyspark.sql import functions as F

RAW_S3_PATH = "s3://wiki-raw-poc/recentchange/"


@dlt.table(
    name="bronze_recentchange_raw",
    comment="Bronze: raw Wikimedia recentchange events ingested from S3 via Auto Loader.",
    table_properties={
        # Silver will read this table via Change Data Feed
        "delta.enableChangeDataFeed": "true",
        # Coalesce small files on write — important for a high-frequency streaming source
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["ingest_date"],
)
def bronze_recentchange_raw():
    return (
        spark.readStream.format("cloudFiles")
        # --- Auto Loader options ---
        # Read each JSONL line verbatim as a string. This is the faithful-capture
        # pattern for Bronze: no JSON parsing, so the original payload is preserved
        # byte-for-byte and upstream schema drift can never break ingestion.
        .option("cloudFiles.format", "text")
        # Directory-listing mode: no SNS/SQS needed at POC volume (~2 events/sec)
        .option("cloudFiles.useNotifications", "false")
        # Pick up files already in the landing zone on first run.
        .option("cloudFiles.includeExistingFiles", "true")
        # Hive-partitioned source path; Auto Loader discovers new
        # year=/month=/day=/hour= directories automatically.
        .load(RAW_S3_PATH)
        # --- Columns ---
        .select(
            # `value` is the single column text format produces — one row per
            # JSONL line, holding the exact original JSON string the producer wrote.
            F.col("value").alias("raw_json"),
            F.current_timestamp().alias("ingest_timestamp"),
            # _metadata.file_path is the recommended way to capture source file
            # with Auto Loader (more reliable than input_file_name()).
            F.col("_metadata.file_path").alias("source_file"),
            # Pre-extract event_id from the raw string so Silver can dedup
            # without re-parsing everything. meta.id is the canonical unique ID.
            F.get_json_object(F.col("value"), "$.meta.id").alias("event_id"),
            # Derive ingest_date for partitioning — cast to date so partition
            # pruning works cleanly in downstream queries.
            F.current_timestamp().cast("date").alias("ingest_date"),
        )
    )
