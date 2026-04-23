"""
Spark Structured Streaming — Speed Layer main entrypoint.

Pipeline flow:
  1. RAW SOURCE: Read from Kafka topic `raw_events`
  2. VALIDATE:   Parse JSON with schema, attach Kafka metadata
  3. CLEAN:      Filter bad rows, parse timestamps, basic cleanup
  4. NORMALIZE:  Standardize business fields
  5. MAIN:       Apply watermark + dedup for realtime processing
  6. GOLD:       Window aggregates (job counts, skill counts)
  7. SINK:       Write gold to Redis speed views

Usage:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    apps/stream/stream_main.py
"""

import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Add project root to sys.path for proper absolute imports
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(os.path.dirname(_current_dir))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from apps.stream.aggregations import (build_job_counts_10m,
                                      build_skill_counts_30m)
from apps.stream.sinks import write_job_counts, write_skill_counts
from apps.stream.transform import (clean_events, normalize_events,
                                   validate_raw_events)
from shared.schemas import RAW_EVENT_SCHEMA

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw_events")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/speed_layer_checkpoints")
TRIGGER_SECONDS = os.getenv("TRIGGER_SECONDS", "10")


def main():
    spark = SparkSession.builder.appName("job-speed-layer-demo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── RAW SOURCE: read from Kafka ─────────────────────────────────────
    raw_kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # ── VALIDATE → CLEAN → NORMALIZE ────────────────────────────────────
    validated_df = validate_raw_events(raw_kafka_df, RAW_EVENT_SCHEMA)
    cleaned_df = clean_events(validated_df)
    normalized_df = normalize_events(cleaned_df)

    # ── MAIN STREAM: late-data policy + dedup ───────────────────────────
    main_stream_df = normalized_df.withWatermark(
        "event_ts", "30 minutes"
    ).dropDuplicates(  # ADDED: allow late events up to 30 minutes in stateful operations
        ["job_id"]
    )  # ADDED: deduplicate by job_id within the watermark horizon

    # ── GOLD: realtime window aggregates ────────────────────────────────
    gold_job_counts_df = build_job_counts_10m(main_stream_df)
    gold_skill_counts_df = build_skill_counts_30m(main_stream_df)

    # ── SINK: write gold to Redis ───────────────────────────────────────
    query_job_counts = (
        gold_job_counts_df.writeStream.outputMode("update")
        .foreachBatch(write_job_counts)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/job_counts")
        .start()
    )

    query_skill_counts = (
        gold_skill_counts_df.writeStream.outputMode("update")
        .foreachBatch(write_skill_counts)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/skill_counts")
        .start()
    )

    # ── DEBUG: inspect main stream rows in console ──────────────────────
    query_main_console = (
        main_stream_df.writeStream.outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/main_console")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
