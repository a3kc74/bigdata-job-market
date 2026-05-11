"""Phase 3 Spark Structured Streaming job: parse raw jobs and emit clean/DLQ."""

from __future__ import annotations

import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv()

from apps.stream_etl.sinks.elasticsearch_sink import write_jobs_realtime
from apps.stream_etl.sinks.kafka_sink import clean_jobs_to_kafka, dead_letter_to_kafka
from apps.stream_etl.transform import (
    build_clean_jobs,
    build_dead_letter,
    parse_raw_kafka,
    validate_raw_jobs,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "jobs_raw")
CLEAN_TOPIC = os.getenv("CLEAN_TOPIC", "jobs_clean")
DEAD_LETTER_TOPIC = os.getenv("DEAD_LETTER_TOPIC", "jobs_dead_letter")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/checkpoints/speed")
TRIGGER_SECONDS = os.getenv("TRIGGER_SECONDS", "10")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")
WRITE_ELASTICSEARCH = os.getenv("WRITE_ELASTICSEARCH", "true").lower() in {"1", "true", "yes"}
WRITE_CONSOLE_DEBUG = os.getenv("WRITE_CONSOLE_DEBUG", "false").lower() in {"1", "true", "yes"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("job-market-speed-phase3-clean-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    print(
        "[phase3] config "
        f"bootstrap={BOOTSTRAP} raw_topic={RAW_TOPIC} clean_topic={CLEAN_TOPIC} "
        f"dead_letter_topic={DEAD_LETTER_TOPIC} checkpoint_dir={CHECKPOINT_DIR} "
        f"starting_offsets={STARTING_OFFSETS} trigger_seconds={TRIGGER_SECONDS} "
        f"write_elasticsearch={WRITE_ELASTICSEARCH} write_console_debug={WRITE_CONSOLE_DEBUG}",
        flush=True,
    )

    raw_kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
    )

    validated_df = validate_raw_jobs(parse_raw_kafka(raw_kafka_df))
    clean_df = build_clean_jobs(validated_df).withWatermark("event_time", "60 minutes").dropDuplicates(
        ["job_id", "hash_content"]
    )
    dead_letter_df = build_dead_letter(validated_df)

    queries = [
        clean_jobs_to_kafka(clean_df)
        .writeStream.format("kafka")
        .queryName("phase3_jobs_clean_to_kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", CLEAN_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/jobs_clean")
        .outputMode("append")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start(),
        dead_letter_to_kafka(dead_letter_df)
        .writeStream.format("kafka")
        .queryName("phase3_jobs_dead_letter_to_kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", DEAD_LETTER_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/jobs_dead_letter")
        .outputMode("append")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start(),
    ]

    if WRITE_ELASTICSEARCH:
        queries.append(
            clean_df.writeStream.foreachBatch(write_jobs_realtime)
            .queryName("phase3_jobs_realtime_to_elasticsearch")
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/jobs_realtime_es")
            .outputMode("append")
            .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
            .start()
        )

    if WRITE_CONSOLE_DEBUG:
        queries.extend(
            [
                clean_df.select("job_id", "title", "city", "salary_bin")
                .writeStream.format("console")
                .queryName("phase3_jobs_clean_console_debug")
                .option("truncate", "false")
                .option("checkpointLocation", f"{CHECKPOINT_DIR}/jobs_clean_console_debug")
                .outputMode("append")
                .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
                .start(),
                dead_letter_df.select("dead_letter_key", "error_reason", "kafka_offset")
                .writeStream.format("console")
                .queryName("phase3_jobs_dead_letter_console_debug")
                .option("truncate", "false")
                .option("checkpointLocation", f"{CHECKPOINT_DIR}/jobs_dead_letter_console_debug")
                .outputMode("append")
                .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
                .start(),
            ]
        )

    spark.streams.awaitAnyTermination()
    for query in queries:
        query.stop()


if __name__ == "__main__":
    main()
