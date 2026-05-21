#!/usr/bin/env bash
set -euo pipefail

export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export KAFKA_TOPIC_JOBS_RAW="${KAFKA_TOPIC_JOBS_RAW:-jobs_raw}"

TODAY="$(date +%F)"

echo "[SPEED] Run crawler"
uv run python -m apps.ingestion.run_crawler --mode speed

echo "[SPEED] Publish latest speed JSONL to Kafka"
LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=${TODAY}/jobs_speed_*.jsonl | head -n 1)"

uv run python -m apps.producer.file_to_kafka \
  --input "$LATEST_FILE" \
  --bootstrap-servers "$KAFKA_BOOTSTRAP_SERVERS" \
  --topic "$KAFKA_TOPIC_JOBS_RAW"