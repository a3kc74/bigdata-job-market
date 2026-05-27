#!/usr/bin/env bash
set -euo pipefail

mkdir -p runtime/logs runtime/crawler

TODAY="$(date +%F)"
LOG_FILE="runtime/logs/speed_pipeline_${TODAY}.log"
LOCK_FILE="runtime/crawler/speed_pipeline.lock"

{
  echo "======================================================="
  echo "[SPEED] Start at $(date)"
  echo "======================================================="

  exec 9>"$LOCK_FILE"

  if ! flock -n 9; then
    echo "[SPEED] Another speed pipeline is still running. Skip this run."
    exit 0
  fi

  SPEED_MAX_PAGES="${SPEED_MAX_PAGES:-15}"
  SPEED_UPDATED_WITHIN_MINUTES="${SPEED_UPDATED_WITHIN_MINUTES:-30}"
  KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
  KAFKA_TOPIC_JOBS_RAW="${KAFKA_TOPIC_JOBS_RAW:-jobs_raw}"

  echo "[SPEED] max_pages=${SPEED_MAX_PAGES}"
  echo "[SPEED] updated_within_minutes=${SPEED_UPDATED_WITHIN_MINUTES}"
  echo "[SPEED] kafka_bootstrap_servers=${KAFKA_BOOTSTRAP_SERVERS}"
  echo "[SPEED] kafka_topic=${KAFKA_TOPIC_JOBS_RAW}"

  echo "[SPEED] Run crawler speed"

  uv run python -m apps.ingestion.run_crawler \
    --mode speed \
    --max-pages "$SPEED_MAX_PAGES" \
    --updated-within-minutes "$SPEED_UPDATED_WITHIN_MINUTES"

  echo "[SPEED] Find latest speed JSONL"

  LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=${TODAY}/jobs_speed_*.jsonl 2>/dev/null | head -n 1 || true)"

  if [ -z "$LATEST_FILE" ]; then
    echo "[SPEED] No speed JSONL file found for ${TODAY}. Nothing to send to Kafka."
    exit 0
  fi

  echo "[SPEED] Latest file: $LATEST_FILE"

  echo "[SPEED] Publish to Kafka"

  uv run python -m apps.producer.file_to_kafka \
    --input "$LATEST_FILE" \
    --bootstrap-servers "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$KAFKA_TOPIC_JOBS_RAW"

  echo "[SPEED] Done at $(date)"
  echo "======================================================="
} >> "$LOG_FILE" 2>&1