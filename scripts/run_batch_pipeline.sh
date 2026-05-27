#!/usr/bin/env bash
set -euo pipefail

mkdir -p runtime/logs runtime/crawler

TODAY="$(date +%F)"
LOG_FILE="runtime/logs/batch_pipeline_${TODAY}.log"
LOCK_FILE="runtime/crawler/batch_pipeline.lock"

{
  echo "======================================================="
  echo "[BATCH] Start at $(date)"
  echo "======================================================="

  exec 9>"$LOCK_FILE"

  if ! flock -n 9; then
    echo "[BATCH] Another batch/resume process is still running. Exit."
    exit 0
  fi

  BATCH_DAYS="${BATCH_DAYS:-7}"
  BATCH_MAX_PAGES="${BATCH_MAX_PAGES:-0}"

  echo "[BATCH] days=${BATCH_DAYS}"
  echo "[BATCH] max_pages=${BATCH_MAX_PAGES}"

  echo "[BATCH] Run crawler batch"

  uv run python -m apps.ingestion.run_crawler \
    --mode batch \
    --days "$BATCH_DAYS" \
    --max-pages "$BATCH_MAX_PAGES"

  echo "[BATCH] Find latest batch JSONL"

  LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=${TODAY}/jobs_batch_*.jsonl 2>/dev/null | head -n 1 || true)"

  if [ -z "$LATEST_FILE" ]; then
    echo "[BATCH] ERROR: No batch JSONL file found for ${TODAY}."
    exit 1
  fi

  echo "[BATCH] Latest file: $LATEST_FILE"

  echo "[BATCH] Upload to HDFS"

  bash scripts/upload_to_hdfs.sh "$LATEST_FILE" "$TODAY"

  echo "[BATCH] Done at $(date)"
  echo "======================================================="
} >> "$LOG_FILE" 2>&1