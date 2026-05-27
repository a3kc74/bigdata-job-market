#!/usr/bin/env bash
set -euo pipefail

mkdir -p runtime/logs runtime/crawler

TODAY="$(date +%F)"
LOG_FILE="runtime/logs/batch_resume_${TODAY}.log"
LOCK_FILE="runtime/crawler/batch_pipeline.lock"
CHECKPOINT_FILE="runtime/crawler/batch_checkpoint.json"

{
  echo "======================================================="
  echo "[RESUME] Check at $(date)"
  echo "======================================================="

  exec 9>"$LOCK_FILE"

  if ! flock -n 9; then
    echo "[RESUME] Batch is still running. Skip resume."
    exit 0
  fi

  if [ ! -f "$CHECKPOINT_FILE" ]; then
    echo "[RESUME] No checkpoint. Nothing to resume."
    exit 0
  fi

  if grep -q '"completed": true' "$CHECKPOINT_FILE"; then
    echo "[RESUME] Checkpoint completed. Nothing to resume."
    exit 0
  fi

  echo "[RESUME] Incomplete checkpoint found. Resume batch."

  uv run python -m apps.ingestion.run_crawler \
    --mode batch \
    --resume

  echo "[RESUME] Find latest batch JSONL for today"

  LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=${TODAY}/jobs_batch_*.jsonl 2>/dev/null | head -n 1 || true)"

  if [ -z "$LATEST_FILE" ]; then
    echo "[RESUME] ERROR: No batch JSONL file found for ${TODAY}."
    exit 1
  fi

  echo "[RESUME] Latest file: $LATEST_FILE"

  echo "[RESUME] Upload to HDFS"

  bash scripts/upload_to_hdfs.sh "$LATEST_FILE" "$TODAY"

  echo "[RESUME] Done at $(date)"
  echo "======================================================="
} >> "$LOG_FILE" 2>&1