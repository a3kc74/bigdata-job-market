#!/usr/bin/env bash
set -euo pipefail

mkdir -p runtime/crawler logs

LOCK_FILE="runtime/crawler/batch_pipeline.lock"
CHECKPOINT_FILE="runtime/crawler/batch_checkpoint.json"

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

uv run python -m apps.ingestion.run_crawler --mode batch --resume

TODAY="$(date +%F)"
LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=${TODAY}/jobs_batch_*.jsonl | head -n 1)"

scripts/upload_jobs_to_hdfs.sh "$LATEST_FILE" "$TODAY"

echo "[RESUME] Done"