#!/usr/bin/env bash
set -euo pipefail

mkdir -p runtime/crawler logs

LOCK_FILE="runtime/crawler/batch_pipeline.lock"

exec 9>"$LOCK_FILE"

if ! flock -n 9; then
  echo "[BATCH] Another batch/resume process is still running. Exit."
  exit 0
fi

TODAY="$(date +%F)"

echo "[BATCH] Start crawler batch"

uv run python -m apps.ingestion.run_crawler --mode batch

echo "[BATCH] Find latest batch output"

LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=${TODAY}/jobs_batch_*.jsonl | head -n 1)"

echo "[BATCH] Latest file: $LATEST_FILE"

echo "[BATCH] Upload to HDFS"

scripts/upload_jobs_to_hdfs.sh "$LATEST_FILE" "$TODAY"

echo "[BATCH] Done"