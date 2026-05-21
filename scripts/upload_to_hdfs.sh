#!/usr/bin/env bash
set -euo pipefail

LOCAL_FILE="${1:-}"
INGEST_DATE="${2:-}"

if [ -z "$LOCAL_FILE" ]; then
  echo "Usage: scripts/upload_jobs_to_hdfs.sh <local_jsonl_file> <ingest_date>"
  exit 1
fi

if [ -z "$INGEST_DATE" ]; then
  INGEST_DATE="$(date +%F)"
fi

HDFS_DIR="/raw/jobs/source=topcv/ingest_date=${INGEST_DATE}"
HDFS_FILE="${HDFS_DIR}/$(basename "$LOCAL_FILE")"

echo "[HDFS] Local file: $LOCAL_FILE"
echo "[HDFS] Target dir: $HDFS_DIR"

hdfs dfs -mkdir -p "$HDFS_DIR"
hdfs dfs -put -f "$LOCAL_FILE" "$HDFS_FILE"

echo "[HDFS] Uploaded to: $HDFS_FILE"