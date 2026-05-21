#!/usr/bin/env bash
set -euo pipefail

mkdir -p runtime/logs

LOCAL_FILE="${1:-}"
INGEST_DATE="${2:-$(date +%F)}"
LOG_FILE="runtime/logs/hdfs_upload_${INGEST_DATE}.log"

HDFS_MODE="${HDFS_MODE:-k8s}"
HDFS_NAMESPACE="${HDFS_NAMESPACE:-hdfs}"
HDFS_NAMENODE_POD="${HDFS_NAMENODE_POD:-hdfs-namenode-0}"
HDFS_BASE_DIR="${HDFS_BASE_DIR:-/raw/jobs/source=topcv}"

{
  echo "======================================================="
  echo "[HDFS] Start upload at $(date)"
  echo "======================================================="

  echo "[HDFS] Local file: $LOCAL_FILE"
  echo "[HDFS] Ingest date: $INGEST_DATE"
  echo "[HDFS] HDFS mode: $HDFS_MODE"
  echo "[HDFS] HDFS base dir: $HDFS_BASE_DIR"

  if [ -z "$LOCAL_FILE" ]; then
    echo "[HDFS] ERROR: missing local file"
    echo "Usage: scripts/upload_to_hdfs.sh <local_jsonl_file> <ingest_date>"
    exit 1
  fi

  if [ ! -f "$LOCAL_FILE" ]; then
    echo "[HDFS] ERROR: file not found: $LOCAL_FILE"
    exit 1
  fi

  HDFS_DIR="${HDFS_BASE_DIR}/ingest_date=${INGEST_DATE}"
  FILENAME="$(basename "$LOCAL_FILE")"

  echo "[HDFS] Target HDFS dir: $HDFS_DIR"
  echo "[HDFS] Filename: $FILENAME"

  if [ "$HDFS_MODE" = "direct" ]; then
    echo "[HDFS] Using direct hdfs dfs command"

    hdfs dfs -mkdir -p "$HDFS_DIR"
    hdfs dfs -put -f "$LOCAL_FILE" "$HDFS_DIR/"
    hdfs dfs -ls "$HDFS_DIR"

  elif [ "$HDFS_MODE" = "k8s" ]; then
    echo "[HDFS] Using Kubernetes NameNode pod"
    echo "[HDFS] Namespace: $HDFS_NAMESPACE"
    echo "[HDFS] NameNode pod: $HDFS_NAMENODE_POD"

    kubectl cp "$LOCAL_FILE" "${HDFS_NAMESPACE}/${HDFS_NAMENODE_POD}:/tmp/${FILENAME}"

    kubectl exec -n "$HDFS_NAMESPACE" "$HDFS_NAMENODE_POD" -- \
      hdfs dfs -mkdir -p "$HDFS_DIR"

    kubectl exec -n "$HDFS_NAMESPACE" "$HDFS_NAMENODE_POD" -- \
      hdfs dfs -put -f "/tmp/${FILENAME}" "$HDFS_DIR/"

    kubectl exec -n "$HDFS_NAMESPACE" "$HDFS_NAMENODE_POD" -- \
      hdfs dfs -ls "$HDFS_DIR"

  else
    echo "[HDFS] ERROR: invalid HDFS_MODE=$HDFS_MODE"
    echo "[HDFS] Use HDFS_MODE=k8s or HDFS_MODE=direct"
    exit 1
  fi

  echo "[HDFS] Done at $(date)"
  echo "======================================================="
} >> "$LOG_FILE" 2>&1