#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

UV_BIN="${UV_BIN:-uv}"
export UV_CACHE_DIR="${UV_CACHE_DIR_HOME:-${HOME}/.cache/uv}"
SPARK_PACKAGES="${SPARK_PACKAGES:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1}"

exec "${UV_BIN}" run --project "${PROJECT_ROOT}" spark-submit \
  --packages "${SPARK_PACKAGES}" \
  apps/stream_etl/stream_main.py \
  "$@"
