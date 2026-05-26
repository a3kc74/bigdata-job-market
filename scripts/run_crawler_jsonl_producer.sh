#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

UV_BIN="${UV_BIN:-uv}"
export UV_CACHE_DIR="${UV_CACHE_DIR_HOME:-${HOME}/.cache/uv}"

CRAWLER_OUTPUT_DIR="${CRAWLER_LOCAL_OUTPUT_DIR:-/tmp/topcv-crawler-output}"
INPUT="${CRAWLER_JSONL_INPUT:-${CRAWLER_OUTPUT_DIR}/source=topcv/ingest_date=*/jobs_speed_*.jsonl}"
TOPIC="${RAW_TOPIC:-jobs_raw}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
SLEEP_MS="${CRAWLER_JSONL_SLEEP_MS:-0}"
EVENT_TIME_MODE="${CRAWLER_JSONL_EVENT_TIME_MODE:-original}"
MAX_RECORDS="${CRAWLER_JSONL_MAX_RECORDS:-0}"
LOG_EVERY="${CRAWLER_JSONL_LOG_EVERY:-10}"
CHECKPOINT_FILE="${CRAWLER_JSONL_CHECKPOINT_FILE:-runtime/producer/crawler_jsonl_offsets.json}"
POLL_SECONDS="${CRAWLER_JSONL_POLL_SECONDS:-2}"
WATCH="${CRAWLER_JSONL_WATCH:-true}"

args=(
  "python"
  "-m" "apps.producer.crawler_jsonl_producer"
  "--input" "${INPUT}"
  "--topic" "${TOPIC}"
  "--bootstrap-servers" "${BOOTSTRAP_SERVERS}"
  "--sleep-ms" "${SLEEP_MS}"
  "--event-time-mode" "${EVENT_TIME_MODE}"
  "--max-records" "${MAX_RECORDS}"
  "--log-every" "${LOG_EVERY}"
  "--checkpoint-file" "${CHECKPOINT_FILE}"
  "--poll-seconds" "${POLL_SECONDS}"
)

if [[ "${WATCH}" =~ ^(1|true|yes)$ ]]; then
  args+=("--watch")
fi

args+=("$@")

exec "${UV_BIN}" run --project "${PROJECT_ROOT}" "${args[@]}"
