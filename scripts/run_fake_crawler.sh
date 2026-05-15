#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

UV_BIN="${UV_BIN:-uv}"
export UV_CACHE_DIR="${UV_CACHE_DIR_HOME:-${HOME}/.cache/uv}"

INPUT="${FAKE_CRAWLER_INPUT:-data/raw/raw_jobs_batch.jsonl}"
TOPIC="${RAW_TOPIC:-jobs_raw}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
SLEEP_MS="${FAKE_CRAWLER_SLEEP_MS:-1000}"
EVENT_TIME_MODE="${FAKE_CRAWLER_EVENT_TIME_MODE:-now}"
MAX_RECORDS="${FAKE_CRAWLER_MAX_RECORDS:-0}"
LOG_EVERY="${FAKE_CRAWLER_LOG_EVERY:-10}"

args=(
  "python"
  "apps/producer/fake_crawler_producer.py"
  "--input" "${INPUT}"
  "--topic" "${TOPIC}"
  "--bootstrap-servers" "${BOOTSTRAP_SERVERS}"
  "--sleep-ms" "${SLEEP_MS}"
  "--event-time-mode" "${EVENT_TIME_MODE}"
  "--max-records" "${MAX_RECORDS}"
  "--log-every" "${LOG_EVERY}"
)

if [[ "${FAKE_CRAWLER_LOOP:-}" =~ ^(1|true|yes)$ ]]; then
  args+=("--loop")
fi

args+=("$@")

exec "${UV_BIN}" run --project "${PROJECT_ROOT}" "${args[@]}"
