#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

UV_BIN="${UV_BIN:-uv}"
export UV_CACHE_DIR="${UV_CACHE_DIR_HOME:-${HOME}/.cache/uv}"

mode="${CRAWLER_MODE:-speed}"
args=(
  "python"
  "-m" "apps.ingestion.run_crawler"
  "--mode" "${mode}"
)

if [[ -n "${CRAWLER_MAX_PAGES:-}" ]]; then
  args+=("--max-pages" "${CRAWLER_MAX_PAGES}")
fi
if [[ -n "${CRAWLER_UPDATED_WITHIN_MINUTES:-}" ]]; then
  args+=("--updated-within-minutes" "${CRAWLER_UPDATED_WITHIN_MINUTES}")
fi
if [[ -n "${CRAWLER_DETAIL_BATCH_SIZE:-}" ]]; then
  args+=("--detail-batch-size" "${CRAWLER_DETAIL_BATCH_SIZE}")
fi
if [[ -n "${CRAWLER_LIST_PAGES_PER_CHUNK:-}" ]]; then
  args+=("--list-pages-per-chunk" "${CRAWLER_LIST_PAGES_PER_CHUNK}")
fi
if [[ -n "${CRAWLER_PROCESSED_TTL_DAYS:-}" ]]; then
  args+=("--processed-ttl-days" "${CRAWLER_PROCESSED_TTL_DAYS}")
fi
if [[ "${CRAWLER_DEBUG_CARD_LINKS:-}" =~ ^(1|true|yes)$ ]]; then
  args+=("--debug-card-links")
fi

args+=("$@")

exec "${UV_BIN}" run --project "${PROJECT_ROOT}" "${args[@]}"

