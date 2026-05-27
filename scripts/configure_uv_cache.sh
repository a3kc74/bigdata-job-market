#!/usr/bin/env bash
set -euo pipefail

UV_CACHE_DIR_HOME="${UV_CACHE_DIR_HOME:-${HOME}/.cache/uv}"
mkdir -p "${UV_CACHE_DIR_HOME}"

cat <<EOF
Use this cache path for uv:

  export UV_CACHE_DIR="${UV_CACHE_DIR_HOME}"

Add that line to your shell profile to make it persistent.
EOF
