#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-infra/docker-compose/docker-compose.speed.yml}"
SPARK_SERVICE="${SPARK_SERVICE:-spark-master}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-/checkpoints/speed}"

case "${CHECKPOINT_DIR}" in
  /checkpoints/*) ;;
  *)
    echo "Refusing to clean checkpoint path outside /checkpoints: ${CHECKPOINT_DIR}" >&2
    exit 1
    ;;
esac

echo "Cleaning stream_etl checkpoints from ${SPARK_SERVICE}:${CHECKPOINT_DIR}"

docker compose -f "${COMPOSE_FILE}" exec -T -u root "${SPARK_SERVICE}" sh -lc \
  "if [ -d '${CHECKPOINT_DIR}' ]; then rm -rf '${CHECKPOINT_DIR}'/*; fi && mkdir -p '${CHECKPOINT_DIR}' && chmod -R 777 '${CHECKPOINT_DIR}'"

echo "stream_etl checkpoints cleaned."
