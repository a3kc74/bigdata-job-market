#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-infra/docker-compose/docker-compose.speed.yml}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"
RETENTION_7D_MS="${RETENTION_7D_MS:-604800000}"
RETENTION_14D_MS="${RETENTION_14D_MS:-1209600000}"

create_topic() {
  local topic="$1"
  local partitions="$2"
  local retention_ms="$3"

  docker compose -f "${COMPOSE_FILE}" exec -T kafka kafka-topics \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config "retention.ms=${retention_ms}"
}

create_topic "jobs_raw" 3 "${RETENTION_7D_MS}"
create_topic "jobs_clean" 3 "${RETENTION_7D_MS}"
create_topic "jobs_dead_letter" 1 "${RETENTION_14D_MS}"

docker compose -f "${COMPOSE_FILE}" exec -T kafka kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --list
