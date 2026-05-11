#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-infra/docker-compose/docker-compose.speed.yml}"
CQL_FILE="${CQL_FILE:-scripts/init_cassandra.cql}"

docker compose -f "${COMPOSE_FILE}" exec -T cassandra cqlsh < "${CQL_FILE}"
docker compose -f "${COMPOSE_FILE}" exec -T cassandra cqlsh \
  -e "DESCRIBE KEYSPACE job_market_speed;"
