#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-infra/docker-compose/docker-compose.speed.yml}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"

require_service() {
  local service="$1"
  local status

  status="$(docker compose -f "${COMPOSE_FILE}" ps --status running --services | grep -x "${service}" || true)"
  if [[ -z "${status}" ]]; then
    echo "Service is not running: ${service}" >&2
    return 1
  fi
}

require_topic() {
  local topic="$1"

  docker compose -f "${COMPOSE_FILE}" exec -T kafka kafka-topics \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --describe \
    --topic "${topic}" >/dev/null
}

require_cassandra_table() {
  local table="$1"

  docker compose -f "${COMPOSE_FILE}" exec -T cassandra cqlsh \
    -e "DESCRIBE TABLE job_market_speed.${table};" >/dev/null
}

for service in kafka kafka-ui spark-master spark-worker elasticsearch kibana cassandra prometheus grafana; do
  require_service "${service}"
done

for topic in jobs_raw jobs_clean jobs_dead_letter; do
  require_topic "${topic}"
done

for table in \
  realtime_job_counts_10m \
  realtime_skill_counts_hourly \
  realtime_top_skills_hourly \
  realtime_salary_bins_hourly \
  jobs_realtime_by_id \
  stream_dead_letter_by_day; do
  require_cassandra_table "${table}"
done

curl -fsS http://localhost:9200 >/dev/null
curl -fsS http://localhost:8088 >/dev/null
curl -fsS http://localhost:9090/-/ready >/dev/null

echo "Speed Layer phase 1 smoke test passed."
