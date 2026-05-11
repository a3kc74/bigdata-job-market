# Speed Layer Phase 1 Implementation Log

Date: 2026-05-11

## Scope

Implemented Phase 1 from `Speed layer.md`: local infrastructure for the Speed Layer, Kafka topic bootstrap, Cassandra realtime schema, and a smoke test script.

## Files Added

- `infra/docker-compose/docker-compose.speed.yml`
  - Added local services for Kafka, Kafka UI, Spark master/worker, Elasticsearch, Kibana, Cassandra, Prometheus, and Grafana.
  - Added named volumes for broker data, Spark checkpoints, Elasticsearch, Cassandra, Prometheus, and Grafana.
  - Exposed local UI ports for quick demo access.

- `configs/prometheus-speed.yml`
  - Added a minimal Prometheus config so the Prometheus service can start.

- `scripts/create_kafka_topics.sh`
  - Creates the Phase 1 Kafka topics:
    - `jobs_raw`
    - `jobs_clean`
    - `jobs_dead_letter`
  - Applies 7-day retention to raw/clean topics and 14-day retention to the dead-letter topic.

- `scripts/create_kafka_topics.ps1`
  - Windows PowerShell equivalent of `scripts/create_kafka_topics.sh`.
  - Avoids relying on WSL or Git Bash when creating Kafka topics from Windows.

- `scripts/init_cassandra.cql`
  - Creates keyspace `job_market_speed`.
  - Creates realtime tables for:
    - jobs per 10 minutes
    - hourly skill counts
    - hourly top skills
    - hourly salary bins
    - realtime jobs by id
    - stream dead-letter records

- `scripts/init_cassandra.sh`
  - Convenience wrapper to apply `scripts/init_cassandra.cql` through the Cassandra container.

- `scripts/init_cassandra.ps1`
  - Windows PowerShell equivalent of `scripts/init_cassandra.sh`.

- `scripts/smoke_test_speed_layer.sh`
  - Checks that all Phase 1 services are running.
  - Verifies the three Kafka topics exist.
  - Verifies the Cassandra keyspace/tables exist.
  - Checks basic HTTP readiness for Elasticsearch, Kafka UI, and Prometheus.

- `scripts/smoke_test_speed_layer.ps1`
  - Windows PowerShell equivalent of `scripts/smoke_test_speed_layer.sh`.

## How To Run

```bash
docker compose -f infra/docker-compose/docker-compose.speed.yml up -d
bash scripts/create_kafka_topics.sh
bash scripts/init_cassandra.sh
bash scripts/smoke_test_speed_layer.sh
```

On Windows PowerShell, use:

```powershell
docker compose -f infra/docker-compose/docker-compose.speed.yml up -d
.\scripts\create_kafka_topics.ps1
.\scripts\init_cassandra.ps1
.\scripts\smoke_test_speed_layer.ps1
```

## Expected Result

- Kafka UI is available at `http://localhost:8088`.
- Spark master UI is available at `http://localhost:8080`.
- Elasticsearch is available at `http://localhost:9200`.
- Kibana is available at `http://localhost:5601`.
- Prometheus is available at `http://localhost:9090`.
- Grafana is available at `http://localhost:3000` with `admin/admin`.
- Kafka contains `jobs_raw`, `jobs_clean`, and `jobs_dead_letter`.
- Cassandra contains the realtime tables under `job_market_speed`.

## Notes

- Existing modified files in the working tree were left untouched.
- The compose file is local-dev oriented and uses single-node settings for Kafka, Cassandra, Elasticsearch, and Spark.
