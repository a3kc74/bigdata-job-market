# Big Data Job Market — Lambda Architecture

This project analyzes IT job postings from Vietnamese job boards using a **Lambda Architecture** with batch, speed, and serving layers.

## What Was Implemented

### Speed Layer (Spark Structured Streaming)

- **Kafka topics**: `jobs_raw`, `jobs_clean`, `jobs_dead_letter`
- **Producer**: File-based producer reads crawler JSON output → `jobs_raw`
- **Spark Structured Streaming**: Reads `jobs_raw`, parses raw schema, normalizes fields
- **Dead-letter handling**: Malformed JSON and missing required fields → `jobs_dead_letter`
- **Watermarking**: 1-hour event-time watermark for late data
- **Deduplication**: By `job_id` within the watermark window
- **Realtime aggregations**:
  - Jobs per 10 minutes (by source, province)
  - Top skills by hour (ranked top 10)
  - Salary bins per 10 minutes (by province)
- **Checkpointing**: Per-query checkpoint paths for crash recovery
- **Idempotent sinks**: Deterministic keys for Cassandra and Elasticsearch upserts
- **Scripts**: Setup, run, stop, test, smoke test, and cleanup scripts
- **Unit tests**: Salary parser, skill normalizer, event time fallback, validation

### Design Explanation

The speed layer is designed as the realtime part of a Lambda Architecture. The existing crawler remains unchanged and is treated as a legacy data source. A Kafka producer reads crawler output and publishes raw job events to the `jobs_raw` topic using `job_id` as the Kafka key. Spark Structured Streaming consumes `jobs_raw`, parses the raw JSON schema, normalizes event time, salary, location, experience, and skills, and separates invalid records into the `jobs_dead_letter` topic. Valid records are written to `jobs_clean`. The stream then applies event-time watermarking and deduplication by `job_id` before computing realtime aggregations: jobs per 10 minutes, top skills by hour, and salary bins. Checkpoints provide recovery, while deterministic sink keys and upserts make external writes idempotent.

## Repository Structure

```text
.
├── README.md
├── configs/
│   ├── .env.example          # Environment variable template
│   ├── streaming.dev.yaml    # Spark streaming config
│   ├── kafka.dev.yaml        # Kafka topic definitions
│   └── app.dev.yaml          # App-level config
├── infra/
│   ├── docker-compose/
│   │   └── docker-compose.dev.yml
│   ├── compose/
│   │   └── docker-compose.yml  (legacy)
│   └── kafka/
├── apps/
│   ├── producer/
│   │   ├── kafka_job_producer.py       # Live crawl → Kafka
│   │   └── crawler_output_producer.py  # File → Kafka
│   ├── stream_etl/
│   │   ├── stream_main.py             # Main streaming pipeline
│   │   ├── schemas/
│   │   │   ├── job_raw_schema.py      # Raw JSON PySpark schema
│   │   │   └── job_clean_schema.py    # Clean output fields
│   │   ├── transforms/
│   │   │   ├── normalize_event_time.py
│   │   │   ├── normalize_salary.py
│   │   │   ├── normalize_skills.py
│   │   │   ├── normalize_location.py
│   │   │   └── validate_job.py
│   │   ├── aggregations/
│   │   │   ├── jobs_per_10m.py
│   │   │   ├── top_skills_hourly.py
│   │   │   └── salary_bins.py
│   │   ├── sinks/
│   │   │   ├── kafka_sink.py
│   │   │   ├── dead_letter_sink.py
│   │   │   ├── cassandra_sink.py
│   │   │   └── elasticsearch_sink.py
│   │   └── tests/
│   │       ├── test_salary_parser.py
│   │       ├── test_skill_normalizer.py
│   │       ├── test_event_time.py
│   │       └── test_validation.py
│   ├── stream/                # Legacy stream code
│   └── ingestion/             # Legacy ingestion code
├── scripts/
│   ├── setup_all.sh
│   ├── run_all.sh
│   ├── stop_all.sh
│   ├── create_kafka_topics.sh
│   ├── run_crawler.sh
│   ├── run_producer.sh
│   ├── run_streaming.sh
│   ├── run_batch.sh
│   ├── run_tests.sh
│   ├── smoke_test_pipeline.sh
│   └── clean_checkpoints.sh
├── docs/
│   ├── speed_layer_design.md
│   └── runbook.md
└── TV1_workspace/
    ├── crawler.py             # Original crawler (NOT modified)
    └── job_posting.json       # Sample data
```

Current speed-layer scripts are available in both Linux/macOS Bash and Windows PowerShell forms:
`create_kafka_topics`, `init_cassandra`, `run_stream_speed_layer`, `run_fake_crawler`,
`smoke_test_speed_layer`, `clean_stream_etl_checkpoints`, and `configure_uv_cache`.

## Prerequisites

| Tool | Required |
|------|----------|
| Docker | Yes |
| Docker Compose | Yes |
| Python 3.10+ | Yes |
| Java 11+ | For Spark |
| Apache Spark 3.5.x | For streaming |
| Hadoop `winutils.exe` | Windows only, required for local Spark mode |
| PySpark | `pip install pyspark` |
| confluent-kafka | `pip install confluent-kafka` |
| pytest | `pip install pytest` |
| python-dotenv | `pip install python-dotenv` |
| cassandra-driver | Optional, for Cassandra sinks |

## Run Speed Layer

### Linux/macOS Bash

```bash
# 1. Start speed-layer infrastructure
docker compose -f infra/docker-compose/docker-compose.speed.yml up -d

# 2. Create required Kafka topics: jobs_raw, jobs_clean, jobs_dead_letter
bash scripts/create_kafka_topics.sh

# 3. Initialize Cassandra keyspace and realtime tables
bash scripts/init_cassandra.sh

# 4. Run Spark Structured Streaming
bash scripts/run_stream_speed_layer.sh

# 5. In another terminal, publish sample crawler records to jobs_raw
bash scripts/run_fake_crawler.sh

# 6. Verify speed-layer services, Kafka topics, Cassandra tables, and UIs
bash scripts/smoke_test_speed_layer.sh
```

### Windows PowerShell

```powershell
# 1. Start speed-layer infrastructure
docker compose -f infra/docker-compose/docker-compose.speed.yml up -d

# 2. Create required Kafka topics: jobs_raw, jobs_clean, jobs_dead_letter
.\scripts\create_kafka_topics.ps1

# 3. Initialize Cassandra keyspace and realtime tables
.\scripts\init_cassandra.ps1

# 4. Run Spark Structured Streaming
.\scripts\run_stream_speed_layer.ps1

# 5. In another PowerShell window, publish sample crawler records to jobs_raw
.\scripts\run_fake_crawler.ps1

# 6. Verify speed-layer services, Kafka topics, Cassandra tables, and UIs
.\scripts\smoke_test_speed_layer.ps1
```

On Windows, `run_stream_speed_layer.ps1` supports three modes:

```powershell
# Auto mode: uses local Spark if HADOOP_HOME/bin/winutils.exe exists,
# otherwise runs Spark inside the Docker spark-master service.
.\scripts\run_stream_speed_layer.ps1

# Force Docker mode. This avoids Windows HADOOP_HOME / winutils issues.
.\scripts\run_stream_speed_layer.ps1 -RunMode docker

# Force local Spark mode. Requires Java, PySpark, and HADOOP_HOME/bin/winutils.exe.
.\scripts\run_stream_speed_layer.ps1 -RunMode local
```

Docker mode requires the speed-layer Compose services to be running. On a new machine, the first Docker-mode run may need internet access to install Python packages in the Spark container and download Spark's Kafka connector package.

For local Spark mode on Windows, install a Hadoop `winutils.exe` package that matches Spark's Hadoop 3.x runtime, place it under a directory such as `C:\hadoop\bin\winutils.exe`, then set:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"
```

`HADOOP_HOME` must be an absolute Windows path with the slash after the drive letter, such as `C:\hadoop`. Do not use `C:hadoop`, `C:HADOOP~1.6`, or a path that points directly to `bin`.

To persist it for future PowerShell sessions:

```powershell
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
[Environment]::SetEnvironmentVariable("Path", "C:\hadoop\bin;" + [Environment]::GetEnvironmentVariable("Path", "User"), "User")
```

Optional checks:

```bash
# Run unit tests
uv run pytest apps/stream_etl/tests -v

# Consume clean jobs
docker compose -f infra/docker-compose/docker-compose.speed.yml exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic jobs_clean --from-beginning

# Consume dead-letter records
docker compose -f infra/docker-compose/docker-compose.speed.yml exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic jobs_dead_letter --from-beginning

# Clean stream_etl checkpoints
bash scripts/clean_stream_etl_checkpoints.sh
```

PowerShell equivalent:

```powershell
# Run unit tests
uv run pytest apps/stream_etl/tests -v

# Consume clean jobs
docker compose -f infra/docker-compose/docker-compose.speed.yml exec kafka kafka-console-consumer `
  --bootstrap-server kafka:29092 --topic jobs_clean --from-beginning

# Consume dead-letter records
docker compose -f infra/docker-compose/docker-compose.speed.yml exec kafka kafka-console-consumer `
  --bootstrap-server kafka:29092 --topic jobs_dead_letter --from-beginning

# Clean stream_etl checkpoints
.\scripts\clean_stream_etl_checkpoints.ps1
```

### Clean Stream Checkpoints

Use this when Spark Structured Streaming refuses to restart because checkpoint metadata no longer matches the current query plan. Stop the streaming job first, then run the matching command for your shell.

```bash
bash scripts/clean_stream_etl_checkpoints.sh
```

```powershell
.\scripts\clean_stream_etl_checkpoints.ps1
```

PowerShell supports explicit checkpoint targets:

```powershell
# Clean local Windows checkpoints used by -RunMode local
.\scripts\clean_stream_etl_checkpoints.ps1 -RunMode local

# Clean Docker checkpoints used by -RunMode docker
.\scripts\clean_stream_etl_checkpoints.ps1 -RunMode docker

# Clean both local and Docker checkpoints
.\scripts\clean_stream_etl_checkpoints.ps1
```

Local mode stores checkpoints under `.checkpoints/speed` in this repository unless `CHECKPOINT_DIR` is set. Docker mode stores checkpoints under `/checkpoints/speed` inside the Spark containers. Override Docker `CHECKPOINT_DIR` only for another path under `/checkpoints`.

### Clean Kafka Data

Use this when you want to delete all messages from Kafka and run the speed layer again from an empty Kafka state. Stop the streaming job and producers first, then delete and recreate the three speed-layer topics.

```bash
docker compose -f infra/docker-compose/docker-compose.speed.yml exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 --delete --topic jobs_raw
docker compose -f infra/docker-compose/docker-compose.speed.yml exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 --delete --topic jobs_clean
docker compose -f infra/docker-compose/docker-compose.speed.yml exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 --delete --topic jobs_dead_letter

bash scripts/create_kafka_topics.sh
```

```powershell
docker compose -f infra/docker-compose/docker-compose.speed.yml exec -T kafka kafka-topics `
  --bootstrap-server kafka:29092 --delete --topic jobs_raw
docker compose -f infra/docker-compose/docker-compose.speed.yml exec -T kafka kafka-topics `
  --bootstrap-server kafka:29092 --delete --topic jobs_clean
docker compose -f infra/docker-compose/docker-compose.speed.yml exec -T kafka kafka-topics `
  --bootstrap-server kafka:29092 --delete --topic jobs_dead_letter

.\scripts\create_kafka_topics.ps1
```

For a full fresh speed-layer rerun, clean Kafka data and stream checkpoints before starting `run_stream_speed_layer`.
If topic recreation fails because a topic is still marked for deletion, wait a few seconds and rerun the create-topics command.

## Deployed Ports

| Service | Host Port | URL / Endpoint | Purpose |
|---------|-----------|----------------|---------|
| Zookeeper | `2181` | `localhost:2181` | Kafka coordination |
| Kafka | `9092` | `localhost:9092` | Kafka broker for local producers/consumers |
| Kafka internal listener | `29092` | `kafka:29092` | Broker endpoint inside Docker network |
| Kafka UI | `8088` | `http://localhost:8088` | Kafka topics and messages UI |
| Spark master | `7077` | `spark://localhost:7077` | Spark cluster master endpoint |
| Spark master UI | `8080` | `http://localhost:8080` | Spark master web UI |
| Spark worker UI | `8081` | `http://localhost:8081` | Spark worker web UI |
| Elasticsearch | `9200` | `http://localhost:9200` | Elasticsearch API |
| Kibana | `5601` | `http://localhost:5601` | Elasticsearch/Kibana UI |
| Cassandra | `9042` | `localhost:9042` | CQL endpoint |
| Prometheus | `9090` | `http://localhost:9090` | Metrics and Prometheus UI |
| Grafana | `3000` | `http://localhost:3000` | Metrics dashboards, default login `admin` / `admin` |

## Kafka Topics

| Topic | Purpose | Key |
|-------|---------|-----|
| `jobs_raw` | Raw job postings from crawler | `job_id` |
| `jobs_clean` | Validated, normalized postings | `job_id` |
| `jobs_dead_letter` | Failed/invalid records | original key |

### Consumer Commands

```bash
# Consume clean jobs
docker exec speed-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic jobs_clean --from-beginning

# Consume dead-letter
docker exec speed-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic jobs_dead_letter --from-beginning
```

## Data Contracts

### Raw Schema (jobs_raw)
See [job_raw_schema.py](apps/stream_etl/schemas/job_raw_schema.py) — nested JSON with `source`, `job_id`, `payload` (title, salary, skills, etc.), and `quality_flags`.

### Clean Schema (jobs_clean)
See [job_clean_schema.py](apps/stream_etl/schemas/job_clean_schema.py) — flat record with normalized fields: `salary_min_vnd`, `salary_bin`, `skills[]`, `province`, `event_ts`, `quality_flags`.

## Fault Tolerance

| Mechanism | Description |
|-----------|-------------|
| Checkpointing | Per-query checkpoint dirs under `/checkpoints/speed` in the Spark containers |
| Dead-letter topic | Malformed or invalid records → `jobs_dead_letter` |
| Watermark | 1-hour late-data tolerance on `event_ts` |
| Dedup | `dropDuplicates(["job_id"])` within watermark window |
| Idempotent writes | Deterministic sink keys in Cassandra + Elasticsearch |

## Testing

```bash
# Run all unit tests
bash scripts/run_tests.sh

# Run specific test
pytest apps/stream_etl/tests/test_salary_parser.py -v

# Run smoke test (requires running infrastructure)
bash scripts/smoke_test_pipeline.sh
```

## Troubleshooting

See [docs/runbook.md](docs/runbook.md) for detailed troubleshooting.

| Issue | Quick Fix |
|-------|-----------|
| Kafka not reachable | `docker ps` — ensure speed-kafka is running |
| Spark Kafka package missing | Use `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` |
| Windows `HADOOP_HOME and hadoop.home.dir are unset` | Run `.\scripts\run_stream_speed_layer.ps1 -RunMode docker`, or install `winutils.exe` and set `HADOOP_HOME` for local mode |
| Kafka `OffsetOutOfRangeException` or `offset was changed` | Kafka data was deleted/recreated while Spark kept old offsets; stop streaming, clean checkpoints, then restart streaming |
| Checkpoint conflicts | Bash: `bash scripts/clean_stream_etl_checkpoints.sh`; PowerShell: `.\scripts\clean_stream_etl_checkpoints.ps1` |
| Rerun from empty Kafka topics | Delete and recreate `jobs_raw`, `jobs_clean`, and `jobs_dead_letter` from the Clean Kafka Data section |
| No messages in jobs_clean | Check producer logs, check DLQ topic |
