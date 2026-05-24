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
- **Elasticsearch indexing**: All aggregations written to Elasticsearch realtime indexes for dashboard/search serving
- **Checkpointing**: Per-query checkpoint paths for crash recovery
- **Idempotent sinks**: Deterministic document IDs for Elasticsearch upserts
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
│   │   │   ├── elasticsearch_sink.py
│   │   │   ├── jobs_per_10m_sink.py
│   │   │   ├── top_skills_hourly_sink.py
│   │   │   ├── salary_bins_realtime_sink.py
│   │   │   └── kafka_sink.py
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

## Prerequisites

| Tool | Required |
|------|----------|
| Docker | Yes |
| Docker Compose | Yes |
| Python 3.10+ | Yes |
| Java 11+ | For Spark |
| Apache Spark 3.5.x | For streaming |
| PySpark | `pip install pyspark` |
| confluent-kafka | `pip install confluent-kafka` |
| pytest | `pip install pytest` |
| python-dotenv | `pip install python-dotenv` |

## Quick Start

```bash
# 1. Setup environment
cp configs/.env.example .env

# 2. Setup infrastructure
bash scripts/setup_all.sh

# 3. Run the system
bash scripts/run_all.sh
```

## Run Components Separately

```bash
# Run crawler (single URL)
bash scripts/run_crawler.sh https://www.topcv.vn/viec-lam/example.html

# Run producer (file-based)
bash scripts/run_producer.sh file TV1_workspace/job_posting.json

# Run producer (live crawl)
bash scripts/run_producer.sh live --keyword "react native"

# Run Spark streaming
bash scripts/run_streaming.sh

# Run unit tests
bash scripts/run_tests.sh

# Run smoke test
bash scripts/smoke_test_pipeline.sh
```

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
| Checkpointing | Per-query checkpoint dirs under `/tmp/job-market-checkpoints/speed/` |
| Dead-letter topic | Malformed or invalid records → `jobs_dead_letter` |
| Watermark | 1-hour late-data tolerance on `event_ts` |
| Dedup | `dropDuplicates(["job_id"])` within watermark window |
| Realtime aggregations | Jobs per 10 min, top skills, salary bins via Spark Structured Streaming |
| Elasticsearch indexes | realtime_jobs_v1, realtime_job_counts_10m_v1, realtime_skill_counts_hourly_v1, realtime_top_skills_hourly_v1, realtime_salary_bins_hourly_v1 |

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
| Checkpoint conflicts | `bash scripts/clean_checkpoints.sh` |
| No messages in jobs_clean | Check producer logs, check DLQ topic |
