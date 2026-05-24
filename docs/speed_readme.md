# Big Data Job Market

Realtime job-market pipeline for TopCV data. The current working path is the
speed layer:

```text
TopCV crawler -> JSONL files -> JSONL producer -> Kafka jobs_raw
  -> Spark Structured Streaming -> jobs_clean / jobs_dead_letter / realtime sinks
```

## Current Speed Layer

Implemented components:

- TopCV crawler runner: `apps.ingestion.run_crawler`
- Raw JSONL output under `data/raw/jobs/source=topcv/ingest_date=YYYY-MM-DD/`
- Checkpointed JSONL producer: `apps.producer.crawler_jsonl_producer`
- Kafka topics: `jobs_raw`, `jobs_clean`, `jobs_dead_letter`
- Spark stream ETL: `apps.stream_etl.stream_main`
- Raw schema parsing: `apps/stream_etl/schemas/raw_job_schema.py`
- Validation and dead-letter handling for malformed or incomplete records
- Clean Kafka sink for normalized jobs
- Elasticsearch upsert sink for latest realtime jobs
- Realtime aggregations:
  - jobs per 10 minutes
  - skill counts and top skills by hour
  - salary bins by hour
- Spark checkpoints for stream recovery
- Producer checkpoints for JSONL file offsets
- Bash and PowerShell helper scripts

Important current status:

- Kafka, Spark, Elasticsearch, and Kibana are enabled in
  `infra/docker-compose/docker-compose.speed.yml`.
- Cassandra, Prometheus, and Grafana are present in compose but currently
  commented out.
- Aggregate sinks still try to write Cassandra when enabled. If Cassandra is
  not running, those writes fail in logs, while Elasticsearch writes can still
  work. Disable aggregate jobs or enable Cassandra depending on the run you need.

## Repository Map

```text
apps/
  ingestion/
    run_crawler.py              # CLI wrapper: speed/batch crawler modes
    topcv_crawler.py            # TopCV crawler implementation
  producer/
    crawler_jsonl_producer.py   # JSONL -> Kafka with file-offset checkpoint
    fake_crawler_producer.py    # sample replay producer
  stream_etl/
    stream_main.py              # Spark Structured Streaming entrypoint
    transform.py                # parse, validate, clean, DLQ transforms
    normalizers.py              # salary, location, skills, event-time helpers
    schemas/raw_job_schema.py
    stateful_jobs/
    sinks/
infra/docker-compose/
  docker-compose.speed.yml
scripts/
  run_topcv_crawler.*
  run_crawler_jsonl_producer.*
  run_stream_speed_layer.*
  create_kafka_topics.*
  clean_stream_etl_checkpoints.*
  smoke_test_speed_layer.*
tests/
```

## Requirements

- Docker and Docker Compose
- Python 3.11+
- `uv`
- Java/Spark only if running Spark locally
- On Windows local Spark mode: Hadoop `winutils.exe`
- Google Chrome or Microsoft Edge for crawler cookie recovery through `nodriver`

Recommended on Windows: run Spark through Docker mode unless local Hadoop/Spark
is already configured.

## Start Infrastructure

Bash:

```bash
docker compose -f infra/docker-compose/docker-compose.speed.yml up -d
bash scripts/create_kafka_topics.sh
```

PowerShell:

```powershell
docker compose -f infra/docker-compose/docker-compose.speed.yml up -d
.\scripts\create_kafka_topics.ps1
```

Useful UIs:

| Service | URL |
|---|---|
| Kafka UI | `http://localhost:8088` |
| Spark master | `http://localhost:8080` |
| Spark worker | `http://localhost:8081` |
| Elasticsearch | `http://localhost:9200` |
| Kibana | `http://localhost:5601` |

Kafka endpoints:

| Context | Bootstrap |
|---|---|
| Host machine producers | `localhost:9092` |
| Docker/Spark containers | `kafka:29092` |

## Run Spark Stream

PowerShell, recommended Docker mode:

```powershell
.\scripts\run_stream_speed_layer.ps1 -RunMode docker
```

PowerShell auto mode:

```powershell
.\scripts\run_stream_speed_layer.ps1
```

Bash local mode:

```bash
bash scripts/run_stream_speed_layer.sh
```

The stream reads `jobs_raw`, validates records, writes clean jobs to
`jobs_clean`, invalid records to `jobs_dead_letter`, and writes Elasticsearch
documents when `WRITE_ELASTICSEARCH=true`.

## Run Real TopCV Ingestion

Run crawler speed mode. It creates a new `jobs_speed_*.jsonl` file.

PowerShell:

```powershell
$env:CRAWLER_MAX_PAGES = "1"
$env:CRAWLER_UPDATED_WITHIN_MINUTES = "1440"
.\scripts\run_topcv_crawler.ps1 -Mode speed
```

Bash:

```bash
CRAWLER_MAX_PAGES=1 CRAWLER_UPDATED_WITHIN_MINUTES=1440 \
  bash scripts/run_topcv_crawler.sh
```

Default speed behavior:

- starts from page 1
- filters jobs updated within `CRAWLER_UPDATED_WITHIN_MINUTES`
- writes JSONL to `data/raw/jobs/source=topcv/ingest_date=...`
- uses `runtime/crawler/speed_processed_jobs_29d.json` to avoid recently
  processed `job_id`s

Batch mode:

```powershell
.\scripts\run_topcv_crawler.ps1 -Mode batch --days 7 --max-pages 0
.\scripts\run_topcv_crawler.ps1 -Mode batch --resume
```

Batch mode uses `runtime/crawler/batch_checkpoint.json` for resume and does not
use the speed processed cache.

## Run JSONL Producer

The producer now works like a small file tailer.

PowerShell:

```powershell
.\scripts\run_crawler_jsonl_producer.ps1
```

Bash:

```bash
bash scripts/run_crawler_jsonl_producer.sh
```

Default behavior:

- follows `data/raw/jobs/source=topcv/ingest_date=*/jobs_speed_*.jsonl`
- selects the newest matching file
- stores sent byte offsets in `runtime/producer/crawler_jsonl_offsets.json`
- sends only newly appended JSONL lines
- keeps running and polling for new lines every 2 seconds
- publishes to Kafka topic `jobs_raw` with key `job_id`

Run once and exit:

```powershell
$env:CRAWLER_JSONL_WATCH = "false"
.\scripts\run_crawler_jsonl_producer.ps1
```

Replay from the beginning, ignoring producer checkpoint:

```powershell
.\scripts\run_crawler_jsonl_producer.ps1 --replay
```

Follow a specific file:

```powershell
.\scripts\run_crawler_jsonl_producer.ps1 `
  --input "data/raw/jobs/source=topcv/ingest_date=2026-05-23/jobs_speed_20260523_113346.jsonl"
```

Producer delivery is at-least-once around crashes: it flushes Kafka before
writing the offset checkpoint. A crash before checkpoint update can resend the
last processed lines.

## Sample Producer

For deterministic local demos, use the fake crawler producer:

```powershell
.\scripts\run_fake_crawler.ps1
```

It reads `data/raw/raw_jobs_batch.jsonl` and publishes sample records to
`jobs_raw`.

## Data Contracts

Raw Kafka topic:

- topic: `jobs_raw`
- key: `job_id`
- value: nested JSON matching `apps/stream_etl/schemas/raw_job_schema.py`

Clean Kafka topic:

- topic: `jobs_clean`
- key: `job_id`
- value: flat normalized JSON from `build_clean_jobs`

Dead-letter Kafka topic:

- topic: `jobs_dead_letter`
- key: original key, `job_id`, or `unknown`
- value: raw JSON plus `error_reason` and Kafka metadata

Validation failures include:

- malformed JSON
- missing `job_id`
- missing `payload`
- missing `payload.title`
- missing event time when Kafka timestamp is unavailable

## Environment Variables

Crawler:

```env
CRAWLER_MODE=speed
CRAWLER_MAX_PAGES=15
CRAWLER_UPDATED_WITHIN_MINUTES=30
CRAWLER_DETAIL_BATCH_SIZE=40
CRAWLER_LIST_PAGES_PER_CHUNK=5
CRAWLER_PROCESSED_TTL_DAYS=29
CRAWLER_DEBUG_CARD_LINKS=false
```

JSONL producer:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
RAW_TOPIC=jobs_raw
CRAWLER_JSONL_INPUT=data/raw/jobs/source=topcv/ingest_date=*/jobs_speed_*.jsonl
CRAWLER_JSONL_CHECKPOINT_FILE=runtime/producer/crawler_jsonl_offsets.json
CRAWLER_JSONL_WATCH=true
CRAWLER_JSONL_POLL_SECONDS=2
CRAWLER_JSONL_LATEST_ONLY=true
CRAWLER_JSONL_EVENT_TIME_MODE=original
CRAWLER_JSONL_MAX_RECORDS=0
CRAWLER_JSONL_LOG_EVERY=10
```

Spark stream:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
RAW_TOPIC=jobs_raw
CLEAN_TOPIC=jobs_clean
DEAD_LETTER_TOPIC=jobs_dead_letter
CHECKPOINT_DIR=/checkpoints/speed
TRIGGER_SECONDS=10
STARTING_OFFSETS=earliest
WRITE_ELASTICSEARCH=true
WRITE_CONSOLE_DEBUG=false
ENABLE_JOBS_PER_10M=true
ENABLE_TOP_SKILLS_HOURLY=true
ENABLE_SALARY_BINS_HOURLY=true
```

If Cassandra is not running, set aggregate toggles to `false` for cleaner logs:

```powershell
$env:ENABLE_JOBS_PER_10M = "false"
$env:ENABLE_TOP_SKILLS_HOURLY = "false"
$env:ENABLE_SALARY_BINS_HOURLY = "false"
```

## Inspect Kafka

PowerShell:

```powershell
docker compose -f infra/docker-compose/docker-compose.speed.yml exec kafka kafka-console-consumer `
  --bootstrap-server kafka:29092 --topic jobs_clean --from-beginning

docker compose -f infra/docker-compose/docker-compose.speed.yml exec kafka kafka-console-consumer `
  --bootstrap-server kafka:29092 --topic jobs_dead_letter --from-beginning
```

Bash:

```bash
docker compose -f infra/docker-compose/docker-compose.speed.yml exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic jobs_clean --from-beginning
```

## Clean State

Clean Spark checkpoints:

```powershell
.\scripts\clean_stream_etl_checkpoints.ps1
```

```bash
bash scripts/clean_stream_etl_checkpoints.sh
```

Reset producer offsets:

```powershell
Remove-Item runtime/producer/crawler_jsonl_offsets.json
```

Delete and recreate Kafka topics:

```powershell
docker compose -f infra/docker-compose/docker-compose.speed.yml exec -T kafka kafka-topics `
  --bootstrap-server kafka:29092 --delete --topic jobs_raw
docker compose -f infra/docker-compose/docker-compose.speed.yml exec -T kafka kafka-topics `
  --bootstrap-server kafka:29092 --delete --topic jobs_clean
docker compose -f infra/docker-compose/docker-compose.speed.yml exec -T kafka kafka-topics `
  --bootstrap-server kafka:29092 --delete --topic jobs_dead_letter

.\scripts\create_kafka_topics.ps1
```

For a clean rerun, stop stream/producers first, clean Spark checkpoints, reset
producer offsets if needed, then recreate Kafka topics.

## Tests

```powershell
$env:UV_CACHE_DIR = ".uv-cache"
uv run --project . pytest
```

Focused producer tests:

```powershell
$env:UV_CACHE_DIR = ".uv-cache"
uv run --project . pytest tests/test_crawler_jsonl_producer.py
```

## Troubleshooting

| Issue | Fix |
|---|---|
| `nodriver` cannot find Chrome | Install Chrome, or set a browser path in crawler code/env if supported |
| `winget` cannot install Chrome | Install Chrome manually from browser, or repair/reset Windows App Installer |
| Spark local mode fails on Windows `winutils.exe` | Use `.\scripts\run_stream_speed_layer.ps1 -RunMode docker` |
| Kafka `OffsetOutOfRangeException` | Kafka topics were reset while Spark checkpoints still hold old offsets; clean stream checkpoints |
| Producer sends nothing | Check newest `jobs_speed_*.jsonl`, checkpoint offset file, and `CRAWLER_JSONL_INPUT` |
| Producer replays old records | You used `--replay` or deleted `runtime/producer/crawler_jsonl_offsets.json` |
| No records in `jobs_clean` | Check `jobs_dead_letter`, producer logs, and stream logs |
| Aggregate Cassandra errors | Cassandra service is commented out; disable aggregate toggles or enable Cassandra |

## Salary prediction in speed layer

Speed layer co the load Spark ML PipelineModel da train tu batch de them cac cot
`salary_display_*`, `salary_predicted_*`, `salary_prediction_applied`, `salary_source` vao
document realtime. Bat/tat bang:

```env
ENABLE_SALARY_PREDICTION=true
SALARY_MODEL_PATH=hdfs://hdfs-namenode.hdfs.svc:9000/models/salary_prediction/latest
```

Neu model chua ton tai, stream van chay va ghi prediction columns null. Sau khi
train model moi, restart stream de load model moi. Chi tiet xem
`docs/salary_prediction_runbook.md`.
