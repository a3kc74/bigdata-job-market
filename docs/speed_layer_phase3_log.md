# Speed Layer Phase 3 Implementation Log

Date: 2026-05-11

## Scope

Implemented the Spark parse and clean stream stage:

```text
Kafka jobs_raw
-> Spark Structured Streaming
-> parsed/validated raw job envelope
-> normalized clean records
-> Kafka jobs_clean
-> Kafka jobs_dead_letter for invalid records
-> Elasticsearch jobs_realtime_v1
```

## Files Added

- `apps/stream_etl/stream_main.py`
  - Main Spark Structured Streaming entrypoint for phase 3.
  - Reads `jobs_raw`, writes `jobs_clean`, writes `jobs_dead_letter`, and optionally writes `jobs_realtime_v1`.

- `apps/stream_etl/schemas/raw_job_schema.py`
  - Defines the nested raw crawler event schema with `payload` and `quality_flags`.

- `apps/stream_etl/transform.py`
  - Parses Kafka records.
  - Applies quality rules.
  - Normalizes event time, city, skills, salary fields, and clean schema columns.

- `shared/quality/streaming_quality_rules.py`
  - Adds validation errors for malformed JSON and required-field failures.

- `apps/stream_etl/sinks/kafka_sink.py`
  - Converts clean and dead-letter rows into Kafka key/value format.

- `apps/stream_etl/sinks/elasticsearch_sink.py`
  - Writes clean jobs to Elasticsearch index `jobs_realtime_v1` through `_bulk`.

- `scripts/run_stream_speed_layer.sh`
  - Bash wrapper running `spark-submit` through `uv run`.

- `scripts/run_stream_speed_layer.ps1`
  - PowerShell wrapper running `spark-submit` through `uv run`.

## How To Run

```bash
bash scripts/run_stream_speed_layer.sh
```

On Windows PowerShell:

```powershell
.\scripts\run_stream_speed_layer.ps1
```

Useful local settings:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:CHECKPOINT_DIR = "C:\tmp\job-market-speed-checkpoints"
$env:WRITE_ELASTICSEARCH = "false"
.\scripts\run_stream_speed_layer.ps1
```

Inside the Docker Compose network, use `kafka:29092` and `/checkpoints/speed`.

## Troubleshooting

If `jobs_raw` already has messages but `jobs_clean` remains empty, use a new checkpoint path:

```powershell
$env:STARTING_OFFSETS = "earliest"
$env:CHECKPOINT_DIR = "C:\tmp\job-market-speed-checkpoints-phase3-rerun"
$env:WRITE_CONSOLE_DEBUG = "true"
.\scripts\run_stream_speed_layer.ps1
```

Spark ignores `STARTING_OFFSETS` after a checkpoint exists, because committed offsets are restored from the checkpoint.
