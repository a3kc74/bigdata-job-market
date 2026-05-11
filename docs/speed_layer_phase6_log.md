# Speed Layer Phase 6 Implementation Log

Date: 2026-05-11

## Scope

Implemented realtime salary-bin aggregation:

```text
clean stream
-> salary_min_million / salary_max_million / salary_avg_million / salary_bin
-> 1-hour event-time window
-> 2-hour watermark
-> group by city, level, salary_bin
-> job_count + average salary bounds + median salary
-> Cassandra realtime_salary_bins_hourly
-> Elasticsearch realtime_salary_bins_hourly_v1
```

## Files Added

- `apps/stream_etl/stateful_jobs/salary_bins_realtime.py`
  - Builds hourly salary-bin aggregates.

- `apps/stream_etl/sinks/salary_bins_realtime_sink.py`
  - Writes aggregate rows to Cassandra table `realtime_salary_bins_hourly`.
  - Indexes aggregate rows to Elasticsearch index `realtime_salary_bins_hourly_v1`.

- `shared/udfs/salary_parser.py`
  - Pure Python salary parsing helper for tests and future UDF reuse.

## Files Updated

- `apps/stream_etl/stream_main.py`
  - Adds the phase 6 streaming query.
  - Uses checkpoint path `${CHECKPOINT_DIR}/salary_bins_hourly`.
  - Controlled by `ENABLE_SALARY_BINS_HOURLY=true|false`.

## How To Run

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:CASSANDRA_HOST = "localhost"
$env:CASSANDRA_KEYSPACE = "job_market_speed"
$env:ES_URL = "http://localhost:9200"
$env:ENABLE_SALARY_BINS_HOURLY = "true"
$env:CHECKPOINT_DIR = "C:\tmp\job-market-speed-checkpoints-phase6"
.\scripts\run_stream_speed_layer.ps1
```
