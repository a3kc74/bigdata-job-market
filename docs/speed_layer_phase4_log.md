# Speed Layer Phase 4 Implementation Log

Date: 2026-05-11

## Scope

Implemented the `jobs per 10 minutes` stateful aggregation:

```text
clean stream
-> 10-minute event-time window
-> 60-minute watermark
-> count distinct job_id to avoid duplicate overcount
-> group by source, city, category
-> job_count + distinct_company_count
-> Cassandra realtime_job_counts_10m
-> Elasticsearch realtime_job_counts_10m_v1
```

## Files Added

- `apps/stream_etl/stateful_jobs/jobs_per_10m.py`
  - Builds the 10-minute event-time window aggregation.

- `apps/stream_etl/sinks/jobs_per_10m_sink.py`
  - Writes aggregate rows to Cassandra table `realtime_job_counts_10m`.
  - Indexes aggregate rows to Elasticsearch index `realtime_job_counts_10m_v1`.

## Files Updated

- `apps/stream_etl/stream_main.py`
  - Adds the phase 4 streaming query.
  - Uses checkpoint path `${CHECKPOINT_DIR}/jobs_per_10m`.
  - Controlled by `ENABLE_JOBS_PER_10M=true|false`.

## How To Run

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:CASSANDRA_HOST = "localhost"
$env:CASSANDRA_KEYSPACE = "job_market_speed"
$env:ES_URL = "http://localhost:9200"
$env:ENABLE_JOBS_PER_10M = "true"
$env:CHECKPOINT_DIR = "C:\tmp\job-market-speed-checkpoints-phase4"
.\scripts\run_stream_speed_layer.ps1
```

If you only want to debug `jobs_clean` without phase 4 sinks:

```powershell
$env:ENABLE_JOBS_PER_10M = "false"
```
