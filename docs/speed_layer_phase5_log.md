# Speed Layer Phase 5 Implementation Log

Date: 2026-05-11

## Scope

Implemented hourly top-skills aggregation:

```text
clean stream
-> explode skills
-> 1-hour event-time window
-> 2-hour watermark
-> count distinct job_id by skill
-> foreachBatch rank top 10 per window
-> Cassandra skill counts + top skills
-> Elasticsearch skill counts + top skills
```

## Files Added

- `apps/stream_etl/stateful_jobs/top_skills_hourly.py`
  - Builds hourly skill counts from clean stream `skills`.

- `apps/stream_etl/sinks/top_skills_hourly_sink.py`
  - Writes full counts to `realtime_skill_counts_hourly`.
  - Ranks top N in `foreachBatch`.
  - Writes top rows to `realtime_top_skills_hourly`.
  - Indexes to `realtime_skill_counts_hourly_v1` and `realtime_top_skills_hourly_v1`.

## Files Updated

- `apps/stream_etl/stream_main.py`
  - Adds the phase 5 streaming query.
  - Uses checkpoint path `${CHECKPOINT_DIR}/top_skills_hourly`.
  - Controlled by `ENABLE_TOP_SKILLS_HOURLY=true|false`.

## How To Run

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:CASSANDRA_HOST = "localhost"
$env:CASSANDRA_KEYSPACE = "job_market_speed"
$env:ES_URL = "http://localhost:9200"
$env:ENABLE_TOP_SKILLS_HOURLY = "true"
$env:CHECKPOINT_DIR = "C:\tmp\job-market-speed-checkpoints-phase5"
.\scripts\run_stream_speed_layer.ps1
```

Useful controls:

```powershell
$env:TOP_SKILLS_HOURLY_N = "10"
$env:ENABLE_JOBS_PER_10M = "false"
$env:ENABLE_TOP_SKILLS_HOURLY = "true"
```
