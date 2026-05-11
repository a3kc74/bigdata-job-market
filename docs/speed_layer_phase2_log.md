# Speed Layer Phase 2 Implementation Log

Date: 2026-05-11

## Scope

Implemented Phase 2 from the speed layer plan: a file-based fake crawler producer that replays `raw_jobs_batch.jsonl` into Kafka topic `jobs_raw`.

## Files Added

- `apps/producer/fake_crawler_producer.py`
  - Reads JSON Lines input one record at a time.
  - Validates each line as a JSON object.
  - Uses `job_id` as Kafka message key.
  - Sends the raw event as JSON message value with stream replay metadata.
  - Supports configurable delay between records.
  - Supports `--loop` replay mode.
  - Supports `--event-time-mode original` and `--event-time-mode now`.
  - Logs record counts and Kafka delivery results.

- `scripts/run_fake_crawler.sh`
  - Bash wrapper for the producer.
  - Defaults to `data/raw/raw_jobs_batch.jsonl`, topic `jobs_raw`, and `localhost:9092`.
  - Runs through `uv run --project <repo>`.

- `scripts/run_fake_crawler.ps1`
  - PowerShell wrapper for Windows.
  - Runs through `uv run --project <repo>`.

- `scripts/configure_uv_cache.ps1`
  - Sets User-level `UV_CACHE_DIR` to `$HOME\.cache\uv`.

- `scripts/configure_uv_cache.sh`
  - Creates `$HOME/.cache/uv` and prints the export line for Bash-compatible shells.

## How To Run

```bash
bash scripts/run_fake_crawler.sh
```

On Windows PowerShell:

```powershell
.\scripts\run_fake_crawler.ps1
```

Configure uv cache on Windows:

```powershell
.\scripts\configure_uv_cache.ps1
```

Useful overrides:

```bash
FAKE_CRAWLER_EVENT_TIME_MODE=original FAKE_CRAWLER_SLEEP_MS=500 bash scripts/run_fake_crawler.sh
FAKE_CRAWLER_LOOP=true FAKE_CRAWLER_MAX_RECORDS=100 bash scripts/run_fake_crawler.sh
```

## Expected Result

- Kafka UI shows messages continuously entering topic `jobs_raw`.
- Message key is `job_id`.
- Message value keeps the raw crawler JSON and includes:
  - `stream_ingest_ts`
  - `replay_id`
  - `replay_seq`
  - `original_event_ts` when `--event-time-mode now` is used
