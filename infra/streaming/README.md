# Speed Layer — Real-Time Job Market Pipeline

Real-time streaming pipeline that **crawls live job postings from TopCV**, pushes them through **Kafka**, processes them with **Spark Structured Streaming**, and serves aggregated results via **Redis + FastAPI**.

## Architecture

```
TopCV Website ──► StreamingCrawler ──► process.py ──► Kafka ──► Spark Streaming ──► Redis ──► FastAPI
  (live data)     (configurable       (schema       (raw_events   (validate →       (speed    (REST
                   throughput)          mapping)      topic)        clean →           views)    API)
                                                                   normalize →
                                                                   aggregate)
```

## What's Here

| File | Location | Purpose |
|------|----------|---------|
| `docker-compose.yml` | `infra/compose/` | Starts Kafka (Zookeeper) + Redis |
| `.env.example` | `configs/` | Environment variable template |
| `create_topics.py` | `scripts/` | Creates Kafka topics (`raw_events`, `raw_events_dlq`) |
| `requirements.txt` | `configs/` | Python dependencies for the speed layer |
| `producer.py` | `apps/ingestion/` | Crawls TopCV, maps to schema, publishes to Kafka |
| `process.py` | `apps/ingestion/` | Maps raw crawler JSON to `RAW_EVENT_SCHEMA` |
| `schemas.py` | `shared/` | Spark schema definition for raw events |
| `transform.py` | `apps/stream/` | Validate → Clean → Normalize transforms |
| `aggregations.py` | `apps/stream/` | Gold-layer window aggregations |
| `sinks.py` | `apps/stream/` | Writes gold aggregates to Redis |
| `stream_main.py` | `apps/stream/` | Spark Structured Streaming entry point |
| `api.py` | `apps/serving/` | FastAPI endpoints to query speed views |
| `sample_events.py` | `scripts/` | (Legacy) Fake event generator for testing |

## Quick Start

### 1. Copy env file

```bash
cp configs/.env.example .env
```

### 2. Start infrastructure

```bash
cd infra/compose
docker compose up -d
cd ../..
```

This starts:
- **Kafka** (Zookeeper + Broker) on `localhost:9092`
- **Redis** on `localhost:6379`

### 3. Install Python dependencies

```bash
pip install -r configs/requirements.txt

# Install Playwright browser (required for crawling TopCV)
playwright install chromium
```

> **Note:** The crawler uses Playwright in **headed mode** (non-headless) to bypass
> Cloudflare bot detection. A display server (X11/Wayland) must be available
> (i.e. `DISPLAY` or `WAYLAND_DISPLAY` env var set). On a headless server,
> use `xvfb-run` to provide a virtual display.

### 4. Create Kafka topics

```bash
# From project root
python scripts/create_topics.py
```

### 5. Start the producer (real crawler → Kafka)

Run from the **project root** directory:

```bash
# Default: crawl all new jobs, 2s delay between each job
python apps/ingestion/producer.py

# Custom: specific keyword, faster throughput
python apps/ingestion/producer.py --keyword "data engineer" --delay-jobs 1.0

# Limit to 5 pages and 50 events, no looping
python apps/ingestion/producer.py --max-pages 5 --max-events 50 --no-loop

# Filter by city
python apps/ingestion/producer.py --keyword "python" --location "ha-noi" --delay-jobs 0.5
```

#### Producer CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--keyword` | `""` | TopCV search keyword (empty = all jobs) |
| `--location` | `""` | City filter (e.g. `ha-noi`, `ho-chi-minh`) |
| `--delay-jobs` | `2.0` | Seconds between crawling each job (throughput control) |
| `--delay-pages` | `5.0` | Seconds between fetching listing pages |
| `--max-pages` | `0` | Max listing pages per cycle (0 = unlimited) |
| `--max-events` | `0` | Stop after N events (0 = unlimited) |
| `--no-loop` | `false` | Don't restart after exhausting all pages |

> **Throughput tuning:** Lower `--delay-jobs` = faster stream. Set `--delay-jobs 0.5` for ~2 events/sec, or `--delay-jobs 5.0` for a slower, gentler crawl.

### 6. Start Spark Structured Streaming

Run from the **project root** directory:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  apps/stream/stream_main.py
```

### 7. Start the API

Run from the **project root** directory:

```bash
uvicorn apps.serving.api:app --reload --port 8000
```

### 8. Test endpoints

```bash
curl http://localhost:8000/health
curl http://localhost:8000/realtime/job-counts
curl http://localhost:8000/realtime/top-skills
```

## Data Flow Detail

### Crawler → Schema Mapping

The `process.py` module maps raw crawler output to `RAW_EVENT_SCHEMA`:

| Crawler Field | Schema Field | Transform |
|---------------|-------------|-----------|
| `source_url` | `source_url` | Direct |
| (hash of `source_url`) | `job_id` | MD5 hash |
| `domain` | `source` | Strip `www.` |
| `title` | `title` | Direct |
| `company_name` | `company_name` | Direct |
| `salary` | `salary_text` | Direct |
| `location` | `location_text` | Direct |
| `skills` (list) | `skills_text` | Join with `, ` |
| `description` + `requirements` + `benefits` | `description_text` | Concatenated |
| `crawled_at` | `event_ts` | Direct (ISO 8601) |
| (generated) | `ingest_ts` | Current UTC time |

### Spark Pipeline

```
RAW SOURCE (Kafka) → VALIDATE (parse JSON + schema) → CLEAN (filter, parse timestamps)
→ NORMALIZE (location mapping, salary parsing, skills array)
→ GOLD (window aggregates: job counts, skill counts)
→ SINK (Redis hashes + sorted-set indices)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `RAW_TOPIC` | `raw_events` | Main Kafka topic |
| `DLQ_TOPIC` | `raw_events_dlq` | Dead-letter queue topic |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_TTL_SECONDS` | `7200` | TTL for Redis speed view keys |
| `CHECKPOINT_DIR` | `/tmp/speed_layer_checkpoints` | Spark checkpoint directory |
| `TRIGGER_SECONDS` | `10` | Spark micro-batch trigger interval |
| `PRODUCER_DELAY_SECONDS` | `0` | Extra delay after each Kafka produce |

## Crawler Standalone Usage

The crawler can also be used independently:

```bash
# Crawl a single URL
python apps/ingestion/crawler.py single "https://www.topcv.vn/viec-lam/..."

# Stream to stdout (no Kafka)
python apps/ingestion/crawler.py stream --keyword "data" --max-events 10 --no-loop
```

## Tear Down docker containers and Spark checkpoints

```bash
cd infra/compose
docker compose down -v
rm -rf /tmp/speed_layer_checkpoints
```
