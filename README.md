# Speed Layer — Real-Time Job Market Pipeline

Real-time streaming pipeline that **crawls live job postings from TopCV**, pushes them through **Kafka**, processes them with **Spark Structured Streaming**, and serves aggregated results via **Cassandra + Elasticsearch + Kibana**.

## Architecture

```
TopCV Website ──► StreamingCrawler ──► process.py ──► Kafka ──► Spark Streaming ──► Cassandra & Elasticsearch
  (live data)     (configurable       (schema       (raw_events   (validate →       (durable      (search &
                   throughput)          mapping)      topic)        clean →           store)        Kibana)
                                                                   normalize →
                                                                   aggregate)
```

## What's Here

| File | Location | Purpose |
|------|----------|---------|
| `docker-compose.yml` | `infra/compose/` | Starts Kafka, Cassandra, Elasticsearch, Kibana |
| `.env` | `/` | Environment variables configuration |
| `pyproject.toml` | `/` | Python dependencies managed by `uv` |
| `producer.py` | `apps/ingestion/` | Crawls TopCV, maps to schema, publishes to Kafka |
| `process.py` | `apps/ingestion/` | Maps raw crawler JSON to `RAW_EVENT_SCHEMA` |
| `schemas.py` | `shared/` | Spark schema definition for raw events |
| `transform.py` | `apps/stream/` | Validate → Clean → Normalize transforms |
| `aggregations.py` | `apps/stream/` | Gold-layer window aggregations |
| `sinks.py` | `apps/stream/` | Writes main stream and aggregates to Cassandra & ES |
| `stream_main.py` | `apps/stream/` | Spark Structured Streaming entry point |
| `kafka_to_cassandra.py`| `apps/spark/` | Standalone job: Kafka → Cassandra |
| `kafka_to_es.py` | `apps/spark/` | Standalone job: Kafka → Elasticsearch |

## Quick Start

### 1. Start infrastructure

```bash
cd infra/compose
docker compose up -d
cd ../..
```

This starts:
- **Kafka** (Zookeeper + Broker) on `localhost:9092`
- **Cassandra** on `localhost:9042`
- **Elasticsearch** on `http://localhost:9200`
- **Kibana** on `http://localhost:5601`

*(Note: Wait 1-2 minutes for Cassandra and Elasticsearch to fully initialize).*

### 2. Install Python dependencies with `uv`

We use `uv` for lightning-fast dependency management.

```bash
# Sync dependencies and create .venv
uv sync

# Install Playwright browser (required for crawling TopCV)
uv run playwright install chromium
```

> **Note:** The crawler uses Playwright in **headed mode** (non-headless) to bypass Cloudflare bot detection. A display server is required.

### 3. Start the producer (real crawler → Kafka)

Run from the **project root** directory using `uv run`:

```bash
# Default: crawl all new jobs, 2s delay between each job
uv run apps/ingestion/producer.py

# Custom: specific keyword, faster throughput
uv run apps/ingestion/producer.py --keyword "data engineer" --delay-jobs 1.0

# Limit to 5 pages and 50 events, no looping
uv run apps/ingestion/producer.py --max-pages 5 --max-events 50 --no-loop
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

### 4. Start Spark Structured Streaming

Open a new terminal, and run from the **project root** directory using `uv run`:

```bash
# Set PySpark to use the Python environment from uv
$env:PYSPARK_PYTHON="python" # (On Windows PowerShell)
# export PYSPARK_PYTHON="python" # (On macOS/Linux)

uv run spark-submit \
  --driver-java-options "-Djava.security.manager=allow" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  apps/stream/stream_main.py
```

*Note: The Spark application will automatically connect and create the Cassandra keyspace (`job_market`) and all required tables if they do not exist.*

### 5. Access Dashboards & Data

- **Kibana**: Navigate to [http://localhost:5601](http://localhost:5601) to explore `jobs_realtime`, `job_counts_10m`, and `skill_counts_30m` indices.
- **Cassandra**: Connect via cqlsh (`docker exec -it speed-cassandra cqlsh`) to query the tables.

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
→ MAIN STREAM (realtime job events sink)
→ GOLD (window aggregates: job counts, skill counts)
→ SINK (Cassandra tables + Elasticsearch indices)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `RAW_TOPIC` | `raw_events` | Main Kafka topic |
| `CASSANDRA_HOST` | `localhost` | Cassandra host |
| `CASSANDRA_PORT` | `9042` | Cassandra port |
| `CASSANDRA_KEYSPACE` | `job_market` | Keyspace to store tables |
| `ES_URL` | `http://localhost:9200` | Elasticsearch URL |
| `ES_INDEX_JOBS` | `jobs_realtime` | Index for individual jobs |
| `CHECKPOINT_DIR` | `/tmp/speed_layer_checkpoints` | Spark checkpoint directory |
| `TRIGGER_SECONDS` | `10` | Spark micro-batch trigger interval |

## Tear Down

To stop the services, remove containers, and clear Spark checkpoints:

```bash
cd infra/compose
docker compose down -v
cd ../..

# On Windows PowerShell
Remove-Item -Recurse -Force \tmp\speed_layer_checkpoints -ErrorAction SilentlyContinue

# On macOS/Linux
rm -rf /tmp/speed_layer_checkpoints
```
