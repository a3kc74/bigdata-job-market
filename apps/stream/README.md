# Speed Layer — Local Demo Infrastructure & Setup

This directory contains everything needed to **run the speed layer demo locally**.

## What's Here

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Starts Kafka (Zookeeper) + Redis |
| `.env.example` | Environment variable template |
| `create_topics.py` | Creates Kafka topics (`raw_events`, `raw_events_dlq`) |
| `requirements.txt` | Python dependencies for the speed layer |

## Architecture

```
Data Generation → Kafka → Spark Structured Streaming → Redis → FastAPI
                  (raw)    (bronze → silver → gold)    (speed   (serving)
                                                        views)
```

## Quick Start

### 1. Copy env file

```bash
cp .env.example ../../.env
# or: cp .env.example .env  (and source it where needed)
```

### 2. Start infrastructure

```bash
docker compose up -d
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Create Kafka topics

```bash
python create_topics.py
```

### 5. Start the producer (from repo root)

```bash
python producer.py
```

### 6. Start Spark streaming (from repo root)

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  stream_main.py
```

### 7. Start the API (from repo root)

```bash
uvicorn api:app --reload --port 8000
```

### 8. Test endpoints

```bash
curl http://localhost:8000/health
curl http://localhost:8000/realtime/job-counts
curl http://localhost:8000/realtime/top-skills
```

## Tear Down

```bash
docker compose down -v
```

## Related Application Code

| Module | Location | Layer |
|--------|----------|-------|
| Sample events | `apps/ingestion/sample_events.py` | Data generation |
| Kafka producer | `apps/ingestion/producer.py` | Raw |
| Schemas | `apps/stream/schemas.py` | Bronze |
| Transforms | `apps/stream/transform.py` | Bronze → Silver |
| Aggregations | `apps/stream/aggregations.py` | Silver → Gold |
| Redis sinks | `apps/stream/sinks.py` | Gold → Speed views |
| Stream main | `apps/stream/stream_main.py` | Orchestrator |
| Serving API | `apps/serving/api.py` | Serving |

## Simplified vs Production

This is a demo. Key simplifications:

- **Data source** uses fake events, not the real crawler
- **DLQ** topic is created but not yet wired into Spark error routing
- **Redis sink** uses `foreachBatch(...collect())` — fine for demo-sized aggregates
- **Salary parsing** is simplified
- **NoSQL** uses Redis (matches the diagram, easier locally than HBase)
