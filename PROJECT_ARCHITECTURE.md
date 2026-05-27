# Project Architecture — Big Data Job Market

## Overview

Hệ thống thu thập và phân tích dữ liệu thị trường lao động từ **TopCV**, áp dụng mô hình **Lambda Architecture** với 5 tầng chức năng.

---

## System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                  │
│                                                                         │
│          Historical JSON Files          Crawler Producer                │
│                   │                          │                          │
└───────────────────┼──────────────────────────┼──────────────────────────┘
                    │                          │
        ┌───────────▼──────────┐   ┌───────────▼───────────┐
        │     BATCH LAYER      │   │     SPEED LAYER       │
        │                      │   │                       │
        │    HDFS Raw Zone     │   │         Kafka         │
        │          ↓           │   │           ↓           │
        │   Spark Batch ETL    │   │    Spark Structured   │
        │          ↓           │   │       Streaming       │
        │   HDFS Bronze Zone   │   │           ↓           │
        │          ↓           │   │  Realtime Aggregation │
        │   Spark Batch ETL    │   │                       │
        │          ↓           │   │                       │
        │ HDFS Silver/Gold Zone│   │                       │
        └──────────┬───────────┘   └──────────┬────────────┘
                   │                          │
        ┌──────────▼──────────────────────────▼─────────────┐
        │                  SERVING LAYER                    │
        │                                                   │
        │                  Elastic Search                   │
        │                         ↓                         │
        │                       Kibana                      │
        └───────────────────────────────────────────────────┘

┌─────────────────────────────┐
│       PLATFORM / OPS        │        
│  Docker / Kubernetes        │
│  Minikube local deployment  │
│           ↓                 │
│      manages all layers     │
└─────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Data Sources** | Python (Requests, BeautifulSoup), Kafka Producer, HDFS Loader |
| **Batch Layer** | PySpark, HDFS, Parquet |
| **Speed Layer** | Kafka, Spark Structured Streaming |
| **Serving Layer** | Elasticsearch, Kibana |
| **Platform / Ops** | Kubernetes (Minikube), Docker |

---

## Data Model — 4-Layer Medallion Architecture

```
Raw (JSONL)  →  Bronze (Parquet)  →  Silver (Parquet)  →  Gold (Parquet/Elasticsearch)
```

| Layer | Format | Location | Description |
|---|---|---|---|
| **Raw** | JSONL | `hdfs:///raw/jobs/ingest_date=YYYY-MM-DD/` | Crawler output, passthrough, immutable |
| **Bronze** | Parquet (Snappy) | `hdfs:///bronze/jobs/ingest_date=YYYY-MM-DD/` | Flatten + cast types + dedup + count metrics |
| **Silver** | Parquet (Snappy) | `hdfs:///silver/jobs/ingest_date=YYYY-MM-DD/` | Canonicalization (salary, location, experience) |
| **Gold** | Parquet / Elasticsearch | `hdfs:///gold/` | Denormalized tables for Serving |

---

## Repository Structure

```
bigdata-job-market/
├── apps/
│   ├── batch/
│   │   └── spark/
│   │       ├── raw_to_bronze.py        # Spark ETL: Raw → Bronze
│   │       ├── bronze_to_silver.py     # Spark ETL: Bronze → Silver
│   │       ├── silver_to_gold.py       # Spark ETL: Silver → Gold
│   │       └── gold_to_elasticsearch.py # Load Gold layer to Elasticsearch
│   ├── stream_etl/
│   │   ├── stream_main.py              # Spark Structured Streaming main
│   │   ├── sinks/
│   │   │   ├── elasticsearch_sink.py   # Write realtime jobs to ES
│   │   │   ├── jobs_per_10m_sink.py    # Write job counts to ES
│   │   │   ├── top_skills_hourly_sink.py # Write top skills to ES
│   │   │   └── salary_bins_realtime_sink.py # Write salary aggregates to ES
│   │   └── stateful_jobs/
│   ├── producer/                       # Kafka producers
│   └── ingestion/                      # Real crawler
├── data/
│   ├── raw/
│   │   └── raw_data_format.md
│   ├── bronze/
│   │   └── bronze_data_format.md
│   ├── silver/
│   └── gold/
├── docs/
├── infra/
│   ├── spark/
│   │   ├── Dockerfile                  # Spark image with batch + streaming
│   │   ├── 10-rbac.yaml
│   │   ├── raw-to-bronze-cronjob.yaml
│   │   ├── bronze-to-silver-cronjob.yaml
│   │   ├── silver-to-gold-cronjob.yaml
│   │   ├── gold-to-elasticsearch-cronjob.yaml
│   │   └── speed-stream-es-job.yaml    # Streaming ES-only job
│   ├── hdfs/
│   │   └── hdfs.yaml                   # HDFS 3-node cluster
│   ├── kafka/
│   │   ├── kafka-cluster.yaml          # Kafka KRaft mode
│   │   └── topics.yaml                 # Kafka topics
│   ├── search/
│   │   ├── elasticsearch.yaml
│   │   └── kibana.yaml
│   ├── serving/
│   │   └── api.yaml                    # FastAPI search endpoint
│   ├── namespaces/
│   │   └── all.yaml                    # hdfs, spark, search, serving, kafka
│   └── docker-compose/
│       └── docker-compose.speed.yml    # Local Docker Compose (dev)
├── shared/
│   └── transformations/
├── tests/
└── scripts/
```

---

## Deployment Model

All services are containerized with **Docker** and orchestrated by **Kubernetes (Minikube)** for local development. Each layer runs in its own dedicated namespace:

| Namespace | Services |
|---|---|
| `spark` | Spark Driver Pods, Executor Pods, CronJobs |
| `hdfs` | HDFS NameNode, DataNode |
| `kafka` | Kafka Broker, Zookeeper |
| `elastic` | Elasticsearch, Kibana |
