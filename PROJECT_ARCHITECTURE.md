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
        ┌───────────▼──────────┐   ┌───────────▼──────────┐
        │     BATCH LAYER      │   │     SPEED LAYER       │
        │                      │   │                       │
        │  HDFS Raw Zone       │   │  Kafka                │
        │       ↓              │   │       ↓               │
        │  Spark Batch ETL     │   │  Spark Structured     │
        │       ↓              │   │  Streaming            │
        │  Silver/Gold         │   │       ↓               │
        │  Analytics           │   │  Realtime Aggregates  │
        └──────────┬───────────┘   └──────────┬────────────┘
                   │                          │
        ┌──────────▼──────────────────────────▼────────────┐
        │                  SERVING LAYER                    │
        │                                                   │
        │         Cassandra          Elasticsearch          │
        │              └──────────┬──────────┘             │
        │                         ↓                        │
        │            FastAPI / Kibana / Grafana             │
        └───────────────────────────────────────────────────┘

┌─────────────────────────────┐
│       PLATFORM / OPS        │
│  Monitoring / Config /      │
│  Security                   │
│           ↓                 │
│      Kubernetes             │──────► (manages all layers above)
└─────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Data Sources** | Python (Requests, BeautifulSoup), Kafka Producer, HDFS Loader |
| **Batch Layer** | PySpark, HDFS, Parquet |
| **Speed Layer** | Kafka, Spark Structured Streaming |
| **Serving Layer** | Cassandra, Elasticsearch, FastAPI, Kibana, Grafana |
| **Platform / Ops** | Kubernetes (Minikube), Docker |

---

## Data Model — 4-Layer Medallion Architecture

```
Raw (JSONL)  →  Bronze (Parquet)  →  Silver (Parquet)  →  Gold (Parquet/Cassandra)
```

| Layer | Format | Location | Description |
|---|---|---|---|
| **Raw** | JSONL | `hdfs:///raw/jobs/ingest_date=YYYY-MM-DD/` | Crawler output, passthrough, immutable |
| **Bronze** | Parquet (Snappy) | `hdfs:///bronze/jobs/ingest_date=YYYY-MM-DD/` | Flatten + cast types + dedup + count metrics |
| **Silver** | Parquet (Snappy) | `hdfs:///silver/jobs/ingest_date=YYYY-MM-DD/` | Canonicalization (salary, location, experience) |
| **Gold** | Parquet / Cassandra | `hdfs:///gold/` | Aggregated analytics tables |

---

## Repository Structure

```
bigdata-job-market/
├── apps/
│   ├── batch/
│   │   └── jobs/
│   │       ├── raw_to_bronze.py        # Spark ETL: Raw → Bronze
│   │       ├── bronze_to_silver.py     # Spark ETL: Bronze → Silver (TODO)
│   │       └── silver_to_gold.py       # Spark ETL: Silver → Gold (TODO)
│   └── spark/
│       └── kafka_to_cassandra_es.py    # Structured Streaming job
├── data/
│   ├── raw/
│   │   └── raw_data_format.md          # Raw schema specification
│   └── bronze/
│       └── bronze_data_format.md       # Bronze schema specification
├── docs/
│   ├── architecture.md                 # This file
│   ├── hdfs_data_ingestion.md          # How to load raw data into HDFS
│   ├── raw_to_bronze_runbook.md        # How to run raw_to_bronze job
│   └── spark_on_minikube.md            # General guide: Spark Jobs on Minikube
├── infra/
│   ├── spark/
│   │   ├── Dockerfile                  # Spark image with ETL jobs
│   │   └── 10-rbac.yaml               # Namespace + ServiceAccount + RoleBinding
│   ├── kubernetes/
│   │   └── batch-etl-cronjob.yaml     # CronJob: daily raw→bronze
│   ├── kafka/
│   ├── cassandra/
│   └── elastic/
├── shared/
│   └── transformations/               # Shared UDFs and helpers (TODO)
└── tests/                             # Unit tests (TODO)
```

---

## Deployment Model

All services are containerized with **Docker** and orchestrated by **Kubernetes (Minikube)** for local development. Each layer runs in its own dedicated namespace:

| Namespace | Services |
|---|---|
| `spark` | Spark Driver Pods, Executor Pods, CronJobs |
| `hdfs` | HDFS NameNode, DataNode |
| `kafka` | Kafka Broker, Zookeeper |
| `cassandra` | Cassandra StatefulSet |
| `elastic` | Elasticsearch, Kibana |
