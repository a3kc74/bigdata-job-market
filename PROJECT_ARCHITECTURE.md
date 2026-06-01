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
| **Serving Layer** | Elasticsearch, Kibana, FastAPI |
| **Platform / Ops / Orchestration** | Kubernetes (Minikube), Docker, Apache Airflow |

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
│   ├── api/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── search_api.py               # FastAPI search server
│   ├── batch/
│   │   ├── jobs/
│   │   │   ├── raw_to_bronze.py        # Spark ETL: Raw → Bronze
│   │   │   ├── bronze_to_silver.py     # Spark ETL: Bronze → Silver
│   │   │   ├── silver_to_gold.py       # Spark ETL: Silver → Gold
│   │   │   ├── gold_to_elasticsearch.py # Load Gold layer to Elasticsearch
│   │   │   └── train_salary_model.py   # Train salary prediction model
│   │   ├── scripts/
│   │   └── instruction/
│   ├── common/
│   ├── ingestion/                      # Job crawlers (TopCV, batch crawlers)
│   │   ├── batch_crawler.py
│   │   ├── crawler.py
│   │   ├── topcv_crawler.py
│   │   └── CRAWLER_LOGIC.md
│   ├── ml/
│   │   ├── salary_prediction.py        # Salary Prediction Model logic
│   │   └── IMPLEMENTATION_PLAN.md
│   ├── producer/                       # Kafka producers for crawled jobs
│   ├── serving/
│   │   └── api.py                      # Serving endpoint helper
│   ├── spark/
│   │   └── kafka_to_es.py
│   └── stream_etl/
│       ├── stream_main.py              # Spark Structured Streaming main
│       ├── normalizers.py              # Realtime data normalization
│       ├── transform.py                # Streaming transformations
│       ├── schemas/
│       │   └── raw_job_schema.py
│       ├── sinks/
│       │   ├── elasticsearch_sink.py   # Write realtime jobs to ES
│       │   ├── jobs_per_10m_sink.py    # Write job counts to ES
│       │   ├── kafka_sink.py           # Write clean jobs back to Kafka
│       │   ├── salary_bins_realtime_sink.py # Write salary aggregates to ES
│       │   └── top_skills_hourly_sink.py # Write top skills to ES
│       └── stateful_jobs/
│           ├── jobs_per_10m.py
│           ├── salary_bins_realtime.py
│           └── top_skills_hourly.py
├── data/
│   ├── raw/
│   │   └── raw_data_format.md          # Raw data contract and fields
│   ├── bronze/
│   │   └── bronze_data_format.md       # Bronze data contract and fields
│   ├── silver/
│   │   └── silver_data_format.md       # Silver data contract and fields
│   └── gold/
│       └── gold_data_format.md         # Gold data contract and fields
├── docs/                               # Runbooks, setup guides, documentation files
├── infra/
│   ├── airflow/                        # Airflow orchestration deployment
│   │   ├── Dockerfile
│   │   ├── airflow.yaml
│   │   ├── airflow-postgres.yaml
│   │   ├── airflow-rbac.yaml
│   │   └── dags/                       # Airflow DAGs
│   │       ├── job_market_batch_pipeline.py
│   │       ├── job_market_speed_layer_bootstrap.py
│   │       └── job_market_speed_real_crawler.py
│   ├── spark/
│   │   ├── Dockerfile                  # Spark image with batch + streaming
│   │   ├── rbac.yaml                   # Spark RBAC configuration
│   │   ├── raw-to-bronze-cronjob.yaml
│   │   ├── bronze-to-silver-cronjob.yaml
│   │   ├── silver-to-gold-cronjob.yaml
│   │   ├── gold-to-elasticsearch-cronjob.yaml
│   │   ├── salary-model-train-cronjob.yaml
│   │   ├── speed-stream-es-job.yaml    # Streaming ES-only job
│   │   └── speed-checkpoint-pvc.yaml
│   ├── hdfs/
│   │   └── hdfs.yaml                   # HDFS 3-node cluster (NameNode, DataNode)
│   ├── kafka/
│   │   ├── kafka-cluster.yaml          # Kafka KRaft mode cluster
│   │   └── jobs-topics.yaml            # Kafka topics configuration
│   ├── search/
│   │   ├── elasticsearch-statefulset.yaml
│   │   ├── elasticsearch-service.yaml
│   │   ├── kibana-deployment.yaml
│   │   └── kibana-service.yaml
│   ├── serving/
│   │   ├── job-search-api-deployment.yaml
│   │   └── job-search-api-service.yaml
│   ├── namespaces/
│   │   └── all.yaml                    # spark, search, serving, kafka, airflow namespaces
│   └── docker-compose/
│       ├── docker-compose.dev.yml      # Local dev environment compose
│       └── docker-compose.speed.yml    # Speed layer local compose
├── shared/
│   ├── quality/
│   │   └── streaming_quality_rules.py  # Shared quality control rules
│   ├── udfs/
│   │   └── salary_parser.py            # Shared Spark UDFs
│   └── schemas.py                      # Shared model schemas
├── tests/
└── scripts/
```

---

## Deployment Model

All services are containerized with **Docker** and orchestrated by **Kubernetes (Minikube)** for local development. Each layer runs in its own dedicated namespace:

| Namespace | Services |
|---|---|
| `spark` | Spark Driver Pods, Executor Pods, CronJobs |
| `hdfs` | HDFS NameNode, DataNode (Namespace defined in hdfs.yaml) |
| `kafka` | Kafka Broker, Topics |
| `search` | Elasticsearch, Kibana |
| `serving` | FastAPI Search API Endpoint |
| `airflow` | Airflow Webserver, Scheduler, Postgres |
