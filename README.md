# Project Architecture - Big Data Job Market

## Overview

Dự án thu thập và phân tích dữ liệu việc làm từ TopCV theo mô hình **Lambda Architecture**:

- **Batch layer**: crawler ghi raw JSON/JSONL vào HDFS, Spark batch ETL chuyển Raw -> Bronze -> Silver -> Gold, sau đó index Gold vào Elasticsearch.
- **Speed layer**: producer đẩy job event vào Kafka, Spark Structured Streaming làm sạch/score dữ liệu realtime, ghi Kafka clean/DLQ và Elasticsearch realtime indexes.
- **Serving layer**: Elasticsearch + Kibana cho dashboard/search, FastAPI cho API tìm kiếm batch Gold.
- **Platform/Ops**: Docker image, Kubernetes/Minikube, Airflow DAGs, scripts và Makefile.

## Architecture Diagram

```text
                         +----------------------+
                         |        TopCV         |
                         +----------+-----------+
                                    |
               +--------------------+--------------------+
               |                                         |
        Batch crawler                              Realtime producer
 apps/ingestion/batch_crawler.py              apps/producer/*.py
 apps/ingestion/run_crawler_to_hdfs.py                |
               |                                      Kafka
               v                         jobs_raw / jobs_clean / jobs_dead_letter
          HDFS Raw                                      |
     /raw/jobs/...                                      v
               |                       Spark Structured Streaming
               v                         apps/stream_etl/stream_main.py
      Spark batch ETL                                   |
 apps/batch/jobs/raw_to_bronze.py                       |
               |                         +--------------+--------------+
               v                         |                             |
          HDFS Bronze                Elasticsearch                 Kafka DLQ/Clean
     /bronze/jobs/...          realtime_jobs_v1 and agg indexes
               |
               v
      Spark batch ETL
 apps/batch/jobs/bronze_to_silver.py
               |
               v
          HDFS Silver ------------------+
     /silver/jobs/...                   |
               |                        v
               |              Salary model training
               |          apps/batch/jobs/train_salary_model.py
               |                        |
               v                        v
      Spark batch ETL           HDFS salary model
 apps/batch/jobs/silver_to_gold.py
               |
               v
           HDFS Gold
 /gold/jobs/job_market_index
               |
               v
    apps/batch/jobs/gold_to_elasticsearch.py
               |
               v
       Elasticsearch gold-jobs-flat
               |
        +------+------+
        |             |
      Kibana      FastAPI Search API
              apps/api/search_api.py
```

## Tech Stack

| Area | Current implementation |
|---|---|
| Crawler/Ingestion | Python, requests/BeautifulSoup/cloudscraper/curl-cffi/nodriver/playwright, helper upload HDFS |
| Messaging | Kafka on Kubernetes via Strimzi manifests |
| Batch processing | PySpark, HDFS, Parquet Snappy |
| Streaming | Spark Structured Streaming, Kafka source/sink, Elasticsearch foreachBatch sinks |
| ML | Spark ML salary prediction model dùng chung cho batch và speed |
| Serving | Elasticsearch, Kibana, FastAPI |
| Orchestration/Ops | Kubernetes/Minikube, Docker, Airflow, Makefile, PowerShell/Bash scripts |
| Tests | pytest unit/schema tests trong `tests/` |

## Data Flow

### Batch Path

1. Dữ liệu crawler raw được lưu ở HDFS path `hdfs://hdfs-namenode.hdfs.svc:9000/raw/jobs`.
2. `apps/batch/jobs/raw_to_bronze.py` đọc Raw JSON/JSONL và ghi Bronze Parquet vào `/bronze/jobs`.
3. `apps/batch/jobs/bronze_to_silver.py` parse JSON-LD, chuẩn hóa salary/location/experience, dedup theo `job_id`, và ghi Silver Parquet vào `/silver/jobs`.
4. `apps/batch/jobs/train_salary_model.py` train Spark ML salary model từ toàn bộ Silver table và lưu vào `/models/salary_prediction/latest`.
5. `apps/batch/jobs/silver_to_gold.py` tạo Gold records dạng denormalized, bổ sung salary prediction fields, và ghi Parquet vào `/gold/jobs/job_market_index`.
6. `apps/batch/jobs/gold_to_elasticsearch.py` index Gold vào Elasticsearch index `gold-jobs-flat`.

### Speed Path

1. `apps/producer/crawler_jsonl_producer.py`, `apps/producer/file_to_kafka.py`, hoặc `apps/producer/fake_crawler_producer.py` publish raw events vào Kafka topic `jobs_raw`.
2. `apps/stream_etl/stream_main.py` consume `jobs_raw`, validate/parse raw schema, chuẩn hóa fields, load salary model, và score các job có salary thỏa thuận khi bật cấu hình.
3. Dòng hợp lệ được ghi vào `jobs_clean`; dòng lỗi được ghi vào `jobs_dead_letter`.
4. Realtime records và aggregations được ghi vào Elasticsearch:
   - `realtime_jobs_v1`
   - `realtime_job_counts_10m_v1`
   - `realtime_skill_counts_hourly_v1`
   - `realtime_top_skills_hourly_v1`
   - `realtime_salary_bins_hourly_v1`
5. Streaming checkpoints được lưu dưới `/mnt/spark-checkpoints` trong Kubernetes qua `infra/spark/speed-checkpoint-pvc.yaml`.

## Medallion Data Model

```text
Raw JSON/JSONL -> Bronze Parquet -> Silver Parquet -> Gold Parquet -> Elasticsearch
```

| Layer | Main path/index | Source of truth |
|---|---|---|
| Raw | `hdfs://hdfs-namenode.hdfs.svc:9000/raw/jobs` | `data/raw/raw_data_format.md` |
| Bronze | `hdfs://hdfs-namenode.hdfs.svc:9000/bronze/jobs` | `data/bronze/bronze_data_format.md` |
| Silver | `hdfs://hdfs-namenode.hdfs.svc:9000/silver/jobs` | `data/silver/silver_data_format.md` |
| Gold | `hdfs://hdfs-namenode.hdfs.svc:9000/gold/jobs/job_market_index` | `data/gold/gold_data_format.md` |
| Batch serving index | `gold-jobs-flat` | `apps/batch/jobs/gold_to_elasticsearch.py` |
| Realtime serving indexes | `realtime_*_v1` indexes | `apps/stream_etl/sinks/*.py` |

## Repository Structure

```text
bigdata-job-market/
├── apps/
│   ├── api/                         # Current FastAPI search API for gold-jobs-flat
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── search_api.py
│   ├── batch/jobs/                  # Spark batch ETL and ML training jobs
│   │   ├── raw_to_bronze.py
│   │   ├── bronze_to_silver.py
│   │   ├── silver_to_gold.py
│   │   ├── gold_to_elasticsearch.py
│   │   └── train_salary_model.py
│   ├── ingestion/                   # TopCV crawler and HDFS ingestion entrypoints
│   ├── ml/                          # Shared salary prediction pipeline
│   ├── producer/                    # Kafka producers for speed layer
│   ├── spark/                       # Utility Spark app(s)
│   ├── stream_etl/                  # Spark Structured Streaming pipeline
│   │   ├── stream_main.py
│   │   ├── transform.py
│   │   ├── normalizers.py
│   │   ├── schemas/
│   │   ├── stateful_jobs/
│   │   └── sinks/
│   └── serving/                     # Legacy Redis speed API, not current K8s serving path
├── configs/                         # Settings and logging helpers
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/                        # Data contract docs
├── docs/                            # Runbooks, integration guides, dashboard docs
├── infra/
│   ├── airflow/                     # Airflow image, RBAC, Postgres, DAGs
│   ├── docker-compose/              # Local compose files
│   ├── hdfs/                        # HDFS NameNode/DataNode manifests
│   ├── kafka/                       # Strimzi Kafka cluster and topic manifests
│   ├── kibana/saved_objects/        # Kibana dashboards
│   ├── namespaces/
│   ├── producer/                    # K8s producer job
│   ├── search/                      # Elasticsearch and Kibana manifests
│   ├── serving/                     # FastAPI deployment/service
│   └── spark/                       # Spark Dockerfile, RBAC, CronJobs, streaming Job
├── scripts/                         # Bash/PowerShell helpers and smoke tests
├── shared/                          # Shared schemas, UDFs, quality rules
├── tests/                           # pytest tests for producers, stream schemas, aggregations
├── Makefile
├── pyproject.toml
└── README.md
```

## Kubernetes Deployment Model

| Namespace | Main resources |
|---|---|
| `hdfs` | HDFS NameNode/DataNode from `infra/hdfs/hdfs.yaml` |
| `kafka` | Strimzi Kafka cluster and topics from `infra/kafka/` |
| `spark` | Spark RBAC, batch CronJobs, ML training CronJob, streaming Job, checkpoint PVCs |
| `search` | Elasticsearch and Kibana |
| `serving` | FastAPI search API deployment/service |
| `airflow` | Airflow scheduler/webserver/Postgres and DAGs |

Các manifest chính:

| Component | Manifest |
|---|---|
| Namespaces | `infra/namespaces/all.yaml` plus `infra/hdfs/hdfs.yaml` for `hdfs` |
| HDFS | `infra/hdfs/hdfs.yaml` |
| Kafka | `infra/kafka/kafka-cluster.yaml`, `infra/kafka/jobs-topics.yaml` |
| Spark image | `infra/spark/Dockerfile` |
| Batch CronJobs | `infra/spark/raw-to-bronze-cronjob.yaml`, `bronze-to-silver-cronjob.yaml`, `silver-to-gold-cronjob.yaml`, `gold-to-elasticsearch-cronjob.yaml` |
| ML training | `infra/spark/salary-model-train-cronjob.yaml` |
| Speed stream | `infra/spark/speed-stream-es-job.yaml` |
| Search | `infra/search/elasticsearch-statefulset.yaml`, `elasticsearch-service.yaml`, `kibana-deployment.yaml`, `kibana-service.yaml` |
| API | `infra/serving/job-search-api-deployment.yaml`, `job-search-api-service.yaml` |
| Airflow | `infra/airflow/*.yaml`, DAGs in `infra/airflow/dags/` |

## Main Runtime Commands

`Makefile` cung cấp các thao tác Minikube chính:

```bash
make namespaces-up
make hdfs-up
make kafka-up
make search-up
make spark-build
make api-build
make serving-up
make platform-up
make speed-k8s-up
make status
```

Các script thường dùng:

| Purpose | Script |
|---|---|
| Create Kafka topics | `scripts/create_kafka_topics.sh`, `scripts/create_kafka_topics.ps1` |
| Run fake producer | `scripts/run_fake_crawler.sh`, `scripts/run_fake_crawler.ps1` |
| Run JSONL producer | `scripts/run_crawler_jsonl_producer.sh`, `scripts/run_crawler_jsonl_producer.ps1` |
| Run speed pipeline | `scripts/run_stream_speed_layer.sh`, `scripts/run_stream_speed_layer.ps1`, `scripts/run_speed_pipeline.sh` |
| Run batch pipeline | `scripts/run_batch_pipeline.sh` |
| Smoke test speed layer | `scripts/smoke_test_speed_layer.sh`, `scripts/smoke_test_speed_layer.ps1` |
| Cleanup checkpoints | `scripts/clean_stream_etl_checkpoints.sh`, `scripts/clean_stream_etl_checkpoints.ps1` |

## API and Indexes

### Batch Search API

Đường serving hiện tại trên Kubernetes:

- Code: `apps/api/search_api.py`
- Image: `apps/api/Dockerfile`
- Deployment: `infra/serving/job-search-api-deployment.yaml`
- Service: `infra/serving/job-search-api-service.yaml`
- Elasticsearch URL: `http://elasticsearch.search.svc:9200`
- Default index: `gold-jobs-flat`

Các endpoint chính:

- `GET /health`
- `GET /jobs/search`
- `GET /jobs/{job_id}`
- `GET /stats/overview`
- `GET /suggest/languages`
- `GET /suggest/frameworks`
- `GET /suggest/provinces`

### Realtime Elasticsearch Indexes

Các sink module của speed layer trong `apps/stream_etl/sinks/` ghi:

- `realtime_jobs_v1`
- `realtime_job_counts_10m_v1`
- `realtime_skill_counts_hourly_v1`
- `realtime_top_skills_hourly_v1`
- `realtime_salary_bins_hourly_v1`

## Notes and Current Repo Caveats

- `apps/serving/api.py` là Redis-based speed API cũ. K8s serving manifests hiện tại deploy `apps/api/search_api.py`.
- `infra/kafka/jobs-topics.yaml` là topic manifest thực tế trong repo. Target `kafka-topics-up` trong `Makefile` hiện đang trỏ tới `infra/kafka/topics.yaml`, file này không tồn tại.
- `infra/namespaces/all.yaml` định nghĩa `spark`, `search`, `serving`, `kafka`, và `airflow`; namespace `hdfs` được tạo trong `infra/hdfs/hdfs.yaml`.
- Một số tài liệu cũ vẫn mô tả folder name hoặc Redis serving trước đây. Xem file này cùng `data/*/*_data_format.md` là reference kiến trúc hiện tại.
