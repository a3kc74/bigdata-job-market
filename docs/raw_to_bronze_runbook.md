# Runbook: raw_to_bronze ETL Job

How to run the `raw_to_bronze.py` Spark ETL job.
This job reads raw JSONL from HDFS and writes Bronze Parquet back to HDFS.

---

## How to Run — LOCAL (spark-submit)

Use this when running on a local machine with Spark installed directly.

```bash
# Incremental: process a specific date
spark-submit apps/batch/jobs/raw_to_bronze.py --date 2026-04-30

# Full load: process all available partitions in /raw/jobs/
spark-submit apps/batch/jobs/raw_to_bronze.py
```

> **Note:** For local mode, update `RAW_BASE_PATH` in `raw_to_bronze.py` to point to your local HDFS:
> ```python
> RAW_BASE_PATH    = "hdfs://localhost:9000/raw/jobs"
> BRONZE_BASE_PATH = "hdfs://localhost:9000/bronze/jobs"
> ```

---

## How to Trigger Manually on Kubernetes

Use this to run the job immediately without waiting for the CronJob schedule.

```bash
# Trigger a one-off job from the CronJob definition
kubectl create job --from=cronjob/batch-etl-raw-to-bronze \
    manual-$(date +%Y%m%d) -n spark

# Watch the job status
kubectl get jobs -n spark -w

# Stream logs from the driver pod
kubectl logs -f <driver-pod-name> -n spark
```

> For the full Kubernetes setup guide (first-time setup, restart procedures, monitoring),
> see: `docs/spark_on_minikube.md`

---

## Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `--date` | `YYYY-MM-DD` | No | Process a single date partition. Omit for full load. |

---

## Input / Output

| | Path |
|---|---|
| **Input** | `hdfs://hdfs-namenode.hdfs.svc:9000/raw/jobs/ingest_date=YYYY-MM-DD/` |
| **Output** | `hdfs://hdfs-namenode.hdfs.svc:9000/bronze/jobs/ingest_date=YYYY-MM-DD/` |
| **Format** | Input: JSONL — Output: Parquet (Snappy) |

---

## Data Flow

```
/raw/jobs/ingest_date=YYYY-MM-DD/*.jsonl
    -> flatten payload fields
    -> cast timestamps (Unix ms -> Timestamp)
    -> merge skillsNeeded + skillsShouldHave -> skills
    -> add metadata (record_version, is_deleted, crawl_domain, ingest_date)
    -> add count metrics (description_count, skills_count, ...)
    -> dedup: per (job_id, hash_content), keep latest ingest_ts
    -> write Parquet /bronze/jobs/ingest_date=YYYY-MM-DD/
```

---

## Schema Reference

- Input schema: `data/raw/raw_data_format.md`
- Output schema: `data/bronze/bronze_data_format.md`
