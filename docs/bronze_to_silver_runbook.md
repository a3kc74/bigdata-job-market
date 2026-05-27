# Runbook: bronze_to_silver ETL Job

How to run the `bronze_to_silver.py` Spark ETL job.
This job reads Bronze Parquet from HDFS and writes Silver Parquet back to HDFS.

---

## How to Run — LOCAL (spark-submit)

Use this when running on a local machine with Spark installed directly.

```bash
# Incremental: process a specific date
spark-submit apps/batch/jobs/bronze_to_silver.py --date 2026-04-30

# Full load: process all available partitions in /bronze/jobs/
spark-submit apps/batch/jobs/bronze_to_silver.py
```

> **Note:** For local mode, update paths in `bronze_to_silver.py` to point to your local HDFS:
> ```python
> BRONZE_BASE_PATH = "hdfs://localhost:9000/bronze/jobs"
> SILVER_BASE_PATH = "hdfs://localhost:9000/silver/jobs"
> ```

---

## How to Trigger Manually on Kubernetes

Use this to run the job immediately without waiting for the CronJob schedule.

```bash
# Trigger a one-off job from the CronJob definition
kubectl create job --from=cronjob/batch-etl-bronze-to-silver \
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
| **Input** | `hdfs://hdfs-namenode.hdfs.svc:9000/bronze/jobs/ingest_date=YYYY-MM-DD/` |
| **Output** | `hdfs://hdfs-namenode.hdfs.svc:9000/silver/jobs/ingest_date=YYYY-MM-DD/` |
| **Format** | Input: Parquet — Output: Parquet (Snappy) |

---

## Data Flow

```
/bronze/jobs/ingest_date=YYYY-MM-DD/*.parquet
    -> Cleanse and standardize text fields (e.g., job descriptions, titles).
    -> Extract and normalize key entities (e.g., company names, locations).
    -> Perform data quality checks (e.g., filter out records with missing critical fields).
    -> Aggregate or enrich data as needed.
    -> write Parquet /silver/jobs/ingest_date=YYYY-MM-DD/
```

---

## Schema Reference

- Input schema: `data/bronze/bronze_data_format.md`
- Output schema: `data/silver/silver_data_format.md`
