"""Airflow DAG for orchestrating the batch job-market pipeline.

Data flow:
    TopCV crawler
        -> HDFS raw JSONL
        -> Bronze
        -> Silver
        -> Gold
        -> Elasticsearch

Tasks:
    check_hdfs
    check_elasticsearch
    crawl_jobs
    raw_to_bronze
    bronze_to_silver
    silver_to_gold
    gold_to_elasticsearch

How it works:
    Airflow creates one-off Kubernetes Jobs from suspended CronJobs in the
    spark namespace. The CronJobs contain the actual Spark/crawler runtime
    definitions; Airflow only controls execution order and monitoring.

Demo:
    Open Airflow UI and trigger DAG `job_market_batch_pipeline`.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

SPARK_NAMESPACE = "spark"

DEFAULT_ARGS = {
    "owner": "job-market",
    "depends_on_past": False,
    "retries": 1,
}


def run_spark_job_from_cronjob(
    *,
    task_id: str,
    cronjob_name: str,
    timeout_seconds: int = 1800,
) -> BashOperator:
    """
    Create a one-off Kubernetes Job from an existing Spark CronJob,
    then wait until the Job completes.

    This keeps Spark job definitions in infra/spark/*-cronjob.yaml
    and lets Airflow only orchestrate the execution order.
    """

    return BashOperator(
        task_id=task_id,
        bash_command=f"""
        set -euo pipefail

        JOB_NAME="{cronjob_name}-airflow-{{{{ ds_nodash }}}}-{{{{ task_instance.try_number }}}}"

        echo "[airflow] deleting old job if exists: $JOB_NAME"
        kubectl delete job "$JOB_NAME" -n {SPARK_NAMESPACE} --ignore-not-found=true

        echo "[airflow] creating job $JOB_NAME from cronjob/{cronjob_name}"
        kubectl create job "$JOB_NAME" \
          --from=cronjob/{cronjob_name} \
          -n {SPARK_NAMESPACE}

        echo "[airflow] waiting for job $JOB_NAME to complete"
        if ! kubectl wait \
          --for=condition=complete \
          "job/$JOB_NAME" \
          -n {SPARK_NAMESPACE} \
          --timeout={timeout_seconds}s; then

          echo "[airflow] job failed or timed out, showing pod logs"
          kubectl get pods -n {SPARK_NAMESPACE} -l job-name="$JOB_NAME"
          kubectl logs -n {SPARK_NAMESPACE} -l job-name="$JOB_NAME" --tail=200 || true
          exit 1
        fi

        echo "[airflow] job completed: $JOB_NAME"
        kubectl logs -n {SPARK_NAMESPACE} -l job-name="$JOB_NAME" --tail=80 || true
        """,
    )


with DAG(
    dag_id="job_market_batch_pipeline",
    description="Batch pipeline: Crawl -> Raw -> Bronze -> Silver -> Gold -> Elasticsearch",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz=TZ),
    schedule="0 2 * * *",
    catchup=False,
    tags=["job-market", "batch", "spark", "kubernetes"],
) as dag:

    check_hdfs = BashOperator(
        task_id="check_hdfs",
        bash_command="""
        set -euo pipefail
        echo "[airflow] checking HDFS NameNode"
        nc -z hdfs-namenode.hdfs.svc.cluster.local 9000
        """,
    )

    check_elasticsearch = BashOperator(
        task_id="check_elasticsearch",
        bash_command="""
        set -euo pipefail
        echo "[airflow] checking Elasticsearch"
        curl -fsS http://elasticsearch.search.svc.cluster.local:9200/_cluster/health
        """,
    )

    crawl_jobs = run_spark_job_from_cronjob(
        task_id="crawl_jobs",
        cronjob_name="batch-etl-crawl-jobs",
        timeout_seconds=108000,
    )

    raw_to_bronze = run_spark_job_from_cronjob(
        task_id="raw_to_bronze",
        cronjob_name="batch-etl-raw-to-bronze",
        timeout_seconds=2400,
    )

    bronze_to_silver = run_spark_job_from_cronjob(
        task_id="bronze_to_silver",
        cronjob_name="batch-etl-bronze-to-silver",
        timeout_seconds=2400,
    )

    silver_to_gold = run_spark_job_from_cronjob(
        task_id="silver_to_gold",
        cronjob_name="batch-etl-silver-to-gold",
        timeout_seconds=2400,
    )

    gold_to_elasticsearch = run_spark_job_from_cronjob(
        task_id="gold_to_elasticsearch",
        cronjob_name="batch-etl-gold-to-elasticsearch",
        timeout_seconds=2400,
    )

    [check_hdfs, check_elasticsearch] >> crawl_jobs
    crawl_jobs >> raw_to_bronze
    raw_to_bronze >> bronze_to_silver >> silver_to_gold >> gold_to_elasticsearch
