"""Scheduled Airflow DAG for running the real TopCV speed crawler producer.

This DAG runs every 20 minutes:
1. Check Kafka cluster resources
2. Check Elasticsearch health
3. Ensure Kafka topics exist
4. Ensure Spark Streaming driver is running, or submit it if missing
5. Run the TopCV speed crawler producer
6. Verify realtime Elasticsearch indexes exist

It does not reset Spark checkpoints.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

SPARK_NAMESPACE = "spark"
KAFKA_NAMESPACE = "kafka"
ES_URL = "http://elasticsearch.search.svc.cluster.local:9200"

MANIFEST_ROOT = "/opt/airflow/infra"

DEFAULT_ARGS = {
    "owner": "job-market",
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    dag_id="job_market_speed_real_crawler",
    description="Scheduled real TopCV speed crawler producer: TopCV -> Kafka jobs_raw",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz=TZ),
    schedule="*/20 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["job-market", "speed-layer", "crawler", "kafka"],
) as dag:

    check_kafka_cluster = BashOperator(
        task_id="check_kafka_cluster",
        bash_command=f"""
        set -euo pipefail

        echo "[airflow-speed-crawler] checking Kafka pods"
        kubectl get pods -n {KAFKA_NAMESPACE}

        echo "[airflow-speed-crawler] checking Kafka cluster resources"
        kubectl get kafka -n {KAFKA_NAMESPACE} || true
        kubectl get kafkatopics -n {KAFKA_NAMESPACE} || true
        """,
    )

    check_elasticsearch = BashOperator(
        task_id="check_elasticsearch",
        bash_command=f"""
        set -euo pipefail

        echo "[airflow-speed-crawler] checking Elasticsearch cluster health"
        curl -fsS "{ES_URL}/_cluster/health?pretty"
        """,
    )

    ensure_kafka_topics = BashOperator(
        task_id="ensure_kafka_topics",
        bash_command=f"""
        set -euo pipefail

        echo "[airflow-speed-crawler] applying Kafka topics manifest"
        kubectl apply -f {MANIFEST_ROOT}/kafka/jobs-topics.yaml

        echo "[airflow-speed-crawler] waiting for required Kafka topics"
        for topic in jobs-raw jobs-clean jobs-dead-letter; do
          echo "[airflow-speed-crawler] checking topic: $topic"
          kubectl get kafkatopic "$topic" -n {KAFKA_NAMESPACE}
        done
        """,
    )

    ensure_speed_streaming_driver = BashOperator(
        task_id="ensure_speed_streaming_driver",
        bash_command=f"""
        set -euo pipefail

        echo "[airflow-speed-crawler] ensuring Spark Streaming driver is running"

        EXISTING_DRIVER="$(
          kubectl get pods -n {SPARK_NAMESPACE} \
            -l spark-role=driver,spark-app-name=speed-stream-es \
            --field-selector=status.phase=Running \
            --no-headers 2>/dev/null \
            | awk 'NR==1 {{print $1}}' \
            || true
        )"

        if [ -n "$EXISTING_DRIVER" ]; then
          echo "[airflow-speed-crawler] existing streaming driver is running: $EXISTING_DRIVER"
          kubectl get pods -n {SPARK_NAMESPACE} -l spark-app-name=speed-stream-es -o wide
          exit 0
        fi

        echo "[airflow-speed-crawler] no running streaming driver found"
        echo "[airflow-speed-crawler] submitting speed-stream-es job"

        kubectl delete job -n {SPARK_NAMESPACE} speed-stream-es-submit --ignore-not-found=true
        kubectl delete pod -n {SPARK_NAMESPACE} -l spark-app-name=speed-stream-es --ignore-not-found=true || true
        kubectl apply -f {MANIFEST_ROOT}/spark/speed-stream-es-job.yaml

        echo "[airflow-speed-crawler] waiting for Spark streaming driver pod to run"
        for i in $(seq 1 90); do
          DRIVER_LINE="$(
            kubectl get pods -n {SPARK_NAMESPACE} --no-headers 2>/dev/null \
              | grep -E "speed-stream-es-.*-driver" \
              | grep "Running" \
              || true
          )"

          if [ -n "$DRIVER_LINE" ]; then
            echo "[airflow-speed-crawler] streaming driver is running:"
            echo "$DRIVER_LINE"
            exit 0
          fi

          kubectl get pods -n {SPARK_NAMESPACE} | grep -E "speed-stream-es|job-market-speed-phase3" || true
          sleep 5
        done

        echo "[airflow-speed-crawler] ERROR: streaming driver did not reach Running state"
        kubectl get pods -n {SPARK_NAMESPACE}
        kubectl logs -n {SPARK_NAMESPACE} -l job-name=speed-stream-es-submit --tail=200 || true
        exit 1
        """,
    )

    run_real_crawler = BashOperator(
        task_id="run_real_crawler",
        execution_timeout=timedelta(minutes=15),
        bash_command=f"""
        set -euo pipefail

        echo "[airflow-speed-crawler] deleting old crawler job"
        kubectl delete job -n {SPARK_NAMESPACE} speed-real-crawler-producer --ignore-not-found=true

        echo "[airflow-speed-crawler] applying real crawler producer job"
        kubectl apply -f {MANIFEST_ROOT}/producer/speed-real-crawler-producer-job.yaml

        echo "[airflow-speed-crawler] waiting for real crawler producer to complete"
        if ! kubectl wait \
          --for=condition=complete \
          job/speed-real-crawler-producer \
          -n {SPARK_NAMESPACE} \
          --timeout=900s; then

          echo "[airflow-speed-crawler] ERROR: real crawler producer failed or timed out"
          kubectl get pods -n {SPARK_NAMESPACE} -l app=speed-real-crawler-producer
          kubectl logs -n {SPARK_NAMESPACE} -l app=speed-real-crawler-producer --tail=300 || true
          exit 1
        fi

        echo "[airflow-speed-crawler] real crawler producer completed"
        kubectl logs -n {SPARK_NAMESPACE} -l app=speed-real-crawler-producer --tail=200 || true
        """,
    )

    verify_realtime_indices = BashOperator(
        task_id="verify_realtime_indices",
        bash_command=f"""
        set -euo pipefail

        EXPECTED_INDICES="
        realtime_jobs_v1
        realtime_job_counts_10m_v1
        realtime_salary_bins_hourly_v1
        realtime_skill_counts_hourly_v1
        realtime_top_skills_hourly_v1
        "

        echo "[airflow-speed-crawler] waiting for realtime Elasticsearch indexes"

        for i in $(seq 1 90); do
          MISSING=""

          for index in $EXPECTED_INDICES; do
            if ! curl -fsS "{ES_URL}/$index" >/dev/null; then
              MISSING="$MISSING $index"
            fi
          done

          if [ -z "$MISSING" ]; then
            echo "[airflow-speed-crawler] all realtime indexes exist"
            curl -fsS "{ES_URL}/_cat/indices/realtime*?v"
            exit 0
          fi

          echo "[airflow-speed-crawler] missing indexes:$MISSING"
          sleep 5
        done

        echo "[airflow-speed-crawler] realtime indexes were not all created in time"
        curl -fsS "{ES_URL}/_cat/indices?v" || true
        exit 1
        """,
    )

    [check_kafka_cluster, check_elasticsearch] >> ensure_kafka_topics
    ensure_kafka_topics >> ensure_speed_streaming_driver
    ensure_speed_streaming_driver >> run_real_crawler
    run_real_crawler >> verify_realtime_indices