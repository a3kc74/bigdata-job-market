"""Airflow DAG for bootstrapping the job-market speed layer.

Data flow:
    Kafka jobs_raw
        -> Spark Structured Streaming
        -> Elasticsearch realtime indexes

Realtime indexes:
    realtime_jobs_v1
    realtime_job_counts_10m_v1
    realtime_salary_bins_hourly_v1
    realtime_skill_counts_hourly_v1
    realtime_top_skills_hourly_v1

How it works:
    Airflow checks Kafka and Elasticsearch, ensures Kafka topics exist,
    optionally resets Spark Streaming checkpoints for demo runs, submits the
    Spark speed-layer Kubernetes Job, optionally runs the fake producer, and
    verifies that realtime Elasticsearch indexes are available.

Important:
    The Spark streaming job is long-running. Airflow only submits and verifies
    it; Airflow should not wait for the streaming job to complete.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.models.param import Param
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
    dag_id="job_market_speed_layer_bootstrap",
    description="Bootstrap speed layer: Kafka -> Spark Streaming -> Elasticsearch",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz=TZ),
    schedule=None,
    catchup=False,
    tags=["job-market", "speed-layer", "spark", "kafka", "elasticsearch"],
    params={
        "reset_checkpoint": Param(
            False,
            type="boolean",
            description="Reset Spark Streaming checkpoint before submitting the speed layer.",
        ),
        "run_real_crawler": Param(
            True,
            type="boolean",
            description="Run real TopCV speed crawler after submitting the streaming job.",
        ),
    },
) as dag:

    check_kafka_cluster = BashOperator(
        task_id="check_kafka_cluster",
        bash_command=f"""
        set -euo pipefail

        echo "[airflow-speed] checking Kafka pods"
        kubectl get pods -n {KAFKA_NAMESPACE}

        echo "[airflow-speed] checking Kafka cluster resources"
        kubectl get kafka -n {KAFKA_NAMESPACE} || true
        kubectl get kafkatopics -n {KAFKA_NAMESPACE} || true
        """,
    )

    check_elasticsearch = BashOperator(
        task_id="check_elasticsearch",
        bash_command=f"""
        set -euo pipefail

        echo "[airflow-speed] checking Elasticsearch cluster health"
        curl -fsS "{ES_URL}/_cluster/health?pretty"
        """,
    )

    ensure_kafka_topics = BashOperator(
        task_id="ensure_kafka_topics",
        bash_command=f"""
        set -euo pipefail

        echo "[airflow-speed] applying Kafka topics manifest"
        kubectl apply -f {MANIFEST_ROOT}/kafka/jobs-topics.yaml

        echo "[airflow-speed] waiting for required Kafka topics"
        for topic in jobs-raw jobs-clean jobs-dead-letter; do
          echo "[airflow-speed] checking topic: $topic"
          kubectl get kafkatopic "$topic" -n {KAFKA_NAMESPACE}
        done
        """,
    )

    reset_checkpoint_if_requested = BashOperator(
        task_id="reset_checkpoint_if_requested",
        bash_command=f"""
        set -euo pipefail

        RESET_CHECKPOINT="{{{{ params.reset_checkpoint }}}}"

        case "$RESET_CHECKPOINT" in
          true|True|1|yes|Yes)
            echo "[airflow-speed] reset_checkpoint=true, cleaning checkpoint PVC"
            ;;
          *)
            echo "[airflow-speed] reset_checkpoint=false, skipping checkpoint cleanup"
            exit 0
            ;;
        esac

        kubectl delete pod -n {SPARK_NAMESPACE} spark-checkpoint-cleaner --ignore-not-found=true
        kubectl apply -f {MANIFEST_ROOT}/spark/pvc-cleaner.yaml

        echo "[airflow-speed] waiting for spark-checkpoint-cleaner to finish"
        for i in $(seq 1 45); do
          PHASE="$(kubectl get pod spark-checkpoint-cleaner -n {SPARK_NAMESPACE} -o jsonpath='{{.status.phase}}' 2>/dev/null || echo Missing)"
          echo "[airflow-speed] cleaner phase=$PHASE"

          if [ "$PHASE" = "Succeeded" ]; then
            kubectl logs -n {SPARK_NAMESPACE} spark-checkpoint-cleaner || true
            kubectl delete pod -n {SPARK_NAMESPACE} spark-checkpoint-cleaner --ignore-not-found=true
            exit 0
          fi

          if [ "$PHASE" = "Failed" ]; then
            kubectl logs -n {SPARK_NAMESPACE} spark-checkpoint-cleaner || true
            exit 1
          fi

          sleep 2
        done

        echo "[airflow-speed] checkpoint cleaner timed out"
        kubectl logs -n {SPARK_NAMESPACE} spark-checkpoint-cleaner || true
        exit 1
        """,
    )

    submit_speed_streaming_job = BashOperator(
        task_id="submit_speed_streaming_job",
        bash_command=f"""
        set -euo pipefail

        echo "[airflow-speed] deleting old speed submit job"
        kubectl delete job -n {SPARK_NAMESPACE} speed-stream-es-submit --ignore-not-found=true

        echo "[airflow-speed] deleting old speed stream driver/executor pods"
        kubectl delete pod -n {SPARK_NAMESPACE} -l spark-app-name=speed-stream-es --ignore-not-found=true || true

        echo "[airflow-speed] applying speed streaming job manifest"
        kubectl apply -f {MANIFEST_ROOT}/spark/speed-stream-es-job.yaml

        echo "[airflow-speed] waiting for Spark streaming driver pod to run"
        for i in $(seq 1 90); do
          kubectl get pods -n {SPARK_NAMESPACE} | grep -E "speed-stream-es|job-market-speed-phase3" || true

          DRIVER_LINE="$(
            kubectl get pods -n {SPARK_NAMESPACE} --no-headers 2>/dev/null \
              | grep -E "speed-stream-es-.*-driver" \
              | grep "Running" \
              || true
          )"

          if [ -n "$DRIVER_LINE" ]; then
            echo "[airflow-speed] streaming driver is running:"
            echo "$DRIVER_LINE"
            break
          fi

          sleep 5
        done

        if [ -z "${{DRIVER_LINE:-}}" ]; then
          echo "[airflow-speed] streaming driver did not reach Running state"
          kubectl get pods -n {SPARK_NAMESPACE}
          kubectl logs -n {SPARK_NAMESPACE} -l job-name=speed-stream-es-submit --tail=200 || true
          exit 1
        fi

        echo "[airflow-speed] waiting for Spark driver pod: speed-stream-es"

        DRIVER_POD="$(kubectl get pods -n {SPARK_NAMESPACE} \\
          -l spark-role=driver,spark-app-name=speed-stream-es \\
          -o jsonpath='{{{{.items[0].metadata.name}}}}' 2>/dev/null || true)"

        echo "[airflow-speed] streaming driver pod: $DRIVER_POD"

        kubectl get pods -n {SPARK_NAMESPACE} -l spark-app-name=speed-stream-es -o wide

        echo "[airflow-speed] driver log snapshot:"
        kubectl logs -n {SPARK_NAMESPACE} "$DRIVER_POD" --all-containers=true --tail=120 || true

        echo "[airflow-speed] to follow streaming logs manually:"
        echo "kubectl logs -n {SPARK_NAMESPACE} $DRIVER_POD -f --tail=200"
        """,
    )

    run_real_crawler_if_requested = BashOperator(
        task_id="run_real_crawler_if_requested",
        bash_command=f"""
        set -euo pipefail

        RUN_REAL_CRAWLER="{{{{ params.run_real_crawler }}}}"

        case "$RUN_REAL_CRAWLER" in
          true|True|1|yes|Yes)
            echo "[airflow-speed] run_real_crawler=true, crawling TopCV and publishing to Kafka"
            ;;
          *)
            echo "[airflow-speed] run_real_crawler=false, skipping real crawler"
            exit 0
            ;;
        esac

        kubectl delete job -n {SPARK_NAMESPACE} speed-real-crawler-producer --ignore-not-found=true
        kubectl apply -f {MANIFEST_ROOT}/producer/speed-real-crawler-producer-job.yaml

        echo "[airflow-speed] waiting for real crawler producer to complete"
        if ! kubectl wait \
          --for=condition=complete \
          job/speed-real-crawler-producer \
          -n {SPARK_NAMESPACE} \
          --timeout=7200s; then

          echo "[airflow-speed] real crawler producer failed or timed out"
          kubectl get pods -n {SPARK_NAMESPACE} -l app=speed-real-crawler-producer
          kubectl logs -n {SPARK_NAMESPACE} -l app=speed-real-crawler-producer --tail=200 || true
          exit 1
        fi

        kubectl logs -n {SPARK_NAMESPACE} -l app=speed-real-crawler-producer --tail=100 || true
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

        echo "[airflow-speed] waiting for realtime Elasticsearch indexes"

        for i in $(seq 1 90); do
          MISSING=""

          for index in $EXPECTED_INDICES; do
            if ! curl -fsS "{ES_URL}/$index" >/dev/null; then
              MISSING="$MISSING $index"
            fi
          done

          if [ -z "$MISSING" ]; then
            echo "[airflow-speed] all realtime indexes exist"
            curl -fsS "{ES_URL}/_cat/indices/realtime*?v"
            exit 0
          fi

          echo "[airflow-speed] missing indexes:$MISSING"
          sleep 5
        done

        echo "[airflow-speed] realtime indexes were not all created in time"
        curl -fsS "{ES_URL}/_cat/indices?v" || true
        exit 1
        """,
    )

    [check_kafka_cluster, check_elasticsearch] >> ensure_kafka_topics
    ensure_kafka_topics >> reset_checkpoint_if_requested
    reset_checkpoint_if_requested >> submit_speed_streaming_job
    submit_speed_streaming_job >> run_real_crawler_if_requested
    run_real_crawler_if_requested >> verify_realtime_indices
