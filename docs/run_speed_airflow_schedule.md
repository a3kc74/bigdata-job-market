# Run Speed Airflow Schedule

This runbook enables the scheduled speed pipeline on Minikube. The Airflow DAG
`job_market_speed_layer_bootstrap` now runs every 20 minutes and allows only one
active DAG run at a time.

The scheduled flow is:

```text
Airflow schedule, every 20 minutes
-> check Kafka and Elasticsearch
-> ensure Kafka topics
-> keep Spark streaming running, or submit it if missing
-> run speed-real-crawler-producer
-> verify realtime Elasticsearch indexes
```

## Prerequisites

Follow `docs/run_speed_minikube.md` through the Minikube, Kafka, Elasticsearch,
Spark runtime, and Airflow deployment steps.

The scheduled run expects:

```text
spark-job-market:latest
job-market-airflow:2.9.3
spark-speed-checkpoints-pvc
my-cluster Kafka
Elasticsearch in the search namespace
```

## Rebuild Images

The schedule change is in the Airflow DAG, and the crawler Job manifest is copied
into the Airflow image. Rebuild the Airflow image after this change:

```powershell
minikube image build -f infra\airflow\Dockerfile -t job-market-airflow:2.9.3 .
kubectl rollout restart deployment/airflow-scheduler -n airflow
kubectl rollout restart deployment/airflow-webserver -n airflow
kubectl rollout status deployment/airflow-scheduler -n airflow
kubectl rollout status deployment/airflow-webserver -n airflow
```

Rebuild the Spark image only if you also changed `apps/*` or Spark runtime code:

```powershell
minikube image build -f infra\spark\Dockerfile -t spark-job-market:latest .
```

## Enable the Schedule

Open Airflow:

```powershell
kubectl port-forward -n airflow svc/airflow-webserver 8082:8080
```

Then browse to:

```text
http://localhost:8082
```

Login:

```text
username: admin
password: admin
```

In the Airflow UI:

1. Open `job_market_speed_layer_bootstrap`.
2. Unpause the DAG.
3. Leave scheduled runs with the default params:

```json
{
  "reset_checkpoint": false,
  "run_real_crawler": true
}
```

Do not use `reset_checkpoint=true` for normal scheduled runs. That option is for
clean demos or recovery after recreating Kafka topics.

## Runtime Settings

The crawler-producer Job is tuned for a target runtime under 10 minutes:

```text
SPEED_CRAWL_MAX_PAGES=8
SPEED_UPDATED_WITHIN_MINUTES=20
SPEED_LIST_PAGES_PER_CHUNK=4
SPEED_DETAIL_BATCH_SIZE=25
CRAWLER_JSONL_POLL_SECONDS=1
PRODUCER_DRAIN_SECONDS=5
HARVESTER_BREAKER_SLEEP_SECONDS=30
```

The Kubernetes Job has:

```text
activeDeadlineSeconds=900
terminationGracePeriodSeconds=60
```

Airflow has:

```text
schedule=*/20 * * * *
catchup=False
max_active_runs=1
```

This means a new scheduled run will not overlap a previous active DAG run.

## Watch Airflow Schedule

Check scheduler and DAG status:

```powershell
kubectl logs -n airflow deployment/airflow-scheduler --tail=200 -f
```

List recent task pods/logs from the Airflow UI, or use the UI task log pages for:

```text
check_kafka_cluster
check_elasticsearch
ensure_kafka_topics
reset_checkpoint_if_requested
submit_speed_streaming_job
run_real_crawler_if_requested
verify_realtime_indices
```

The key scheduled task is:

```text
run_real_crawler_if_requested
```

## Watch Crawler-Producer Logs

Follow the current crawler-producer Job:

```powershell
kubectl logs -n spark -l app=speed-real-crawler-producer --tail=200 -f
```

Check Job and pod status:

```powershell
kubectl get jobs -n spark
kubectl get pods -n spark -l app=speed-real-crawler-producer -o wide
kubectl describe job -n spark speed-real-crawler-producer
```

If the Job exceeds 900 seconds, Kubernetes marks it failed because of
`activeDeadlineSeconds`.

## Watch Spark Streaming Logs

The DAG skips Spark streaming resubmit when a `speed-stream-es` driver is already
running. Follow the running driver:

```powershell
kubectl logs -n spark -l spark-role=driver,spark-app-name=speed-stream-es --tail=200 -f
```

Check Spark pods:

```powershell
kubectl get pods -n spark -l spark-app-name=speed-stream-es -o wide
```

## Verify Elasticsearch

Check realtime indexes:

```powershell
kubectl exec -n search elasticsearch-0 -- curl -s "http://localhost:9200/_cat/indices/realtime*?v"
```

Check recent realtime job documents:

```powershell
kubectl exec -n search elasticsearch-0 -- curl -s "http://localhost:9200/realtime_jobs_v1/_search?size=5&sort=ingested_at:desc"
```

## Troubleshooting

If the crawler Job repeatedly times out:

- Check whether `Số link MỚI cần cào ở Pha 2` is too high in crawler logs.
- Reduce `SPEED_CRAWL_MAX_PAGES` or `SPEED_UPDATED_WITHIN_MINUTES`.
- Check for repeated block/cookie recovery messages.
- Keep `max_active_runs=1`; do not run overlapping crawler-producer Jobs against
  the shared cache files.

If the scheduled DAG runs but no crawler starts:

- Confirm `run_real_crawler=true` in the DAG params.
- Check the `run_real_crawler_if_requested` task log.
- Check whether a previous DAG run is still active.

