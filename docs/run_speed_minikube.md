# Run Speed Layer on Minikube

This runbook starts the current repo's speed layer end to end through Airflow.

The runtime flow is:

```text
Airflow DAG
-> check Kafka and Elasticsearch
-> create Kafka topics
-> optionally clean Spark streaming checkpoints
-> submit Spark Structured Streaming job
-> run TopCV crawler and JSONL producer
-> verify realtime Elasticsearch indexes
```

Run all commands from the repository root:

```powershell
cd "c:\Users\PHAM ANH KHOI\Projects\bigdata-job-market"
```

## 1. Start Minikube

```powershell
minikube start -p job-market --driver=docker --cpus=8 --memory=10000 --disk-size=40g
minikube addons enable storage-provisioner
minikube addons enable default-storageclass
kubectl get nodes
```

## 2. Create Namespaces

```powershell
kubectl apply -f infra\namespaces\all.yaml
kubectl get ns
```

Expected namespaces include:

```text
airflow
spark
kafka
search
```

## 3. Install Strimzi Kafka Operator

The Kafka manifests in this repo use Strimzi custom resources such as `Kafka`,
`KafkaNodePool`, and `KafkaTopic`.

Check whether the CRDs already exist:

```powershell
kubectl get crd kafkas.kafka.strimzi.io
```

If that command fails, install the operator into the `kafka` namespace:

```powershell
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl rollout status deployment/strimzi-cluster-operator -n kafka
```

## 4. Build Local Images

Build the Spark image. This image contains the `apps/*` code used by the
crawler, producer, and streaming ETL.

```powershell
minikube image build -f infra\spark\Dockerfile -t spark-job-market:latest .
```

Build the Airflow image. This image contains the DAGs and the Kubernetes
manifests copied under `/opt/airflow/infra`.

```powershell
minikube image build -f infra\airflow\Dockerfile -t job-market-airflow:2.9.3 .
```

Check both images:

```powershell
minikube image ls | Select-String "spark-job-market|job-market-airflow"
```

Rebuild `spark-job-market:latest` after changing `apps/*`. Rebuild
`job-market-airflow:2.9.3` after changing `infra/airflow/dags`,
`infra/spark`, `infra/kafka`, or `infra/producer`.

## 5. Deploy Elasticsearch

```powershell
kubectl apply -f infra\search\elasticsearch-service.yaml
kubectl apply -f infra\search\elasticsearch-statefulset.yaml
kubectl rollout status statefulset/elasticsearch -n search
```

Check health:

```powershell
kubectl exec -n search elasticsearch-0 -- curl -s http://localhost:9200/_cluster/health?pretty
```

## 6. Deploy Kafka

```powershell
kubectl apply -f infra\kafka\kafka-cluster.yaml
kubectl wait kafka/my-cluster -n kafka --for=condition=Ready --timeout=600s
kubectl get pods -n kafka
```

Kafka topics are created by the Airflow DAG, so you do not need to apply
`infra\kafka\jobs-topics.yaml` manually.

## 7. Prepare Spark Runtime

```powershell
kubectl apply -f infra\spark\rbac.yaml
kubectl apply -f infra\spark\speed-checkpoint-pvc.yaml
kubectl get pvc -n spark
```

Do not apply `infra\spark\speed-stream-es-job.yaml` manually for the normal
Airflow run. The DAG applies it.

## 8. Deploy Airflow

```powershell
kubectl apply -f infra\airflow\airflow-rbac.yaml
kubectl apply -f infra\airflow\airflow-postgres.yaml
kubectl apply -f infra\airflow\airflow.yaml
```

Wait for Airflow:

```powershell
kubectl wait --for=condition=complete job/airflow-init -n airflow --timeout=300s
kubectl rollout status deployment/airflow-scheduler -n airflow
kubectl rollout status deployment/airflow-webserver -n airflow
kubectl get pods -n airflow
```

## 9. Open Airflow UI

Keep this command running in its own terminal:

```powershell
kubectl port-forward -n airflow svc/airflow-webserver 8082:8080
```

Open:

```text
http://localhost:8082
```

Login:

```text
username: admin
password: admin
```

## 10. Trigger the Speed DAG

In Airflow UI, open:

```text
job_market_speed_layer_bootstrap
```

Trigger it with this config for a clean demo run:

```json
{
  "reset_checkpoint": true,
  "run_real_crawler": true
}
```

The crawler/producer Kubernetes Job now runs the JSONL producer in watch mode
while the crawler is running. The producer polls the crawler output folder,
publishes newly appended JSONL records to Kafka, drains briefly after the
crawler exits, then stops so the Airflow task can complete.

## 11. Watch Runtime Logs

Airflow task logs show each orchestration step. For Kubernetes-level checks:

```powershell
kubectl get pods -n spark
kubectl get jobs -n spark
kubectl get kafkatopics -n kafka
```

Follow the Spark streaming driver:

```powershell
kubectl logs -n spark -l spark-role=driver,spark-app-name=speed-stream-es --tail=200 -f
```

Follow the crawler/producer job:

```powershell
kubectl logs -n spark -l app=speed-real-crawler-producer --tail=200 -f
```

## 12. Verify Elasticsearch Indexes

```powershell
kubectl exec -n search elasticsearch-0 -- curl -s "http://localhost:9200/_cat/indices/realtime*?v"
```

Expected indexes:

```text
realtime_jobs_v1
realtime_job_counts_10m_v1
realtime_salary_bins_hourly_v1
realtime_skill_counts_hourly_v1
realtime_top_skills_hourly_v1
```

## 13. Clean Rerun

To rerun from a clean speed state through Airflow, trigger the DAG again with:

```json
{
  "reset_checkpoint": true,
  "run_real_crawler": true
}
```

If a previous streaming driver is still running, the DAG deletes old
`speed-stream-es` driver/executor pods before submitting the new streaming job.

If Kafka topics were recreated manually, always reset Spark checkpoints too;
otherwise Spark may hold old Kafka offsets.
