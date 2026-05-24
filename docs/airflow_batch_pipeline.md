# Airflow Batch Pipeline

## 1. Mục tiêu

Tài liệu này hướng dẫn triển khai và chạy Airflow cho nhánh batch của hệ thống phân tích thị trường việc làm.

Batch pipeline đầy đủ:

```text
TopCV crawler
  -> HDFS raw JSONL
  -> Bronze
  -> Silver
  -> Gold
  -> Elasticsearch
```

Airflow không trực tiếp xử lý dữ liệu. Airflow đóng vai trò điều phối các Kubernetes CronJob/Spark jobs theo đúng thứ tự.

## 2. DAG Airflow

Tên DAG:

```
job_market_batch_pipeline
```

File DAG:

```
infra/airflow/dags/job_market_batch_pipeline.py
```

Các task trong DAG:

```
check_hdfs
check_elasticsearch
crawl_jobs
raw_to_bronze
bronze_to_silver
silver_to_gold
gold_to_elasticsearch
```

Luồng chạy:

```
check_hdfs + check_elasticsearch
    -> crawl_jobs
    -> raw_to_bronze
    -> bronze_to_silver
    -> silver_to_gold
    -> gold_to_elasticsearch
```

## 3. Các file liên quan

**Airflow:**
- infra/airflow/Dockerfile
- infra/airflow/requirements.txt
- infra/airflow/airflow.yaml
- infra/airflow/airflow-rbac.yaml
- infra/airflow/dags/job_market_batch_pipeline.py

**Spark batch jobs:**
- apps/batch/jobs/raw_to_bronze.py
- apps/batch/jobs/bronze_to_silver.py
- apps/batch/jobs/silver_to_gold.py
- apps/batch/jobs/gold_to_elasticsearch.py

**Batch crawler:**
- apps/ingestion/batch_crawler.py
- infra/spark/batch-crawler-cronjob.yaml

**Spark CronJobs được Airflow gọi:**
- batch-etl-crawl-jobs
- batch-etl-raw-to-bronze
- batch-etl-bronze-to-silver
- batch-etl-silver-to-gold
- batch-etl-gold-to-elasticsearch

Các CronJob này để `suspend: true` vì Airflow là scheduler chính. Airflow chỉ dùng chúng như template để tạo Kubernetes Job thủ công.

## 4. Chuẩn bị Minikube

Kiểm tra Minikube:

```bash
minikube status
kubectl get nodes
```

Nếu Minikube chưa chạy:

```bash
minikube start -p job-market --driver=docker --cpus=8 --memory=10000 --disk-size=40g
kubectl config use-context job-market
```

Kiểm tra node:

```bash
kubectl get nodes
```

Kỳ vọng:

```
NAME         STATUS   ROLES           VERSION
job-market   Ready    control-plane   ...
```

## 5. Tạo namespace

Apply namespace:

```bash
kubectl apply -f infra\namespaces\all.yaml
```

Kiểm tra:

```bash
kubectl get ns
```

Cần có các namespace:

```
hdfs
search
spark
kafka
serving
airflow
```

Nếu namespace airflow chưa có, thêm vào `infra/namespaces/all.yaml`:

```yaml
---
apiVersion: v1
kind: Namespace
metadata:
  name: airflow
```

## 6. Deploy các hạ tầng cần thiết

**HDFS**

Nếu HDFS chưa chạy:

```bash
kubectl apply -f infra\hdfs\
```

Kiểm tra:

```bash
kubectl get pods -n hdfs
```

Cần thấy:

```
hdfs-namenode-0   Running
hdfs-datanode-0   Running
```

**Elasticsearch**

Nếu Elasticsearch chưa chạy:

```bash
kubectl apply -f infra\search\
```

Kiểm tra:

```bash
kubectl get pods -n search
```

Cần thấy:

```
elasticsearch-0   Running
kibana-...        Running
```

Port-forward Elasticsearch:

```bash
kubectl port-forward svc/elasticsearch 9200:9200 -n search
```

Mở terminal khác kiểm tra:

```bash
curl.exe "http://localhost:9200"
```

## 7. Build Spark image

Vì batch crawler và các batch jobs chạy bằng image `spark-job-market:latest`, cần build image sau khi sửa code:

```bash
minikube image build -f infra\spark\Dockerfile -t spark-job-market:latest .
```

## 8. Apply Spark CronJobs

Apply các CronJob batch:

```bash
kubectl apply -f infra\spark\raw-to-bronze-cronjob.yaml
kubectl apply -f infra\spark\bronze-to-silver-cronjob.yaml
kubectl apply -f infra\spark\silver-to-gold-cronjob.yaml
kubectl apply -f infra\spark\gold-to-elasticsearch-cronjob.yaml
kubectl apply -f infra\spark\batch-crawler-cronjob.yaml
```

Kiểm tra:

```bash
kubectl get cronjobs -n spark
```

Kỳ vọng:

```
batch-etl-crawl-jobs
batch-etl-raw-to-bronze
batch-etl-bronze-to-silver
batch-etl-silver-to-gold
batch-etl-gold-to-elasticsearch
```

Các CronJob có thể ở trạng thái:

```
SUSPEND=True
```

Điều này là đúng, vì Airflow sẽ trigger job thủ công.

## 9. Test riêng batch crawler

Tạo một job thủ công từ CronJob crawler:

```bash
kubectl delete job -n spark batch-etl-crawl-jobs-manual --ignore-not-found=true
kubectl create job batch-etl-crawl-jobs-manual --from=cronjob/batch-etl-crawl-jobs -n spark
```

Theo dõi log:

```bash
kubectl logs -n spark -l job-name=batch-etl-crawl-jobs-manual --tail=200 -f
```

Kỳ vọng log có dạng:

```
[batch_crawler] wrote 50 raw records to hdfs://hdfs-namenode.hdfs.svc.cluster.local:9000/raw/jobs/ingest_date=...
```

Kiểm tra file raw trong HDFS:

```bash
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /raw/jobs
```

Ví dụ kết quả:

```
/raw/jobs/ingest_date=2026-05-17/raw_jobs_20260517T071132Z.jsonl
```

## 10. Deploy Airflow

Apply RBAC:

```bash
kubectl apply -f infra\airflow\airflow-rbac.yaml
```

Build Airflow image:

```bash
minikube image build -f infra\airflow\Dockerfile -t job-market-airflow:2.9.3 .
```

Apply Airflow:

```bash
kubectl apply -f infra\airflow\airflow.yaml
```

Kiểm tra pod:

```bash
kubectl get pods -n airflow
```

Kỳ vọng:

```
airflow-postgres-...     Running
airflow-init-...         Completed
airflow-scheduler-...    Running
airflow-webserver-...    Running
```

## 11. Mở Airflow UI

Port-forward Airflow webserver:

```bash
kubectl port-forward svc/airflow-webserver 8082:8080 -n airflow
```

Mở trình duyệt:

```
http://localhost:8082
```

Tài khoản mặc định:

```
admin / admin
```

Tìm DAG:

```
job_market_batch_pipeline
```

Trigger DAG thủ công bằng nút Play.

## 12. Theo dõi DAG batch

Trong Airflow UI, DAG thành công khi toàn bộ task đều xanh:

```
check_hdfs
check_elasticsearch
crawl_jobs
raw_to_bronze
bronze_to_silver
silver_to_gold
gold_to_elasticsearch
```

Có thể kiểm tra Kubernetes Jobs do Airflow tạo:

```bash
kubectl get jobs -n spark | Select-String "batch-etl"
```

Kiểm tra pods:

```bash
kubectl get pods -n spark | Select-String "batch-etl"
```

## 13. Kiểm tra kết quả HDFS

Sau khi DAG chạy xong:

```bash
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /raw/jobs
```

Ví dụ:

```
/raw/jobs/ingest_date=2026-05-08/jobs_2026-05-08.jsonl
/raw/jobs/ingest_date=2026-05-17/raw_jobs_20260517T063417Z.jsonl
/raw/jobs/ingest_date=2026-05-17/raw_jobs_20260517T071132Z.jsonl
```

Điều này chứng minh crawler đã ghi raw JSONL mới vào HDFS.

## 14. Kiểm tra kết quả Elasticsearch

Nếu chưa port-forward Elasticsearch:

```bash
kubectl port-forward svc/elasticsearch 9200:9200 -n search
```

Kiểm tra count index Gold:

```bash
curl.exe "http://localhost:9200/gold-jobs-flat/_count?pretty"
```

Ví dụ:

```json
{
  "count": 3613
}
```

Kiểm tra index:

```bash
curl.exe "http://localhost:9200/_cat/indices/gold-jobs-flat?v"
```
