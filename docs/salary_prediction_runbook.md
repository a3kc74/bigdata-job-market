# Runbook End-to-End: Batch Layer, Spark ML Salary Prediction, Speed Layer

Tài liệu này hướng dẫn chạy từ đầu đến cuối pipeline Big Data Job Market có tích
hợp Spark ML dự đoán lương. File này thiên về thao tác vận hành: chạy lệnh nào,
kiểm tra ở đâu, khi sửa code thì rerun từ bước nào.

## 1. Luồng Tổng Thể

```text
Batch layer:

TopCV crawler hoặc file raw có sẵn
-> HDFS /raw/jobs
-> Spark raw_to_bronze
-> Spark bronze_to_silver
-> Spark ML train salary model
-> Spark silver_to_gold
-> Elasticsearch gold-jobs-flat
-> Kibana

Speed layer:

TopCV crawler speed mode
-> JSONL producer
-> Kafka jobs_raw
-> Spark Structured Streaming
-> load salary model latest
-> Elasticsearch realtime_jobs_v1
-> Kibana
```

## 2. Luật Nghiệp Vụ Salary Prediction

Model dự đoán:

```text
X = title, skills, experience, location, company, remote
y = salary thật của những job có lương công khai
f(x) = salary dự đoán cho job thật sự không có min/max lương
```

Luật quan trọng nhất:

```text
Chỉ dự đoán khi:
salary_min_vnd IS NULL
AND salary_max_vnd IS NULL
AND model load thành công
```

Nếu còn một trong hai cột `salary_min_vnd` hoặc `salary_max_vnd` có số, hệ
thống coi đó là lương thật crawl được và không cho model ghi đè.

| Dữ liệu crawl | Cách hiểu | Có dự đoán không |
|---|---|---|
| `10 - 60 triệu` | Có range lương thật | Không |
| `Tới 40 triệu`, `Lên đến 40 triệu` | Có cận trên thật | Không |
| `Lớn hơn 40 triệu`, `Từ 40 triệu` | Có cận dưới thật | Không |
| `Thỏa thuận` nhưng JSON-LD có min/max | Có lương thật trong structured data | Không |
| `Thỏa thuận` nhưng chỉ có min | Có một cận lương thật | Không |
| `Thỏa thuận` và min/max đều null | Không có lương thật | Có |

`salary_is_negotiable` chỉ là cờ mô tả text nguồn. Nó không được phép ép model
dự đoán khi min/max đã có số.

## 3. Model Hiện Tại

Model production hiện tại là Spark ML `GBTRegressor`, version:

```text
spark_ml_salary_gbt_v1
```

Pipeline:

```text
title -> RegexTokenizer -> CountVectorizer
skills -> CountVectorizer
location/company -> StringIndexer -> OneHotEncoder
experience/remote -> numeric
VectorAssembler -> GBTRegressor
```

Model train bằng toàn bộ Silver:

```text
mỗi lần batch có dữ liệu mới
-> train lại bằng toàn bộ Silver cũ + mới
-> overwrite model cũ tại /models/salary_prediction/latest
```

Không chạy train theo `--date`, vì train một partition ngày dễ thiếu dữ liệu và
làm model lệch.

Path model:

```text
hdfs://hdfs-namenode.hdfs.svc:9000/models/salary_prediction/latest
```

Path metrics:

```text
hdfs://hdfs-namenode.hdfs.svc:9000/models/salary_prediction/metrics/latest
```

## 4. Output Salary Fields

| Field | Ý nghĩa |
|---|---|
| `salary_min_vnd` | Cận dưới lương thật parse được. |
| `salary_max_vnd` | Cận trên lương thật parse được. |
| `salary_predicted_min_vnd` | Cận dưới dự đoán, 90% predicted average. |
| `salary_predicted_avg_vnd` | Lương trung bình do model dự đoán. |
| `salary_predicted_max_vnd` | Cận trên dự đoán, 110% predicted average. |
| `salary_display_min_vnd` | Lương min cuối cùng cho UI/API. |
| `salary_display_avg_vnd` | Lương average cuối cùng cho sort/filter/chart. |
| `salary_display_max_vnd` | Lương max cuối cùng cho UI/API. |
| `salary_prediction_applied` | `true` nếu model đã được dùng. |
| `salary_source` | Nguồn tạo ra salary hiển thị. |
| `salary_prediction_model_version` | Version model nếu có prediction. |

Quy tắc dùng:

```text
Kibana/API nên dùng salary_display_*
salary_predicted_* chỉ dùng để xem riêng kết quả model
salary_source dùng để giải thích salary_display_* đến từ đâu
```

## 5. `salary_source`

| salary_source | Điều kiện |
|---|---|
| `parsed_range` | Có cả `salary_min_vnd` và `salary_max_vnd`. |
| `parsed_min_only` | Chỉ có `salary_min_vnd`. |
| `parsed_max_only` | Chỉ có `salary_max_vnd`. |
| `predicted` | Không có min/max thật và model đã dự đoán. |
| `unknown` | Không có min/max thật và chưa có prediction, thường do model chưa load được. |

Ví dụ đúng:

| salary | min | max | display avg | prediction | salary_source |
|---|---:|---:|---:|---|---|
| `10 - 60 triệu` | 10M | 60M | 35M | false | `parsed_range` |
| `Tới 40 triệu` | null | 40M | 40M | false | `parsed_max_only` |
| `Lớn hơn 40 triệu` | 40M | null | 40M | false | `parsed_min_only` |
| `Thỏa thuận` | null | null | 21M | true | `predicted` |
| `Thỏa thuận` | 30M | 40M | 35M | false | `parsed_range` |
| `Thỏa thuận` | 30M | null | 30M | false | `parsed_min_only` |

## 6. Chuẩn Bị Terminal

Đứng ở root repo:

```powershell
cd D:\2025_2\bigdata\bigdata-job-market
```

Kích hoạt virtual environment nếu cần:

```powershell
.\.venv\Scripts\Activate.ps1
```

Nếu `minikube.exe` nằm ở `C:\minikube`:

```powershell
$env:Path += ";C:\minikube"
```

Kiểm tra tool:

```powershell
kubectl version --client
minikube version
docker version
```

## 7. Start Minikube

Nếu chưa có cluster:

```powershell
minikube start -p job-market --driver=docker --cpus=8 --memory=10000 --disk-size=40g
kubectl config use-context job-market
kubectl get nodes
```

Nếu cluster đã tồn tại:

```powershell
minikube start -p job-market
kubectl config use-context job-market
```

Lưu ý: vì profile là `job-market`, các lệnh `minikube image build` phải có
`-p job-market`.

## 8. Deploy Hạ Tầng Batch

```powershell
kubectl apply -f infra\namespaces\all.yaml
kubectl apply -f infra\hdfs\
kubectl apply -f infra\search\
kubectl apply -f infra\spark\rbac.yaml
```

Chờ pod:

```powershell
kubectl get pods -n hdfs
kubectl get pods -n search
kubectl get pods -n spark
```

Kỳ vọng:

```text
hdfs-namenode-0   Running
hdfs-datanode-0   Running
elasticsearch-0   Running
kibana-*          Running
```

## 9. Build Spark Image

Mỗi khi sửa code trong `apps/`, `configs/`, `shared/`, hoặc Spark ML docs không
cần build, nhưng sửa Python code thì cần build lại image:

```powershell
minikube -p job-market image build -f infra\spark\Dockerfile -t spark-job-market:latest .
```

Image phải có:

```text
apps/ml/
apps/batch/jobs/
apps/stream_etl/
configs/
shared/
numpy
```

Nếu train model lỗi:

```text
ModuleNotFoundError: No module named 'numpy'
```

hãy rebuild image bằng đúng profile `job-market`.

## 10. Chuẩn Bị HDFS Cho Spark Ghi

Spark job chạy bằng user `spark`. Trong môi trường local/dev, tạo sẵn thư mục
và cấp quyền để tránh lỗi permission:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -mkdir -p /raw/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -mkdir -p /bronze/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -mkdir -p /silver/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -mkdir -p /gold/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -mkdir -p /models/salary_prediction

kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /raw
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /bronze
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /silver
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /gold
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /models
```

## 11. Apply Batch CronJobs

```powershell
kubectl apply -f infra\spark\batch-crawler-checkpoint-pvc.yaml

kubectl apply -f infra\spark\batch-crawler-cronjob.yaml
kubectl apply -f infra\spark\raw-to-bronze-cronjob.yaml
kubectl apply -f infra\spark\bronze-to-silver-cronjob.yaml
kubectl apply -f infra\spark\salary-model-train-cronjob.yaml
kubectl apply -f infra\spark\silver-to-gold-cronjob.yaml
kubectl apply -f infra\spark\gold-to-elasticsearch-cronjob.yaml
```

Kiểm tra:

```powershell
kubectl get cronjobs -n spark
```

Các CronJob có thể `SUSPEND=True`. Điều này bình thường vì ta trigger thủ công
hoặc để Airflow trigger.

## 12. Helper Chạy Batch Job Thủ Công

Tạo function trong PowerShell:

```powershell
function Run-BatchJob {
  param(
    [string] $CronJob,
    [string] $JobName,
    [string] $Timeout = "7200s"
  )

  kubectl delete job -n spark $JobName --ignore-not-found=true
  kubectl create job $JobName --from=cronjob/$CronJob -n spark

  kubectl wait --for=condition=complete job/$JobName -n spark --timeout=$Timeout

  kubectl logs -n spark -l job-name=$JobName --all-containers=true --tail=300
}
```

## 13. Cách A: Chạy Full Batch Có Crawl Thật

```powershell
Run-BatchJob "batch-etl-crawl-jobs" "manual-crawl-jobs" "108000s"
Run-BatchJob "batch-etl-raw-to-bronze" "manual-raw-to-bronze" "7200s"
Run-BatchJob "batch-etl-bronze-to-silver" "manual-bronze-to-silver" "7200s"
Run-BatchJob "batch-etl-train-salary-model" "manual-train-salary-model" "7200s"
Run-BatchJob "batch-etl-silver-to-gold" "manual-silver-to-gold" "7200s"
Run-BatchJob "batch-etl-gold-to-elasticsearch" "manual-gold-to-elasticsearch" "7200s"
```

## 14. Cách B: Dùng File Raw Có Sẵn

Nếu đã có file local như:

```text
data/raw/jobs/jobs_2026-05-08.jsonl
```

copy vào HDFS:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -mkdir -p /raw/jobs/ingest_date=2026-05-08

kubectl cp data/raw/jobs/jobs_2026-05-08.jsonl hdfs/hdfs-namenode-0:/tmp/jobs_2026-05-08.jsonl

kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -put -f /tmp/jobs_2026-05-08.jsonl /raw/jobs/ingest_date=2026-05-08/jobs_2026-05-08.jsonl

kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /raw/jobs
```

Sau đó không cần chạy crawler, chạy từ raw-to-bronze:

```powershell
Run-BatchJob "batch-etl-raw-to-bronze" "manual-raw-to-bronze" "7200s"
Run-BatchJob "batch-etl-bronze-to-silver" "manual-bronze-to-silver" "7200s"
Run-BatchJob "batch-etl-train-salary-model" "manual-train-salary-model" "7200s"
Run-BatchJob "batch-etl-silver-to-gold" "manual-silver-to-gold" "7200s"
Run-BatchJob "batch-etl-gold-to-elasticsearch" "manual-gold-to-elasticsearch" "7200s"
```

## 15. Khi Chỉ Sửa Logic ML/Gold

Nếu chỉ sửa:

- `apps/ml/salary_prediction.py`
- `apps/batch/jobs/silver_to_gold.py`
- logic `salary_source`
- rule scoring min/max

thì không cần crawl lại và thường không cần raw-to-bronze/bronze-to-silver lại.
Chạy:

```powershell
minikube -p job-market image build -f infra\spark\Dockerfile -t spark-job-market:latest .

Run-BatchJob "batch-etl-silver-to-gold" "manual-silver-to-gold" "7200s"
Run-BatchJob "batch-etl-gold-to-elasticsearch" "manual-gold-to-elasticsearch" "7200s"
```

Nếu vừa đổi model từ LR sang GBT hoặc muốn model mới:

```powershell
minikube -p job-market image build -f infra\spark\Dockerfile -t spark-job-market:latest .

Run-BatchJob "batch-etl-train-salary-model" "manual-train-salary-model" "7200s"
Run-BatchJob "batch-etl-silver-to-gold" "manual-silver-to-gold" "7200s"
Run-BatchJob "batch-etl-gold-to-elasticsearch" "manual-gold-to-elasticsearch" "7200s"
```

## 16. Kiểm Tra HDFS Output

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /raw/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /bronze/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /silver/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /models/salary_prediction
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /gold/jobs
```

Model cần có:

```text
/models/salary_prediction/latest
/models/salary_prediction/metrics/latest
```

## 17. Kiểm Tra Elasticsearch Batch

Mở port-forward Elasticsearch:

```powershell
kubectl port-forward svc/elasticsearch 9200:9200 -n search
```

Mở terminal khác:

```powershell
curl.exe "http://localhost:9200/gold-jobs-flat/_count?pretty"
curl.exe "http://localhost:9200/gold-jobs-flat/_search?pretty&size=3"
```

Kiểm tra các field:

```text
salary_min_vnd
salary_max_vnd
salary_display_min_vnd
salary_display_avg_vnd
salary_display_max_vnd
salary_predicted_min_vnd
salary_predicted_avg_vnd
salary_predicted_max_vnd
salary_prediction_applied
salary_source
salary_prediction_model_version
```

## 18. Xem Trên Kibana

Mở port-forward Kibana:

```powershell
kubectl port-forward -n search svc/kibana 5601:5601
```

Mở trình duyệt:

```text
http://localhost:5601
```

Tạo Data View:

```text
Stack Management
-> Data Views
-> Create data view
-> Name: gold-jobs-flat
-> Index pattern: gold-jobs-flat
```

Nếu hỏi time field, chọn `date_posted` nếu có. Nếu không thấy dữ liệu theo thời
gian, chỉnh time picker sang `Last 5 years` hoặc chọn khoảng có ngày crawl.

Nếu vừa thêm field mới như `salary_source`, refresh field list:

```text
Stack Management
-> Data Views
-> gold-jobs-flat
-> Refresh field list
```

Nên add các field này trong Discover:

```text
title
company_name
salary
salary_min_vnd
salary_max_vnd
salary_display_avg_vnd
salary_prediction_applied
salary_source
salary_prediction_model_version
```

KQL hữu ích:

```text
salary_source: "predicted"
salary_source: "parsed_range"
salary_source: "parsed_min_only"
salary_source: "parsed_max_only"
salary_source: "unknown"
salary: "Thỏa thuận" and salary_source: "parsed_range"
salary: "Thỏa thuận" and salary_source: "parsed_min_only"
salary: "Thỏa thuận" and salary_prediction_applied: true
salary_min_vnd >= 30000000 and salary_prediction_applied: false
```

Query kiểm tra lỗi cũ không nên còn:

```text
salary: "Thỏa thuận" and salary_min_vnd >= 0 and salary_prediction_applied: true
```

Nếu job có `salary_min_vnd` hoặc `salary_max_vnd`, query trên không nên trả ra
những job bị model ghi đè.

## 19. Chạy Batch Bằng Airflow

Deploy Airflow:

```powershell
kubectl apply -f infra\airflow\airflow-rbac.yaml
minikube -p job-market image build -f infra\airflow\Dockerfile -t job-market-airflow:2.9.3 .
kubectl apply -f infra\airflow\airflow-postgres.yaml
kubectl apply -f infra\airflow\airflow.yaml
```

Mở Airflow UI:

```powershell
kubectl port-forward svc/airflow-webserver 8082:8080 -n airflow
```

Truy cập:

```text
http://localhost:8082
```

Tài khoản:

```text
admin / admin
```

Trigger DAG:

```text
job_market_batch_pipeline
```

Thứ tự DAG:

```text
check_hdfs
check_elasticsearch
-> crawl_jobs
-> raw_to_bronze
-> bronze_to_silver
-> train_salary_model
-> silver_to_gold
-> gold_to_elasticsearch
```

## 20. Deploy Speed Layer Trên Kubernetes

Speed layer cần Kafka, Elasticsearch, Spark image và model đã train từ batch.

```powershell
kubectl apply -f infra\kafka\kafka-cluster.yaml
kubectl apply -f infra\kafka\jobs-topics.yaml
kubectl apply -f infra\spark\speed-checkpoint-pvc.yaml
```

Chờ Kafka:

```powershell
kubectl get pods -n kafka
kubectl get kafka -n kafka
```

Start speed job:

```powershell
kubectl delete job -n spark speed-stream-es-submit --ignore-not-found=true
kubectl apply -f infra\spark\speed-stream-es-job.yaml
kubectl logs -n spark -l app=speed-stream --all-containers=true --tail=300 -f
```

`speed-stream-es-job.yaml` bật:

```text
ENABLE_SALARY_PREDICTION=true
SALARY_MODEL_PATH=hdfs://hdfs-namenode.hdfs.svc:9000/models/salary_prediction/latest
```

Nếu train model mới, restart speed job để load model mới.

## 21. Đẩy Dữ Liệu Vào Speed Layer

Chạy crawler speed mode:

```powershell
$env:CRAWLER_MAX_PAGES = "1"
$env:CRAWLER_UPDATED_WITHIN_MINUTES = "1440"
.\scripts\run_topcv_crawler.ps1 -Mode speed
```

Port-forward Kafka nếu producer chạy từ máy host:

```powershell
kubectl port-forward -n kafka svc/my-cluster-kafka-bootstrap 9092:9092
```

Mở terminal khác:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
.\scripts\run_crawler_jsonl_producer.ps1
```

## 22. Kiểm Tra Speed Layer

```powershell
curl.exe "http://localhost:9200/realtime_jobs_v1/_count?pretty"
curl.exe "http://localhost:9200/realtime_jobs_v1/_search?pretty&size=3"
```

Trong document cần thấy:

```text
salary_display_avg_vnd
salary_prediction_applied
salary_source
salary_prediction_model_version
```

Kibana có thể tạo thêm Data View:

```text
realtime_jobs_v1
```

## 23. Chạy Speed Layer Bằng Docker Compose Local

Start hạ tầng speed local:

```powershell
docker compose -f infra\docker-compose\docker-compose.speed.yml up -d
.\scripts\create_kafka_topics.ps1
```

Nếu local Docker không đọc được HDFS model từ Minikube, tắt prediction:

```powershell
$env:ENABLE_SALARY_PREDICTION = "false"
.\scripts\run_stream_speed_layer.ps1 -RunMode docker
```

Nếu container Spark đọc được model path:

```powershell
$env:ENABLE_SALARY_PREDICTION = "true"
$env:SALARY_MODEL_PATH = "hdfs://hdfs-namenode.hdfs.svc:9000/models/salary_prediction/latest"
.\scripts\run_stream_speed_layer.ps1 -RunMode docker
```

## 24. Clean Và Chạy Lại

Xóa Kubernetes jobs thủ công:

```powershell
kubectl delete job -n spark manual-crawl-jobs --ignore-not-found=true
kubectl delete job -n spark manual-raw-to-bronze --ignore-not-found=true
kubectl delete job -n spark manual-bronze-to-silver --ignore-not-found=true
kubectl delete job -n spark manual-train-salary-model --ignore-not-found=true
kubectl delete job -n spark manual-silver-to-gold --ignore-not-found=true
kubectl delete job -n spark manual-gold-to-elasticsearch --ignore-not-found=true
kubectl delete job -n spark speed-stream-es-submit --ignore-not-found=true
```

Xóa speed checkpoint nếu Kafka topic bị reset:

```powershell
kubectl delete pvc -n spark spark-speed-checkpoints-pvc --ignore-not-found=true
kubectl apply -f infra\spark\speed-checkpoint-pvc.yaml
```

## 25. Troubleshooting

| Vấn đề | Cách kiểm tra/sửa |
|---|---|
| `cluster "minikube" does not exist` khi build image | Dùng `minikube -p job-market image build ...`. |
| Spark job không tạo driver pod | `kubectl get pods -n spark`, `kubectl describe job -n spark <job>`. |
| HDFS permission denied user `spark` | Chạy lại mục chuẩn bị HDFS và `chmod -R 777 /bronze /silver /gold /models`. |
| Train model lỗi thiếu `numpy` | Rebuild Spark image sau khi Dockerfile đã cài `numpy`. |
| Không thấy model | Chạy `batch-etl-train-salary-model`, kiểm tra `/models/salary_prediction`. |
| Gold không có `salary_source` | Rebuild image, chạy lại `silver_to_gold`, rồi `gold_to_elasticsearch`. |
| Kibana không thấy `salary_source` | Refresh field list trong Data View `gold-jobs-flat`. |
| Job `Thỏa thuận` có min/max vẫn bị predicted | Rebuild image, chạy lại `silver_to_gold` và `gold_to_elasticsearch`; kiểm tra rule min/max. |
| Speed không dùng model mới | Restart `speed-stream-es-submit`. |
| Elasticsearch không có index | Kiểm tra log `gold_to_elasticsearch` hoặc speed sink. |

## 26. File Liên Quan

| File | Mục đích |
|---|---|
| `apps/ml/salary_prediction.py` | Logic feature, label, GBTRegressor, prediction, `salary_source`. |
| `apps/batch/jobs/bronze_to_silver.py` | Parse salary thật từ JSON-LD/text, gồm range/min-only/max-only. |
| `apps/batch/jobs/train_salary_model.py` | Train model bằng toàn bộ Silver và overwrite `latest`. |
| `apps/batch/jobs/silver_to_gold.py` | Enrich Gold bằng prediction và select output columns. |
| `apps/batch/jobs/gold_to_elasticsearch.py` | Ghi Gold vào `gold-jobs-flat`. |
| `apps/stream_etl/normalizers.py` | Parse salary text cho speed layer. |
| `apps/stream_etl/transform.py` | Tạo clean stream schema có salary VND fields. |
| `apps/stream_etl/stream_main.py` | Load model và score realtime stream. |
| `infra/spark/salary-model-train-cronjob.yaml` | CronJob template train model. |
| `infra/spark/speed-stream-es-job.yaml` | Job chạy speed layer trên Kubernetes. |
| `docs/spark_ml_salary_prediction_readme.md` | Tài liệu giải thích nghiệp vụ và kiến trúc Spark ML. |
