# Run Batch ETL Pipeline tới Elasticsearch

Tài liệu này hướng dẫn setup môi trường local bằng Minikube, chạy pipeline Batch ETL từ Raw → Bronze → Silver → Gold, sau đó load Gold vào Elasticsearch và kiểm tra bằng Kibana.

---

## 1. Yêu cầu môi trường

Cần cài sẵn:

- Docker Desktop
- Minikube
- kubectl
- Git
- PowerShell
- Python nếu cần chạy script phụ trợ

Khuyến nghị Docker Desktop trên máy 16GB RAM:

```text
CPU: 8 cores
Memory: 10GB - 11GB
Disk: 40GB+
```

Nếu dùng WSL2 backend, có thể cấu hình file:

```powershell
notepad $env:USERPROFILE\.wslconfig
```

Nội dung khuyến nghị:

```text
[wsl2]
processors=8
memory=11GB
swap=4GB
```

Sau đó chạy:

```powershell
wsl --shutdown
```

Rồi mở lại Docker Desktop.

---

## 2. Start Minikube

Chạy tại root project (thay đổi path cho phù hợp):

```powershell
cd C:\path\to\bigdata-job-market
```

Start Minikube profile job-market:

```powershell
minikube start -p job-market --driver=docker --cpus=8 --memory=11000 --disk-size=40g
kubectl config use-context job-market
```

Kiểm tra:

```powershell
minikube status -p job-market
kubectl get nodes
kubectl config current-context
```

Kết quả mong muốn:

```text
current-context = job-market
node status = Ready
```

---

## 3. Build Spark image

Pipeline dùng image local:

```text
spark-job-market:latest
```

Build image vào đúng Minikube profile:

```powershell
minikube -p job-market image build -f infra/spark/Dockerfile -t spark-job-market:latest .
```

Kiểm tra image:

```powershell
minikube -p job-market image ls | Select-String "spark-job-market"
```

Nếu image không xuất hiện, có thể build bằng Docker rồi load vào Minikube:

```powershell
docker build -f infra/spark/Dockerfile -t spark-job-market:latest .
minikube -p job-market image load spark-job-market:latest
```

---

## 4. Deploy hạ tầng

Apply namespaces:

```powershell
kubectl apply -f infra\namespaces\all.yaml
```

Apply HDFS:

```powershell
kubectl apply -f infra\hdfs\hdfs.yaml
```

Apply Elasticsearch/Kibana:

```powershell
kubectl apply -f infra\search\
```

Apply Spark RBAC và CronJobs:

```powershell
kubectl apply -f infra\spark\rbac.yaml
kubectl apply -f infra\spark\raw-to-bronze-cronjob.yaml
kubectl apply -f infra\spark\bronze-to-silver-cronjob.yaml
kubectl apply -f infra\spark\silver-to-gold-cronjob.yaml
kubectl apply -f infra\spark\gold-to-elasticsearch-cronjob.yaml
```

Kiểm tra pod:

```powershell
kubectl get pods -n hdfs
kubectl get pods -n search
kubectl get cronjobs -n spark
```

Đợi HDFS và Elasticsearch chạy:

```text
hdfs-namenode-0      1/1 Running
hdfs-datanode-0      1/1 Running
elasticsearch-0      1/1 Running
```

---

## 5. Tạm dừng CronJob tự động

Để tránh các job tự chạy chồng lên nhau, suspend toàn bộ CronJob và chạy thủ công theo thứ tự:

```powershell
kubectl patch cronjob batch-etl-raw-to-bronze -n spark --type merge -p '{"spec":{"suspend":true}}'
kubectl patch cronjob batch-etl-bronze-to-silver -n spark --type merge -p '{"spec":{"suspend":true}}'
kubectl patch cronjob batch-etl-silver-to-gold -n spark --type merge -p '{"spec":{"suspend":true}}'
kubectl patch cronjob batch-etl-gold-to-elasticsearch -n spark --type merge -p '{"spec":{"suspend":true}}'
```

Kiểm tra:

```powershell
kubectl get cronjobs -n spark
```

Cột SUSPEND nên đều là:

```text
True
```

---

## 6. Chuẩn bị HDFS

Tạo thư mục HDFS:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -mkdir -p /raw/jobs /bronze/jobs /silver/jobs /gold/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /raw /bronze /silver /gold
```

Copy raw data vào HDFS:

```powershell
$INGEST_DATE="2026-05-08"
$RAW_FILE="jobs_2026-05-08.jsonl"
$NN="hdfs-namenode-0"

kubectl cp -n hdfs .\data\raw\jobs_2026-05-08.jsonl ${NN}:/tmp/$RAW_FILE

kubectl exec -n hdfs $NN -- hdfs dfs -mkdir -p /raw/jobs/ingest_date=$INGEST_DATE
kubectl exec -n hdfs $NN -- hdfs dfs -put -f /tmp/$RAW_FILE /raw/jobs/ingest_date=$INGEST_DATE/
```

Kiểm tra raw:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /raw/jobs
```

Kết quả mong muốn:

```text
/raw/jobs/ingest_date=2026-05-08/jobs_2026-05-08.jsonl
```

---

## 7. Dọn output cũ

Trước khi chạy lại pipeline, xóa Bronze/Silver/Gold cũ để tránh nhân bản dữ liệu:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -rm -r -f /bronze/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -rm -r -f /silver/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -rm -r -f /gold/jobs

kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -mkdir -p /bronze/jobs /silver/jobs /gold/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /bronze /silver /gold
```

---

## 8. Chạy Raw → Bronze

Tạo job thủ công từ CronJob:

```powershell
$RUN_ID = Get-Date -Format "yyyyMMddHHmmss"
kubectl create job raw-to-bronze-manual-$RUN_ID --from=cronjob/batch-etl-raw-to-bronze -n spark
```

Theo dõi:

```powershell
kubectl get pods -n spark -w
```

Khi pod driver và pod manual chuyển Completed, kiểm tra Bronze:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /bronze/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -du -h /bronze/jobs
```

Kết quả mong muốn:

```text
/bronze/jobs/_SUCCESS
/bronze/jobs/ingest_date=2026-05-08/part-....parquet
```

Nếu job lỗi, xem log driver:

```powershell
kubectl get pods -n spark | Select-String "raw-to-bronze.*driver"
kubectl logs -n spark <RAW_TO_BRONZE_DRIVER_POD> --tail=200
```

---

## 9. Chạy Bronze → Silver

```powershell
$RUN_ID = Get-Date -Format "yyyyMMddHHmmss"
kubectl create job bronze-to-silver-manual-$RUN_ID --from=cronjob/batch-etl-bronze-to-silver -n spark
```

Theo dõi:

```powershell
kubectl get pods -n spark -w
```

Kiểm tra Silver:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /silver/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -du -h /silver/jobs
```

Kết quả mong muốn:

```text
/silver/jobs/_SUCCESS
/silver/jobs/ingest_date=2026-05-08/part-....parquet
```

---

## 10. Chạy Silver → Gold

```powershell
$RUN_ID = Get-Date -Format "yyyyMMddHHmmss"
kubectl create job silver-to-gold-manual-$RUN_ID --from=cronjob/batch-etl-silver-to-gold -n spark
```

Theo dõi:

```powershell
kubectl get pods -n spark -w
```

Kiểm tra Gold:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /gold/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -du -h /gold/jobs
```

Kết quả mong muốn:

```text
/gold/jobs/job_market_index/_SUCCESS
/gold/jobs/job_market_index/ingest_date=2026-05-08/part-....parquet
```

---

## 11. Chạy Gold → Elasticsearch

Trước tiên kiểm tra Elasticsearch:

```powershell
kubectl exec -n search elasticsearch-0 -- curl -s http://localhost:9200/_cluster/health?pretty
```

Kết quả mong muốn:

```text
"status" : "green"
```

hoặc:

```text
"status" : "yellow"
```

Chạy job load Gold vào Elasticsearch:

```powershell
$RUN_ID = Get-Date -Format "yyyyMMddHHmmss"
kubectl create job gold-to-elasticsearch-manual-$RUN_ID --from=cronjob/batch-etl-gold-to-elasticsearch -n spark
```

Theo dõi:

```powershell
kubectl get pods -n spark -w
```

Khi driver Completed, xem log:

```powershell
kubectl get pods -n spark | Select-String "gold-to-elasticsearch.*driver"
kubectl logs -n spark <GOLD_TO_ES_DRIVER_POD> --tail=120
```

---

## 12. Kiểm tra Elasticsearch

Mở port-forward Elasticsearch:

```powershell
kubectl port-forward -n search svc/elasticsearch 9200:9200
```

Giữ cửa sổ này mở.

Ở PowerShell khác, kiểm tra count:

```powershell
Invoke-RestMethod http://localhost:9200/gold-jobs-flat/_count
```

Kết quả mong muốn:

```text
count = 3598
```

Kiểm tra index:

```powershell
Invoke-RestMethod "http://localhost:9200/_cat/indices/gold-jobs-flat?v"
```

Search thử vài document:

```powershell
Invoke-RestMethod "http://localhost:9200/gold-jobs-flat/_search?size=3"
```

---

## 13. Kiểm tra Kibana

Mở port-forward Kibana:

```powershell
kubectl port-forward -n search svc/kibana 5601:5601
```

Mở trình duyệt vào:

```text
http://localhost:5601
```

Tạo Index Pattern:

- Index pattern name: `gold-jobs-flat`
- Timestamp field: `ingest_date`

Sau đó vào Discover để xem dữ liệu.

---

## 14. Bật CronJob tự động (Optional)

Nếu muốn các job chạy theo schedule định sẵn:

```powershell
kubectl patch cronjob batch-etl-raw-to-bronze -n spark --type merge -p '{"spec":{"suspend":false}}'
kubectl patch cronjob batch-etl-bronze-to-silver -n spark --type merge -p '{"spec":{"suspend":false}}'
kubectl patch cronjob batch-etl-silver-to-gold -n spark --type merge -p '{"spec":{"suspend":false}}'
kubectl patch cronjob batch-etl-gold-to-elasticsearch -n spark --type merge -p '{"spec":{"suspend":false}}'
```

Schedule:

```text
19:00 UTC → Raw → Bronze
19:30 UTC → Bronze → Silver
20:00 UTC → Silver → Gold
20:30 UTC → Gold → Elasticsearch
```

---

## 15. Cleanup

Xóa deployment:

```powershell
kubectl delete ns hdfs spark search serving kafka
```

Stop Minikube:

```powershell
minikube stop -p job-market
```

Delete Minikube profile:

```powershell
minikube delete -p job-market
```
