# Run HDFS + Spark Batch ETL Pipeline tới Silver

Tài liệu này hướng dẫn setup Minikube và chạy pipeline Batch ETL từ raw data JSONL tới Silver trên HDFS trong môi trường Kubernetes local.

Pipeline hiện tại:

```text
Raw JSONL
→ HDFS /raw/jobs/ingest_date=YYYY-MM-DD
→ Spark raw_to_bronze
→ HDFS /bronze/jobs/ingest_date=YYYY-MM-DD
→ Spark bronze_to_silver
→ HDFS /silver/jobs/ingest_date=YYYY-MM-DD
```

---

## 0. Minikube resource setup

Với máy local 16GB RAM, cấu hình khuyến nghị:

```powershell
minikube start -p job-market --driver=docker --cpus=4 --memory=9500 --disk-size=30g
kubectl config use-context job-market
```

Cấu hình này đủ để demo local ở mức nhẹ:

```text
HDFS
Spark driver + 1 executor
MongoDB
Elasticsearch
Kibana
FastAPI
```

Nếu chỉ chạy pipeline tới Silver thì bắt buộc cần `hdfs` và `spark`. Các namespace `database`, `search`, `serving` chỉ cần khi chạy Serving/Search layer.

Kiểm tra Minikube:

```powershell
minikube status -p job-market
kubectl get nodes
```

Kỳ vọng node ở trạng thái:

```text
Ready
```

> Lưu ý: Không nên cấp quá nhiều RAM cho Minikube trên máy 16GB. Mức `9500MB` là cân bằng để Windows/Docker Desktop/IDE vẫn còn RAM.

---

## 1. Namespace architecture

## 1. Namespace architecture

Project dùng namespace theo layer:

| Namespace | Vai trò |
|---|---|
| `hdfs` | Lưu dữ liệu Raw/Bronze/Silver/Gold bằng HDFS |
| `spark` | Chạy Spark CronJob, driver, executor |
| `database` | MongoDB |
| `search` | Elasticsearch + Kibana |
| `serving` | FastAPI search API |
| `kafka` | Streaming/speed layer nếu dùng sau |

Để chạy pipeline tới Silver, bắt buộc cần:

```text
hdfs
spark
```

Các namespace `database`, `search`, `serving` dùng cho Serving/Search layer sau khi có Gold data.

---

## 2. Kiểm tra trạng thái cluster

## 2. Kiểm tra trạng thái cluster

```powershell
kubectl get pods -n hdfs
kubectl get pods -n spark
kubectl get pods -n database
kubectl get pods -n search
kubectl get pods -n serving
```

Kỳ vọng HDFS:

```text
hdfs-namenode-0   1/1   Running
hdfs-datanode-0   1/1   Running
```

Với namespace `spark`, bình thường có thể không có pod nào đang chạy. Spark pod chỉ xuất hiện khi chạy job.

---

## 3. Deploy namespace và HDFS

## 3. Deploy namespace và HDFS

Apply namespaces:

```powershell
kubectl apply -f infra\namespaces\all.yaml
```

Deploy HDFS:

```powershell
kubectl apply -f infra\hdfs\hdfs.yaml
kubectl get pods -n hdfs -w
```

Đợi đến khi:

```text
hdfs-namenode-0   1/1   Running
hdfs-datanode-0   1/1   Running
```

Kiểm tra service HDFS:

```powershell
kubectl get svc -n hdfs
```

Kỳ vọng có service NameNode:

```text
hdfs-namenode   9000/TCP,9870/TCP
```

Tạo thư mục dữ liệu trên HDFS:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -mkdir -p /raw/jobs /bronze/jobs /silver/jobs /gold
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /raw /bronze /silver /gold
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls /
```

Kỳ vọng:

```text
/bronze
/gold
/raw
/silver
```

---

## 4. Copy raw data vào HDFS

## 4. Copy raw data vào HDFS

Ví dụ raw file local:

```text
data/raw/jobs_2026-05-08.jsonl
```

Set biến PowerShell:

```powershell
$INGEST_DATE="2026-05-08"
$RAW_FILE="jobs_2026-05-08.jsonl"
$NN="hdfs-namenode-0"
```

Copy file local vào pod NameNode:

```powershell
kubectl cp -n hdfs .\data\raw\jobs_2026-05-08.jsonl ${NN}:/tmp/$RAW_FILE
```

Đưa file vào HDFS theo partition ngày:

```powershell
kubectl exec -n hdfs $NN -- hdfs dfs -mkdir -p /raw/jobs/ingest_date=$INGEST_DATE
kubectl exec -n hdfs $NN -- hdfs dfs -put -f /tmp/$RAW_FILE /raw/jobs/ingest_date=$INGEST_DATE/
```

Kiểm tra:

```powershell
kubectl exec -n hdfs $NN -- hdfs dfs -ls -R /raw/jobs
```

Kỳ vọng:

```text
/raw/jobs/ingest_date=2026-05-08/jobs_2026-05-08.jsonl
```

> Không commit raw data lớn vào Git. File raw nên nằm ngoài Git hoặc lấy từ Drive/HDFS.

---

## 5. Build Spark image

## 5. Build Spark image

Mỗi khi sửa code trong các thư mục sau thì cần build lại image:

```text
apps/batch/jobs/
configs/
shared/
infra/spark/Dockerfile
```

Build image:

```powershell
minikube image build -f infra\spark\Dockerfile -t spark-job-market:latest .
```

Kiểm tra image:

```powershell
minikube image ls | Select-String "spark-job-market"
```

Kỳ vọng:

```text
docker.io/library/spark-job-market:latest
```

---

## 6. Apply Spark RBAC và CronJob

## 6. Apply Spark RBAC và CronJob

Apply RBAC:

```powershell
kubectl apply -f infra\spark\rbac.yaml
```

Kiểm tra:

```powershell
kubectl get sa -n spark
kubectl get role -n spark
kubectl get rolebinding -n spark
```

Kỳ vọng có:

```text
spark
spark-role
spark-rolebinding
```

Apply CronJob:

```powershell
kubectl apply -f infra\spark\raw-to-bronze-cronjob.yaml
kubectl apply -f infra\spark\bronze-to-silver-cronjob.yaml
```

Kiểm tra:

```powershell
kubectl get cronjobs -n spark
```

Kỳ vọng:

```text
batch-etl-raw-to-bronze
batch-etl-bronze-to-silver
```

---

## 7. Chạy Raw → Bronze

## 7. Chạy Raw → Bronze

Trước khi chạy lại cùng một ngày, nên xóa output Bronze cũ để tránh ghi lặp file parquet:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -rm -r -f /bronze/jobs/ingest_date=2026-05-08
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -rm -f /bronze/jobs/_SUCCESS
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /bronze
```

Xóa job/pod cũ nếu có:

```powershell
kubectl delete job raw-to-bronze-20260508 -n spark --ignore-not-found
kubectl delete pod -n spark -l spark-app-name=raw-to-bronze --ignore-not-found
```

Tạo job thủ công từ CronJob:

```powershell
kubectl create job raw-to-bronze-20260508 --from=cronjob/batch-etl-raw-to-bronze -n spark
```

Theo dõi pod:

```powershell
kubectl get pods -n spark -w
```

Lấy tên driver pod:

```powershell
kubectl get pods -n spark | Select-String "raw-to-bronze.*driver"
```

Xem log driver:

```powershell
kubectl logs -n spark <RAW_TO_BRONZE_DRIVER_POD> -f
```

Khi job xong, kiểm tra:

```powershell
kubectl get pods -n spark | Select-String "raw-to-bronze"
```

Kỳ vọng:

```text
raw-to-bronze-...            Completed
raw-to-bronze-...-driver     Completed
```

Kiểm tra Bronze output:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /bronze/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -du -h /bronze/jobs
```

Kỳ vọng:

```text
/bronze/jobs/_SUCCESS
/bronze/jobs/ingest_date=2026-05-08/part-00000-....snappy.parquet
/bronze/jobs/ingest_date=2026-05-08/part-00001-....snappy.parquet
```

---

## 8. Chạy Bronze → Silver

## 8. Chạy Bronze → Silver

Trước khi chạy lại cùng một ngày, nên xóa output Silver cũ:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -rm -r -f /silver/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -mkdir -p /silver/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /silver
```

Xóa job/pod cũ nếu có:

```powershell
kubectl delete job bronze-to-silver-20260509 -n spark --ignore-not-found
kubectl delete pod -n spark -l spark-app-name=bronze-to-silver --ignore-not-found
```

Tạo job thủ công từ CronJob:

```powershell
kubectl create job bronze-to-silver-20260509 --from=cronjob/batch-etl-bronze-to-silver -n spark
```

Theo dõi pod:

```powershell
kubectl get pods -n spark -w
```

Lấy tên driver pod:

```powershell
kubectl get pods -n spark | Select-String "bronze-to-silver.*driver"
```

Xem log driver:

```powershell
kubectl logs -n spark <BRONZE_TO_SILVER_DRIVER_POD> -f
```

Khi job xong, kiểm tra:

```powershell
kubectl get pods -n spark | Select-String "bronze-to-silver"
```

Kỳ vọng:

```text
bronze-to-silver-...            Completed
bronze-to-silver-...-driver     Completed
```

Kiểm tra Silver output:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /silver/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -du -h /silver/jobs
```

Kỳ vọng:

```text
/silver/jobs/_SUCCESS
/silver/jobs/ingest_date=2026-05-08/part-00000-....snappy.parquet
/silver/jobs/ingest_date=2026-05-08/part-00001-....snappy.parquet
```

Ví dụ kết quả thành công:

```text
/silver/jobs/_SUCCESS
/silver/jobs/ingest_date=2026-05-08
/silver/jobs/ingest_date=2026-05-08/part-00000-99cb806a-....snappy.parquet
/silver/jobs/ingest_date=2026-05-08/part-00001-99cb806a-....snappy.parquet

108.0 M  324.1 M  /silver/jobs/ingest_date=2026-05-08
```

Trong đó:

```text
108.0 M  = dung lượng logic
324.1 M  = dung lượng tính cả replication factor 3 của HDFS
```

---

## 9. Kiểm tra trạng thái cuối

Kiểm tra HDFS:

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /raw/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /bronze/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /silver/jobs
```

Kiểm tra pods:

```powershell
kubectl get pods -n hdfs
kubectl get pods -n spark
```

Kỳ vọng:

```text
hdfs:
  hdfs-namenode-0   Running
  hdfs-datanode-0   Running

spark:
  raw-to-bronze...       Completed
  bronze-to-silver...    Completed
```

---

## 10. Resume sau khi restart máy

Start lại Minikube:

```powershell
minikube start -p job-market
kubectl config use-context job-market
```

Kiểm tra pod:

```powershell
kubectl get pods -A
```

Kiểm tra Spark image:

```powershell
minikube image ls | Select-String "spark-job-market"
```

Nếu image không còn, build lại:

```powershell
minikube image build -f infra\spark\Dockerfile -t spark-job-market:latest .
```

CronJob và manifest thường vẫn còn nếu cluster chưa bị xóa:

```powershell
kubectl get cronjobs -n spark
```

Nếu thiếu, apply lại:

```powershell
kubectl apply -f infra\spark\rbac.yaml
kubectl apply -f infra\spark\raw-to-bronze-cronjob.yaml
kubectl apply -f infra\spark\bronze-to-silver-cronjob.yaml
```

Nếu cluster bị xóa và tạo lại, cần copy raw data vào HDFS lại từ đầu.

---

## 11. Resource notes cho máy 16GB RAM

Khuyến nghị:

```powershell
minikube start -p job-market --driver=docker --cpus=4 --memory=9500 --disk-size=30g
```

Spark job được cấu hình nhẹ cho Minikube:

```yaml
spark.executor.instances=1
spark.executor.memory=768m
spark.driver.memory=768m
spark.sql.shuffle.partitions=8
spark.kubernetes.executor.request.cores=250m
spark.kubernetes.executor.limit.cores=500m
spark.kubernetes.driver.request.cores=250m
spark.kubernetes.driver.limit.cores=500m
```

Nếu thiếu RAM, chỉ chạy pipeline namespaces:

```text
hdfs
spark
```

Có thể tạm scale down Serving/Search/Database:

```powershell
kubectl scale statefulset mongodb -n database --replicas=0
kubectl scale statefulset elasticsearch -n search --replicas=0
kubectl scale deployment kibana -n search --replicas=0
kubectl scale deployment job-search-api -n serving --replicas=0
```

Bật lại sau:

```powershell
kubectl scale statefulset mongodb -n database --replicas=1
kubectl scale statefulset elasticsearch -n search --replicas=1
kubectl scale deployment kibana -n search --replicas=1
kubectl scale deployment job-search-api -n serving --replicas=1
```

---

## 12. Monitoring and logs

List Spark pods:

```powershell
kubectl get pods -n spark
```

Watch Spark pods:

```powershell
kubectl get pods -n spark -w
```

List jobs:

```powershell
kubectl get jobs -n spark
```

Stream driver logs:

```powershell
kubectl logs -n spark <DRIVER_POD> -f
```

Inspect pod details:

```powershell
kubectl describe pod <POD_NAME> -n spark
```

Check CronJob status:

```powershell
kubectl get cronjobs -n spark
kubectl describe cronjob batch-etl-raw-to-bronze -n spark
kubectl describe cronjob batch-etl-bronze-to-silver -n spark
```

Open Minikube dashboard:

```powershell
minikube dashboard -p job-market
```

---

## 13. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Pod stuck in `ImagePullBackOff` | Image not found in Minikube | Run `minikube image build -f infra\spark\Dockerfile -t spark-job-market:latest .` |
| Executor pod stuck `Pending` | Not enough CPU/RAM | Reduce Spark resources or scale down database/search/serving |
| `Insufficient cpu` | Executor request too high | Use `spark.kubernetes.executor.request.cores=250m` |
| HDFS connection refused | NameNode not running | Check `kubectl get pods -n hdfs` |
| HDFS permission denied | Spark user cannot write to HDFS path | Run `hdfs dfs -chmod -R 777 /raw /bronze /silver /gold` |
| Job never starts | RBAC/CronJob not applied | Reapply `infra\spark\rbac.yaml` and CronJob YAML |
| `ModuleNotFoundError: configs` | Spark image missing `configs/` | Ensure Dockerfile copies `configs/` and rebuild image |
| `ModuleNotFoundError: pydantic_settings` | Missing Python dependency | Add `pydantic-settings` in Dockerfile and rebuild image |
| `CAST_INVALID_INPUT` for salary | Empty string cast to double | Use `try_cast(nullif(..., '') as double)` |
| Java heap space reading Parquet | Vectorized reader too memory-heavy | Disable Parquet vectorized reader in CronJob |

Useful commands:

```powershell
kubectl describe pod <POD_NAME> -n spark
kubectl logs -n spark <DRIVER_POD> --tail=150
kubectl get pods -A
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /silver/jobs
```

---

## 14. Next steps

Pipeline hiện tại chỉ chạy tới Silver.

Để dữ liệu mới xuất hiện trên Kibana/FastAPI, cần chạy tiếp:

```text
Silver → Gold
Gold → MongoDB
Gold → Elasticsearch
```

Kibana và FastAPI không đọc trực tiếp HDFS. Chúng đọc dữ liệu từ Elasticsearch và MongoDB.
