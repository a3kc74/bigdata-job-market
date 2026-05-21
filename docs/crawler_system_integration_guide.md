# Hướng dẫn sử dụng crawler để tích hợp vào hệ thống

Tài liệu này dành cho các thành viên khác trong nhóm khi cần dùng code crawler của TV2 để tích hợp vào các phần: Kafka speed layer, HDFS batch layer, Spark ETL, logging, cron/server deployment và Kubernetes.

---

## 1. Crawler của TV2 cung cấp gì?

Crawler hiện tại không chỉ là script crawl đơn lẻ. Nó cung cấp 3 phần chính:

```text
apps/ingestion/
├── topcv_crawler.py      # core crawler: crawl TopCV, parse, ghi JSONL
└── run_crawler.py        # runner: chọn mode speed/batch/resume

apps/producer/
├── __init__.py
└── file_to_kafka.py      # đọc JSONL speed và gửi vào Kafka

scripts/
├── run_speed_pipeline.sh       # chạy speed crawler rồi gửi Kafka
├── run_batch_pipeline.sh       # chạy batch crawler rồi upload HDFS
├── resume_batch_if_needed.sh   # resume batch nếu batch bị dừng giữa chừng
└── upload_to_hdfs.sh           # upload JSONL batch vào HDFS
```

Crawler tạo dữ liệu raw dạng JSONL:

```text
data/raw/jobs/source=topcv/ingest_date=YYYY-MM-DD/jobs_speed_YYYYMMDD_HHMMSS.jsonl
data/raw/jobs/source=topcv/ingest_date=YYYY-MM-DD/jobs_batch_YYYYMMDD_HHMMSS.jsonl
```

Runtime state và log nằm ở:

```text
runtime/
├── crawler/
│   ├── cookie_cache.json
│   ├── speed_processed_jobs_29d.json
│   ├── batch_checkpoint.json
│   ├── failed_links.json
│   └── missing_jobs.log
└── logs/
    ├── speed_pipeline_YYYY-MM-DD.log
    ├── batch_pipeline_YYYY-MM-DD.log
    └── batch_resume_YYYY-MM-DD.log
```

`runtime/` và `data/raw/jobs/` là dữ liệu sinh ra khi chạy, không commit lên GitHub.

---

## 2. Cách chạy nhanh cho người mới

Từ root repo:

```bash
uv sync
```

Test speed 1 page:

```bash
uv run python -m apps.ingestion.run_crawler --mode speed --max-pages 1 --debug-card-links
```

Test batch 2 page:

```bash
uv run python -m apps.ingestion.run_crawler --mode batch --max-pages 2
```

Nếu muốn kiểm tra file output:

```bash
find data/raw/jobs -type f
```

---

## 3. Luồng tích hợp tổng thể

```text
                ┌──────────────────────┐
                │      TopCV site       │
                └───────────┬──────────┘
                            │
                    apps/ingestion
                            │
        ┌───────────────────┴───────────────────┐
        │                                       │
  Speed crawler                            Batch crawler
        │                                       │
 jobs_speed_*.jsonl                    jobs_batch_*.jsonl
        │                                       │
 apps/producer/file_to_kafka.py         scripts/upload_to_hdfs.sh
        │                                       │
 Kafka topic: jobs_raw                  HDFS raw zone
        │                                       │
 Spark Structured Streaming             Spark Batch ETL
        │                                       │
 Realtime aggregates                     Bronze/Silver/Gold
```

---

## 4. Raw JSONL schema

Mỗi dòng JSONL là một record job.

Các field cấp ngoài:

```json
{
  "source": "topcv",
  "source_url": "...",
  "normalized_source_url": "...",
  "crawl_version": 1,
  "ingest_ts": 1777812000123,
  "event_ts": 1777811000123,
  "job_id": "...",
  "hash_content": "...",
  "payload": {},
  "quality_flags": {}
}
```

Ý nghĩa field chính:

| Field | Ý nghĩa |
|---|---|
| `source` | Nguồn dữ liệu, hiện là `topcv` |
| `source_url` | URL gốc của job |
| `normalized_source_url` | URL đã chuẩn hóa để hash |
| `crawl_version` | Version của crawler/schema raw |
| `ingest_ts` | Thời điểm crawler ghi nhận record |
| `event_ts` | Thời điểm job được cập nhật trên listing page nếu parse được |
| `job_id` | ID ổn định của job |
| `hash_content` | Hash nội dung chính để phát hiện job update |
| `payload` | Nội dung job |
| `quality_flags` | Cờ kiểm tra dữ liệu thiếu/đủ |

Các field thường dùng trong `payload`:

```text
title
company_name
company_details
salary
location
monthOfExperience
deadline
occupationalCategory
education
employmentType
openings
description
requirements
income
benefits
extra_inf
schedule
skillsNeeded
skillsShouldHave
specialty
meta_tags
json_ld
sectionsByHeading
pageText
listing_updated_text
listing_updated_at
```

Các field quality quan trọng:

```text
has_salary_info
has_location_info
has_experience_info
has_requirements
has_description
has_benefits
has_skills_info
has_listing_updated_time
```

---

## 5. Hướng dẫn cho TV4 — dùng crawler cho Speed/Kafka

### 5.1. Dữ liệu TV4 nhận được

Speed pipeline sẽ gửi raw job record vào Kafka topic:

```text
jobs_raw
```

Producer dùng:

```text
key   = job_id
value = full raw JSON string
```

TV4 có thể parse Kafka value theo raw schema ở trên.

### 5.2. Chạy speed crawler thủ công

```bash
uv run python -m apps.ingestion.run_crawler --mode speed
```

Mặc định:

```text
max_pages = 15
updated_within_minutes = 30
speed_processed_cache_ttl_days = 29
```

Output local:

```text
data/raw/jobs/source=topcv/ingest_date=YYYY-MM-DD/jobs_speed_*.jsonl
```

### 5.3. Gửi file speed vào Kafka thủ công

```bash
uv run python -m apps.producer.file_to_kafka   --input "data/raw/jobs/source=topcv/ingest_date=$(date +%F)/jobs_speed_*.jsonl"   --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"   --topic "${KAFKA_TOPIC_JOBS_RAW:-jobs_raw}"
```

### 5.4. Chạy full speed pipeline

```bash
scripts/run_speed_pipeline.sh
```

Script này tự làm:

```text
1. Chạy crawler speed.
2. Tìm file jobs_speed_*.jsonl mới nhất hôm nay.
3. Gửi file đó vào Kafka topic jobs_raw.
4. Ghi log vào runtime/logs/.
```

### 5.5. Lưu ý cho TV4

Nếu stream schema của TV4 đang yêu cầu `stream_ingest_ts`, có thể map:

```text
stream_ingest_ts = ingest_ts
```

Không cần bắt TV2 đổi crawler core chỉ vì tên field này. Producer hoặc Spark transform có thể thêm alias này.

---

## 6. Hướng dẫn cho TV3 — dùng crawler cho Batch/HDFS

### 6.1. Dữ liệu TV3 nhận được

Batch pipeline tạo file:

```text
jobs_batch_*.jsonl
```

Sau đó upload vào HDFS raw zone:

```text
/raw/jobs/source=topcv/ingest_date=YYYY-MM-DD/
```

TV3 đọc raw zone này để chạy:

```text
raw → bronze → silver → gold
```

### 6.2. Chạy batch crawler thủ công

```bash
uv run python -m apps.ingestion.run_crawler --mode batch
```

Mặc định:

```text
days = 7
max_pages = 0
use_processed_cache = false
```

`max_pages = 0` nghĩa là crawler tự lấy tổng số page từ TopCV.

### 6.3. Test batch ít page

```bash
uv run python -m apps.ingestion.run_crawler --mode batch --max-pages 2
```

### 6.4. Upload file batch vào HDFS

Lấy file mới nhất:

```bash
LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=$(date +%F)/jobs_batch_*.jsonl | head -n 1)"
```

Upload:

```bash
scripts/upload_to_hdfs.sh "$LATEST_FILE" "$(date +%F)"
```

Script upload nên thực hiện một trong hai cách:

Nếu server có lệnh `hdfs` trực tiếp:

```bash
hdfs dfs -mkdir -p /raw/jobs/source=topcv/ingest_date=${INGEST_DATE}
hdfs dfs -put -f "$LOCAL_FILE" /raw/jobs/source=topcv/ingest_date=${INGEST_DATE}/
```

Nếu HDFS chạy trong Kubernetes/Minikube:

```bash
kubectl cp "$LOCAL_FILE" hdfs/hdfs-namenode-0:/tmp/$(basename "$LOCAL_FILE")

kubectl exec hdfs-namenode-0 -n hdfs --   hdfs dfs -mkdir -p /raw/jobs/source=topcv/ingest_date=${INGEST_DATE}

kubectl exec hdfs-namenode-0 -n hdfs --   hdfs dfs -put -f /tmp/$(basename "$LOCAL_FILE")   /raw/jobs/source=topcv/ingest_date=${INGEST_DATE}/
```

### 6.5. Chạy full batch pipeline

```bash
scripts/run_batch_pipeline.sh
```

Script này tự làm:

```text
1. Chạy crawler batch.
2. Tìm file jobs_batch_*.jsonl mới nhất hôm nay.
3. Upload vào HDFS raw zone.
4. Ghi log vào runtime/logs/.
```

---

## 7. Batch resume dùng khi nào?

Dùng khi batch bị chết giữa chừng do:

```text
server bị kill
process bị dừng
mất mạng
bị block nặng
```

Không dùng resume để retry từng link failed.

Chạy thủ công:

```bash
uv run python -m apps.ingestion.run_crawler --mode batch --resume
```

Hoặc dùng script:

```bash
scripts/resume_batch_if_needed.sh
```

Script này kiểm tra:

```text
runtime/crawler/batch_checkpoint.json
```

Nếu không có checkpoint hoặc checkpoint `completed=true`, script thoát. Nếu `completed=false`, script chạy resume.

Batch checkpoint hết hạn sau 2 ngày để tránh tuần sau resume nhầm từ page cũ.

---

## 8. Tự động chạy trên server bằng cron

Tạo folder log:

```bash
mkdir -p runtime/logs
```

Mở cron:

```bash
crontab -e
```

Thêm:

```cron
*/15 * * * * cd /path/to/bigdata-job-market && scripts/run_speed_pipeline.sh >> runtime/logs/speed_cron.log 2>&1

0 2 * * 0 cd /path/to/bigdata-job-market && scripts/run_batch_pipeline.sh >> runtime/logs/batch_cron.log 2>&1

0 4-23/2 * * 0 cd /path/to/bigdata-job-market && scripts/resume_batch_if_needed.sh >> runtime/logs/batch_resume_cron.log 2>&1
```

Ý nghĩa:

```text
Speed: chạy mỗi 15 phút.
Batch: chạy 02:00 Chủ nhật.
Resume: kiểm tra mỗi 2 tiếng sau batch chính trong Chủ nhật.
```

### Vì sao resume không chạy đè batch?

`run_batch_pipeline.sh` và `resume_batch_if_needed.sh` phải dùng chung lock file:

```text
runtime/crawler/batch_pipeline.lock
```

Nếu batch chính vẫn đang chạy, resume thấy lock và tự thoát.

---

## 9. Hướng dẫn cho TV5 — logging và monitoring

### 9.1. Runtime logs

Các script nên ghi log vào:

```text
runtime/logs/
├── speed_pipeline_YYYY-MM-DD.log
├── batch_pipeline_YYYY-MM-DD.log
├── batch_resume_YYYY-MM-DD.log
├── kafka_producer.log
└── hdfs_upload.log
```

### 9.2. Crawler state/error files

```text
runtime/crawler/
├── cookie_cache.json
├── speed_processed_jobs_29d.json
├── batch_checkpoint.json
├── failed_links.json
├── missing_jobs.log
└── debug_card_links/
```

Ý nghĩa:

| File | Ý nghĩa |
|---|---|
| `cookie_cache.json` | Cookie TopCV để giảm số lần mở browser |
| `speed_processed_jobs_29d.json` | Cache chống speed gửi trùng |
| `batch_checkpoint.json` | Resume batch |
| `failed_links.json` | Link bị HTTP fail/parse fail |
| `missing_jobs.log` | Job thiếu field quan trọng |
| `debug_card_links/` | Debug listing card URL |

Nếu muốn audit lâu dài, có thể upload error logs vào:

```text
/raw/errors/source=topcv/ingest_date=YYYY-MM-DD/
```

Không upload chung vào raw jobs.

---

## 10. Triển khai trên Kubernetes

Có 2 cách.

---

## 10.1. Cách khuyến nghị ban đầu

Crawler chạy bằng cron trên server.

Kafka/HDFS/Spark chạy trong Kubernetes/Minikube.

Cách này dễ hơn vì crawler cần Chrome/nodriver. Server chỉ cần cài Chrome/Chromium, còn các thành phần Big Data vẫn chạy trong cluster.

---

## 10.2. Chạy crawler trong Kubernetes CronJob

Nếu nhóm muốn chạy crawler trong K8s, cần Docker image có:

```text
Python
uv hoặc pip dependencies
Chrome/Chromium
project code
scripts/
apps/
```

Ví dụ build image trong Minikube:

```bash
eval $(minikube docker-env)
docker build -t bigdata-job-market/crawler:latest -f infra/crawler/Dockerfile .
```

PowerShell:

```powershell
& minikube -p minikube docker-env --shell powershell | Invoke-Expression
docker build -t bigdata-job-market/crawler:latest -f infra/crawler/Dockerfile .
```

Tạo namespace:

```bash
kubectl create namespace ingestion
```

---

## 11. Kubernetes CronJob mẫu cho speed

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: topcv-speed-pipeline
  namespace: ingestion
spec:
  schedule: "*/15 * * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 2
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      backoffLimit: 1
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: topcv-speed
              image: bigdata-job-market/crawler:latest
              imagePullPolicy: IfNotPresent
              command: ["/bin/bash", "-lc"]
              args:
                - scripts/run_speed_pipeline.sh
              env:
                - name: KAFKA_BOOTSTRAP_SERVERS
                  value: "kafka.kafka.svc:9092"
                - name: KAFKA_TOPIC_JOBS_RAW
                  value: "jobs_raw"
              volumeMounts:
                - name: crawler-runtime
                  mountPath: /app/runtime
          volumes:
            - name: crawler-runtime
              persistentVolumeClaim:
                claimName: crawler-runtime-pvc
```

---

## 12. Kubernetes CronJob mẫu cho batch

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: topcv-batch-pipeline
  namespace: ingestion
spec:
  schedule: "0 2 * * 0"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 2
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      backoffLimit: 1
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: topcv-batch
              image: bigdata-job-market/crawler:latest
              imagePullPolicy: IfNotPresent
              command: ["/bin/bash", "-lc"]
              args:
                - scripts/run_batch_pipeline.sh
              volumeMounts:
                - name: crawler-runtime
                  mountPath: /app/runtime
          volumes:
            - name: crawler-runtime
              persistentVolumeClaim:
                claimName: crawler-runtime-pvc
```

---

## 13. Kubernetes CronJob mẫu cho resume

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: topcv-batch-resume
  namespace: ingestion
spec:
  schedule: "0 4-23/2 * * 0"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      backoffLimit: 0
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: topcv-batch-resume
              image: bigdata-job-market/crawler:latest
              imagePullPolicy: IfNotPresent
              command: ["/bin/bash", "-lc"]
              args:
                - scripts/resume_batch_if_needed.sh
              volumeMounts:
                - name: crawler-runtime
                  mountPath: /app/runtime
          volumes:
            - name: crawler-runtime
              persistentVolumeClaim:
                claimName: crawler-runtime-pvc
```

---

## 14. Theo dõi trên Kubernetes

```bash
kubectl get cronjob -n ingestion
kubectl get jobs -n ingestion
kubectl get pods -n ingestion
kubectl logs -f <pod-name> -n ingestion
kubectl describe pod <pod-name> -n ingestion
```

Nếu dùng HDFS trong namespace `hdfs`:

```bash
kubectl get pods -n hdfs
kubectl exec -it hdfs-namenode-0 -n hdfs -- hdfs dfs -ls /raw/jobs/
```

---

## 15. Checklist bàn giao cho từng thành viên

### TV1 — Data Contract

TV1 cần kiểm tra:

```text
job_id
hash_content
ingest_ts
event_ts
payload
quality_flags
```

Nếu đổi field contract, báo TV2 trước khi merge.

### TV3 — Batch ETL

TV3 dùng:

```text
/raw/jobs/source=topcv/ingest_date=YYYY-MM-DD/
```

và đọc các file `jobs_batch_*.jsonl`.

### TV4 — Speed ETL

TV4 dùng Kafka topic:

```text
jobs_raw
```

key là `job_id`, value là full raw JSON.

### TV5 — Platform/Ops

TV5 dùng:

```text
scripts/*.sh
runtime/logs/
runtime/crawler/
CronJob YAML hoặc cron server
```

để deploy, monitor và debug.

---

## 16. Smoke test trước khi merge

Chạy:

```bash
uv run python -m apps.ingestion.run_crawler --mode speed --max-pages 1 --debug-card-links
```

Kỳ vọng:

```text
cards=50
no_url=0 hoặc rất thấp
no_time=0 hoặc rất thấp
```

Chạy:

```bash
uv run python -m apps.ingestion.run_crawler --mode batch --max-pages 2
```

Kiểm tra file:

```bash
find data/raw/jobs -type f
```

Test Kafka producer:

```bash
uv run python -m apps.producer.file_to_kafka   --input "data/raw/jobs/source=topcv/ingest_date=$(date +%F)/jobs_speed_*.jsonl"   --bootstrap-servers "$KAFKA_BOOTSTRAP_SERVERS"   --topic jobs_raw
```

Test HDFS upload:

```bash
LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=$(date +%F)/jobs_batch_*.jsonl | head -n 1)"
scripts/upload_to_hdfs.sh "$LATEST_FILE" "$(date +%F)"
```

---

## 17. Troubleshooting

### Không thấy JSONL output

File JSONL chỉ được tạo khi có record đầu tiên ghi thành công.

Kiểm tra:

```bash
find data/raw/jobs -type f
```

### Speed `link_mới=0`

Nếu log có:

```text
fresh=50
skipped_by_speed_cache=50
link_mới=0
```

thì bình thường. Nghĩa là job đã được xử lý trong cache 29 ngày.

Muốn test lại từ đầu:

```bash
rm -f runtime/crawler/speed_processed_jobs_29d.json
```

### Batch resume không làm gì

Kiểm tra:

```bash
cat runtime/crawler/batch_checkpoint.json
```

Nếu `completed=true`, resume sẽ tự thoát.

### HDFS upload lỗi

Kiểm tra HDFS:

```bash
kubectl get pods -n hdfs
kubectl exec -it hdfs-namenode-0 -n hdfs -- hdfs dfs -ls /raw/jobs/
```

### Kafka producer lỗi

Kiểm tra:

```bash
echo $KAFKA_BOOTSTRAP_SERVERS
```

Kiểm tra topic:

```bash
kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list
```

---

## 18. Kết luận

Các thành viên khác không cần sửa crawler core. Cách dùng đúng là:

```text
TV4:
run_speed_pipeline.sh → Kafka jobs_raw

TV3:
run_batch_pipeline.sh → HDFS raw zone

TV5:
cron/K8s CronJob + logs + runtime state

TV1:
review raw schema + data contract
```

Crawler core chỉ chịu trách nhiệm tạo raw JSONL chuẩn. Kafka producer và HDFS uploader là lớp tích hợp bên ngoài.
