# Hướng dẫn cài đặt và triển khai logging cho TopCV Crawler trên Kubernetes

Tài liệu này hướng dẫn cách thiết kế, cài đặt và triển khai logging cho phần crawler của TV2 khi hệ thống cuối cùng chạy bằng Kubernetes/K8s.

Mục tiêu chính:

```text
1. Log vận hành phải xem được bằng kubectl logs.
2. State quan trọng của crawler phải được lưu bền vững bằng PVC.
3. Error/data-quality logs không được trộn vào raw jobs.
4. Crawler, producer, batch upload dùng chung format log.
5. Không phụ thuộc vào file log trong container khi chạy K8s.
```

---

## 1. Chiến lược logging cuối cùng

Khi chạy bằng Kubernetes, chiến lược đúng là:

```text
Operational logs  -> stdout/stderr -> kubectl logs
Runtime state     -> PVC mounted tại /app/runtime
Raw JSONL output  -> PVC mounted tại /app/data hoặc xử lý trong cùng pod
Error logs        -> runtime/crawler/failed_links.json, missing_jobs.log
Metrics           -> Prometheus/Grafana nếu còn thời gian
```

Không nên redirect toàn bộ script vào file theo kiểu:

```bash
} >> "$LOG_FILE" 2>&1
```

Vì nếu làm vậy, `kubectl logs <pod>` sẽ không thấy đầy đủ log. Trong K8s, log chuẩn phải in ra stdout/stderr.

---

## 2. Các loại log cần có

### 2.1. Application logs

Đây là log từ Python code.

Ví dụ:

```text
crawler started
scan page 1
cards=50 fresh=50 no_url=0
HTTP 403
saved 45 records
producer sent 45 messages
```

Module tạo log:

```text
apps/ingestion/topcv_crawler.py
apps/producer/file_to_kafka.py
```

Cách ghi:

```text
stdout là chính
file log là optional qua LOG_TO_FILE=true
```

---

### 2.2. Pipeline logs

Đây là log từ shell script.

Ví dụ:

```text
[SPEED] Start
[SPEED] Run crawler speed
[SPEED] Publish to Kafka
[SPEED] Done
```

Các file script:

```text
scripts/run_speed_pipeline.sh
scripts/run_batch_pipeline.sh
scripts/resume_batch_if_needed.sh
scripts/upload_to_hdfs.sh
```

Khi chạy K8s, các script này nên dùng `echo` trực tiếp ra stdout.

---

### 2.3. Data-quality/error logs

Đây không phải operational log, mà là dữ liệu lỗi cần lưu riêng.

```text
runtime/crawler/failed_links.json
runtime/crawler/missing_jobs.log
```

Ý nghĩa:

| File | Ý nghĩa |
|---|---|
| `failed_links.json` | Link bị HTTP error, request fail, parse fail |
| `missing_jobs.log` | Job crawl được nhưng thiếu field quan trọng |
| `batch_checkpoint.json` | Resume batch |
| `speed_processed_jobs_29d.json` | Chống speed gửi trùng |
| `cookie_cache.json` | Cache cookie để giảm số lần mở browser |

Không đưa các file này vào Kafka `jobs_raw`.

Nếu cần lưu lâu dài, có thể upload riêng vào HDFS:

```text
/raw/errors/source=topcv/ingest_date=YYYY-MM-DD/
```

Không để chung với raw jobs:

```text
/raw/jobs/source=topcv/ingest_date=YYYY-MM-DD/
```

---

## 3. Cấu trúc thư mục sau khi cài logging

Nên có:

```text
bigdata-job-market/
├── apps/
│   ├── common/
│   │   ├── __init__.py
│   │   └── logger.py
│   ├── ingestion/
│   │   ├── run_crawler.py
│   │   └── topcv_crawler.py
│   └── producer/
│       ├── __init__.py
│       └── file_to_kafka.py
├── scripts/
│   ├── run_speed_pipeline.sh
│   ├── run_batch_pipeline.sh
│   ├── resume_batch_if_needed.sh
│   └── upload_to_hdfs.sh
├── infra/
│   └── kubernetes/
│       └── ingestion/
│           ├── namespace.yaml
│           ├── crawler-pvc.yaml
│           ├── topcv-speed-cronjob.yaml
│           ├── topcv-batch-cronjob.yaml
│           └── topcv-batch-resume-cronjob.yaml
└── .gitignore
```

---

## 4. File cần thêm mới

### 4.1. `apps/common/__init__.py`

Tạo file rỗng:

```python
# apps/common/__init__.py
```

---

### 4.2. `apps/common/logger.py`

Tạo file:

```python
import logging
import os
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Logger chung cho server/Kubernetes.

    Mặc định:
    - luôn log ra stdout để kubectl logs đọc được.
    - chỉ log ra file nếu LOG_TO_FILE=true.

    Trong Kubernetes:
    - LOG_TO_FILE=false
    - log chính xem bằng kubectl logs

    Khi chạy local/server:
    - có thể bật LOG_TO_FILE=true để ghi thêm vào runtime/logs/<name>.log
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    log_to_file = os.getenv("LOG_TO_FILE", "false").lower() == "true"

    if log_to_file:
        os.makedirs("runtime/logs", exist_ok=True)

        file_handler = logging.FileHandler(
            f"runtime/logs/{name}.log",
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
```

---

## 5. File cần sửa

### 5.1. Sửa `apps/ingestion/topcv_crawler.py`

Trong file crawler, bỏ logger riêng kiểu:

```python
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | CRAWLER | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
```

Thay bằng:

```python
from apps.common.logger import get_logger

logger = get_logger("crawler")
```

Giữ nguyên logic crawler, parser, cache, checkpoint, failed links, missing jobs.

Không sửa phần trích xuất dữ liệu nếu không cần.

---

### 5.2. Sửa `apps/producer/file_to_kafka.py`

Thêm:

```python
from apps.common.logger import get_logger

logger = get_logger("producer")
```

Đổi các `print(...)` thành logger:

```python
logger.info(...)
logger.warning(...)
logger.error(...)
```

Ví dụ:

```python
logger.info(f"[KAFKA] Publishing file: {file_path}")
logger.warning(f"[SKIP] Missing job_id {file_path}:{line_no}")
logger.error(f"[KAFKA ERROR] Delivery failed: {err}")
```

Nếu muốn ghi Kafka failed messages riêng, thêm runtime file:

```text
runtime/producer/failed_kafka_messages.jsonl
```

Phần này optional.

---

### 5.3. Sửa `.gitignore`

Thêm chắc chắn:

```gitignore
runtime/
data/raw/jobs/
__pycache__/
*.pyc
.venv/
project.log
debug_page_*.html

processed_links.txt
raw_jobs_batch.jsonl
missing_jobs.log
failed_links.json
checkpoint.txt
cookie_cache.json
```

Lý do:

```text
runtime/ chứa cookie, checkpoint, speed cache, failed links, logs.
data/raw/jobs/ chứa output JSONL runtime.
```

Không commit những file này.

---

## 6. Shell script theo chuẩn K8s

Khi triển khai bằng K8s, script nên in ra stdout. Không redirect cứng vào file log.

---

### 6.1. `scripts/run_speed_pipeline.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

mkdir -p runtime/crawler data/raw/jobs

echo "======================================================="
echo "[SPEED] Start at $(date)"
echo "======================================================="

SPEED_MAX_PAGES="${SPEED_MAX_PAGES:-15}"
SPEED_UPDATED_WITHIN_MINUTES="${SPEED_UPDATED_WITHIN_MINUTES:-30}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_TOPIC_JOBS_RAW="${KAFKA_TOPIC_JOBS_RAW:-jobs_raw}"

echo "[SPEED] max_pages=${SPEED_MAX_PAGES}"
echo "[SPEED] updated_within_minutes=${SPEED_UPDATED_WITHIN_MINUTES}"
echo "[SPEED] kafka_bootstrap_servers=${KAFKA_BOOTSTRAP_SERVERS}"
echo "[SPEED] kafka_topic=${KAFKA_TOPIC_JOBS_RAW}"

echo "[SPEED] Run crawler speed"

uv run python -m apps.ingestion.run_crawler \
  --mode speed \
  --max-pages "$SPEED_MAX_PAGES" \
  --updated-within-minutes "$SPEED_UPDATED_WITHIN_MINUTES"

TODAY="$(date +%F)"

echo "[SPEED] Find latest speed JSONL"

LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=${TODAY}/jobs_speed_*.jsonl 2>/dev/null | head -n 1 || true)"

if [ -z "$LATEST_FILE" ]; then
  echo "[SPEED] No speed JSONL file found. Nothing to send to Kafka."
  exit 0
fi

echo "[SPEED] Latest file: $LATEST_FILE"

echo "[SPEED] Publish to Kafka"

uv run python -m apps.producer.file_to_kafka \
  --input "$LATEST_FILE" \
  --bootstrap-servers "$KAFKA_BOOTSTRAP_SERVERS" \
  --topic "$KAFKA_TOPIC_JOBS_RAW"

echo "[SPEED] Done at $(date)"
echo "======================================================="
```

---

### 6.2. `scripts/run_batch_pipeline.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

mkdir -p runtime/crawler data/raw/jobs

echo "======================================================="
echo "[BATCH] Start at $(date)"
echo "======================================================="

LOCK_FILE="runtime/crawler/batch_pipeline.lock"

exec 9>"$LOCK_FILE"

if ! flock -n 9; then
  echo "[BATCH] Another batch/resume process is still running. Exit."
  exit 0
fi

BATCH_DAYS="${BATCH_DAYS:-7}"
BATCH_MAX_PAGES="${BATCH_MAX_PAGES:-0}"

echo "[BATCH] days=${BATCH_DAYS}"
echo "[BATCH] max_pages=${BATCH_MAX_PAGES}"

echo "[BATCH] Run crawler batch"

uv run python -m apps.ingestion.run_crawler \
  --mode batch \
  --days "$BATCH_DAYS" \
  --max-pages "$BATCH_MAX_PAGES"

TODAY="$(date +%F)"

echo "[BATCH] Find latest batch JSONL"

LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=${TODAY}/jobs_batch_*.jsonl 2>/dev/null | head -n 1 || true)"

if [ -z "$LATEST_FILE" ]; then
  echo "[BATCH] ERROR: No batch JSONL file found for ${TODAY}."
  exit 1
fi

echo "[BATCH] Latest file: $LATEST_FILE"

echo "[BATCH] Upload to HDFS"

bash scripts/upload_to_hdfs.sh "$LATEST_FILE" "$TODAY"

echo "[BATCH] Done at $(date)"
echo "======================================================="
```

---

### 6.3. `scripts/resume_batch_if_needed.sh`

Nếu bạn chọn resume chỉ trong ngày Chủ nhật, dùng bản này:

```bash
#!/usr/bin/env bash
set -euo pipefail

mkdir -p runtime/crawler data/raw/jobs

echo "======================================================="
echo "[RESUME] Check at $(date)"
echo "======================================================="

TODAY="$(date +%F)"
LOCK_FILE="runtime/crawler/batch_pipeline.lock"
CHECKPOINT_FILE="runtime/crawler/batch_checkpoint.json"

exec 9>"$LOCK_FILE"

if ! flock -n 9; then
  echo "[RESUME] Batch is still running. Skip resume."
  exit 0
fi

if [ ! -f "$CHECKPOINT_FILE" ]; then
  echo "[RESUME] No checkpoint. Nothing to resume."
  exit 0
fi

if grep -q '"completed": true' "$CHECKPOINT_FILE"; then
  echo "[RESUME] Checkpoint completed. Nothing to resume."
  exit 0
fi

echo "[RESUME] Incomplete checkpoint found. Resume batch."

uv run python -m apps.ingestion.run_crawler \
  --mode batch \
  --resume

echo "[RESUME] Find latest batch JSONL for today"

LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=${TODAY}/jobs_batch_*.jsonl 2>/dev/null | head -n 1 || true)"

if [ -z "$LATEST_FILE" ]; then
  echo "[RESUME] ERROR: No batch JSONL file found for ${TODAY}."
  exit 1
fi

echo "[RESUME] Latest file: $LATEST_FILE"

echo "[RESUME] Upload to HDFS"

bash scripts/upload_to_hdfs.sh "$LATEST_FILE" "$TODAY"

echo "[RESUME] Done at $(date)"
echo "======================================================="
```

---

### 6.4. `scripts/upload_to_hdfs.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

LOCAL_FILE="${1:-}"
INGEST_DATE="${2:-$(date +%F)}"

HDFS_MODE="${HDFS_MODE:-k8s}"
HDFS_NAMESPACE="${HDFS_NAMESPACE:-hdfs}"
HDFS_NAMENODE_POD="${HDFS_NAMENODE_POD:-hdfs-namenode-0}"
HDFS_BASE_DIR="${HDFS_BASE_DIR:-/raw/jobs/source=topcv}"

echo "======================================================="
echo "[HDFS] Start upload at $(date)"
echo "======================================================="

echo "[HDFS] Local file: $LOCAL_FILE"
echo "[HDFS] Ingest date: $INGEST_DATE"
echo "[HDFS] HDFS mode: $HDFS_MODE"
echo "[HDFS] HDFS base dir: $HDFS_BASE_DIR"

if [ -z "$LOCAL_FILE" ]; then
  echo "[HDFS] ERROR: missing local file"
  echo "Usage: scripts/upload_to_hdfs.sh <local_jsonl_file> <ingest_date>"
  exit 1
fi

if [ ! -f "$LOCAL_FILE" ]; then
  echo "[HDFS] ERROR: file not found: $LOCAL_FILE"
  exit 1
fi

HDFS_DIR="${HDFS_BASE_DIR}/ingest_date=${INGEST_DATE}"
FILENAME="$(basename "$LOCAL_FILE")"

echo "[HDFS] Target HDFS dir: $HDFS_DIR"
echo "[HDFS] Filename: $FILENAME"

if [ "$HDFS_MODE" = "direct" ]; then
  echo "[HDFS] Using direct hdfs dfs command"

  hdfs dfs -mkdir -p "$HDFS_DIR"
  hdfs dfs -put -f "$LOCAL_FILE" "$HDFS_DIR/"
  hdfs dfs -ls "$HDFS_DIR"

elif [ "$HDFS_MODE" = "k8s" ]; then
  echo "[HDFS] Using Kubernetes NameNode pod"
  echo "[HDFS] Namespace: $HDFS_NAMESPACE"
  echo "[HDFS] NameNode pod: $HDFS_NAMENODE_POD"

  kubectl cp "$LOCAL_FILE" "${HDFS_NAMESPACE}/${HDFS_NAMENODE_POD}:/tmp/${FILENAME}"

  kubectl exec -n "$HDFS_NAMESPACE" "$HDFS_NAMENODE_POD" -- \
    hdfs dfs -mkdir -p "$HDFS_DIR"

  kubectl exec -n "$HDFS_NAMESPACE" "$HDFS_NAMENODE_POD" -- \
    hdfs dfs -put -f "/tmp/${FILENAME}" "$HDFS_DIR/"

  kubectl exec -n "$HDFS_NAMESPACE" "$HDFS_NAMENODE_POD" -- \
    hdfs dfs -ls "$HDFS_DIR"

else
  echo "[HDFS] ERROR: invalid HDFS_MODE=$HDFS_MODE"
  echo "[HDFS] Use HDFS_MODE=k8s or HDFS_MODE=direct"
  exit 1
fi

echo "[HDFS] Done at $(date)"
echo "======================================================="
```

---

## 7. Kubernetes manifests cần thêm

### 7.1. `infra/kubernetes/ingestion/namespace.yaml`

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: ingestion
```

---

### 7.2. `infra/kubernetes/ingestion/crawler-pvc.yaml`

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: crawler-runtime-pvc
  namespace: ingestion
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 2Gi
```

PVC này lưu:

```text
/app/runtime
/app/data
```

---

### 7.3. `infra/kubernetes/ingestion/topcv-speed-cronjob.yaml`

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
                - name: LOG_TO_FILE
                  value: "false"
                - name: SPEED_MAX_PAGES
                  value: "15"
                - name: SPEED_UPDATED_WITHIN_MINUTES
                  value: "30"
                - name: KAFKA_BOOTSTRAP_SERVERS
                  value: "kafka.kafka.svc:9092"
                - name: KAFKA_TOPIC_JOBS_RAW
                  value: "jobs_raw"
              volumeMounts:
                - name: crawler-state
                  mountPath: /app/runtime
                - name: crawler-state
                  mountPath: /app/data
                  subPath: data
          volumes:
            - name: crawler-state
              persistentVolumeClaim:
                claimName: crawler-runtime-pvc
```

---

### 7.4. `infra/kubernetes/ingestion/topcv-batch-cronjob.yaml`

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
      backoffLimit: 2
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
              env:
                - name: LOG_TO_FILE
                  value: "false"
                - name: BATCH_DAYS
                  value: "7"
                - name: BATCH_MAX_PAGES
                  value: "0"
                - name: HDFS_MODE
                  value: "k8s"
                - name: HDFS_NAMESPACE
                  value: "hdfs"
                - name: HDFS_NAMENODE_POD
                  value: "hdfs-namenode-0"
                - name: HDFS_BASE_DIR
                  value: "/raw/jobs/source=topcv"
              volumeMounts:
                - name: crawler-state
                  mountPath: /app/runtime
                - name: crawler-state
                  mountPath: /app/data
                  subPath: data
          volumes:
            - name: crawler-state
              persistentVolumeClaim:
                claimName: crawler-runtime-pvc
```

---

### 7.5. `infra/kubernetes/ingestion/topcv-batch-resume-cronjob.yaml`

Nếu bạn vẫn muốn resume riêng trong Chủ nhật:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: topcv-batch-resume
  namespace: ingestion
spec:
  schedule: "0 5-23/3 * * 0"
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
              env:
                - name: LOG_TO_FILE
                  value: "false"
                - name: HDFS_MODE
                  value: "k8s"
                - name: HDFS_NAMESPACE
                  value: "hdfs"
                - name: HDFS_NAMENODE_POD
                  value: "hdfs-namenode-0"
                - name: HDFS_BASE_DIR
                  value: "/raw/jobs/source=topcv"
              volumeMounts:
                - name: crawler-state
                  mountPath: /app/runtime
                - name: crawler-state
                  mountPath: /app/data
                  subPath: data
          volumes:
            - name: crawler-state
              persistentVolumeClaim:
                claimName: crawler-runtime-pvc
```

---

## 8. Docker image cho crawler

Cần thêm Dockerfile, ví dụ:

```text
infra/crawler/Dockerfile
```

Ví dụ cơ bản:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    bash \
    curl \
    wget \
    unzip \
    ca-certificates \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./

RUN pip install uv && uv sync --frozen

COPY apps ./apps
COPY scripts ./scripts

RUN chmod +x scripts/*.sh

ENV PYTHONPATH=/app
ENV LOG_TO_FILE=false

CMD ["bash"]
```

Lưu ý: Dockerfile thực tế có thể cần chỉnh tùy dependency `nodriver`, Chrome/Chromium và cách project đang dùng `uv`.

---

## 9. Build và deploy trên Minikube

Trỏ Docker CLI vào Docker daemon của Minikube:

```bash
eval $(minikube docker-env)
```

PowerShell:

```powershell
& minikube -p minikube docker-env --shell powershell | Invoke-Expression
```

Build image:

```bash
docker build -f infra/crawler/Dockerfile -t bigdata-job-market/crawler:latest .
```

Apply manifests:

```bash
kubectl apply -f infra/kubernetes/ingestion/namespace.yaml
kubectl apply -f infra/kubernetes/ingestion/crawler-pvc.yaml
kubectl apply -f infra/kubernetes/ingestion/topcv-speed-cronjob.yaml
kubectl apply -f infra/kubernetes/ingestion/topcv-batch-cronjob.yaml
kubectl apply -f infra/kubernetes/ingestion/topcv-batch-resume-cronjob.yaml
```

---

## 10. Cách xem log trên K8s

Xem CronJob:

```bash
kubectl get cronjob -n ingestion
```

Xem Jobs:

```bash
kubectl get jobs -n ingestion
```

Xem Pods:

```bash
kubectl get pods -n ingestion
```

Xem log pod:

```bash
kubectl logs -f <pod-name> -n ingestion
```

Xem log pod đã chạy xong:

```bash
kubectl logs <pod-name> -n ingestion
```

Debug pod lỗi:

```bash
kubectl describe pod <pod-name> -n ingestion
```

Xem HDFS:

```bash
kubectl get pods -n hdfs

kubectl exec -it hdfs-namenode-0 -n hdfs -- \
  hdfs dfs -ls /raw/jobs/source=topcv/
```

---

## 11. Test thủ công một CronJob

Trigger speed job thủ công:

```bash
kubectl create job --from=cronjob/topcv-speed-pipeline \
  test-speed-$(date +%Y%m%d%H%M) -n ingestion
```

Trigger batch job thủ công:

```bash
kubectl create job --from=cronjob/topcv-batch-pipeline \
  test-batch-$(date +%Y%m%d%H%M) -n ingestion
```

Xem pod tương ứng:

```bash
kubectl get pods -n ingestion
kubectl logs -f <pod-name> -n ingestion
```

---

## 12. Nếu vẫn muốn file logs khi chạy local

Mặc định K8s dùng:

```text
LOG_TO_FILE=false
```

Khi chạy local/server, có thể bật:

```bash
LOG_TO_FILE=true uv run python -m apps.ingestion.run_crawler --mode speed --max-pages 1
```

Khi đó sẽ có:

```text
runtime/logs/crawler.log
```

Test:

```bash
cat runtime/logs/crawler.log
```

---

## 13. Checklist triển khai logging

Trước khi merge/deploy:

```text
[ ] Có apps/common/logger.py
[ ] topcv_crawler.py dùng get_logger("crawler")
[ ] file_to_kafka.py dùng get_logger("producer")
[ ] Shell scripts echo ra stdout, không redirect cứng vào file
[ ] .gitignore có runtime/ và data/raw/jobs/
[ ] K8s CronJob có LOG_TO_FILE=false
[ ] K8s CronJob mount PVC vào /app/runtime
[ ] K8s CronJob mount PVC vào /app/data
[ ] failed_links.json và missing_jobs.log nằm trong runtime/crawler/
[ ] Xem được log bằng kubectl logs
[ ] HDFS upload log hiện trong kubectl logs của batch pod
```

---

## 14. Quy tắc cuối cùng

```text
K8s logging:
- stdout/stderr là nguồn log chính.
- file log chỉ dùng optional khi LOG_TO_FILE=true.

Crawler state:
- cookie_cache, checkpoint, speed cache, failed_links, missing_jobs phải nằm trên PVC.

Raw job output:
- jobs_speed/jobs_batch nằm trong data/ để producer/uploader xử lý.

Error logs:
- không đưa vào Kafka jobs_raw.
- không để chung raw jobs.
- nếu cần audit, upload riêng vào /raw/errors/.
```

Tóm lại:

```text
Kubernetes thu log qua stdout.
PVC lưu state.
HDFS/Kafka nhận data, không nhận operational logs.
```
