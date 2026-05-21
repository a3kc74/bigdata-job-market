# Hướng dẫn tách logger chung cho crawler integration

Tài liệu này tổng hợp chiến lược logging nếu dùng **logger chung** trong branch `crawler`, các file cần thêm/sửa, và thứ tự sửa an toàn để không làm hỏng crawler core.

---

## 1. Vì sao nên dùng logger chung?

Hiện tại `topcv_crawler.py` đang tự cấu hình logging bằng `logging.basicConfig(...)` và chỉ log ra stdout. Trong cùng file, crawler cũng đang ghi các file runtime/error như:

```text
runtime/crawler/failed_links.json
runtime/crawler/missing_jobs.log
runtime/crawler/cookie_cache.json
```

Nếu mỗi module tự cấu hình logger riêng, hệ thống sẽ dễ bị:

```text
- format log không thống nhất
- log nằm lung tung
- duplicate log do nhiều handler
- khó cho TV5 gom log khi deploy bằng cron/K8s
- khó debug producer/crawler/HDFS upload cùng lúc
```

Vì vậy nên tách một logger chung:

```text
apps/common/logger.py
```

Tất cả module dùng chung:

```python
from apps.common.logger import get_logger

logger = get_logger("crawler")
logger = get_logger("producer")
logger = get_logger("hdfs_upload")
```

---

## 2. Mục tiêu logging sau khi tách

Sau khi sửa, hệ thống nên có 3 nhóm log rõ ràng.

### 2.1. Runtime logs

Dùng để biết pipeline có chạy ổn không:

```text
runtime/logs/
├── crawler.log
├── producer.log
├── speed_pipeline_YYYY-MM-DD.log
├── batch_pipeline_YYYY-MM-DD.log
├── batch_resume_YYYY-MM-DD.log
└── hdfs_upload_YYYY-MM-DD.log
```

Nội dung log:

```text
- crawler start/end
- đang quét page nào
- số card/fresh/link_mới
- bị block/HTTP error bao nhiêu lần
- producer gửi Kafka bao nhiêu record
- upload HDFS file nào
```

### 2.2. Data error logs

Dùng để kiểm tra dữ liệu lỗi hoặc link lỗi:

```text
runtime/crawler/
├── failed_links.json
└── missing_jobs.log
```

Không đưa các file này vào Kafka `jobs_raw`.

Nếu cần audit lâu dài, có thể upload riêng vào HDFS:

```text
/raw/errors/source=topcv/ingest_date=YYYY-MM-DD/
```

### 2.3. Metrics/monitoring

Prometheus/Grafana có thể làm sau. Giai đoạn này chỉ cần file logs + stdout là đủ để tích hợp với cron/K8s.

---

## 3. Cấu trúc repo sau khi thêm logger chung

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
└── .gitignore
```

Không nên dùng song song `logger.py` ở root nếu đã có `apps/common/logger.py`.

---

## 4. File cần thêm mới

## 4.1. Thêm `apps/common/__init__.py`

Tạo file rỗng:

```python
# apps/common/__init__.py
```

## 4.2. Thêm `apps/common/logger.py`

```python
import logging
import os
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Logger dùng chung cho toàn hệ thống.

    - Ghi ra stdout để cron/K8s đọc được.
    - Ghi vào runtime/logs/<name>.log để debug local/server.
    - Tránh add handler lặp khi module bị import nhiều lần.
    """
    os.makedirs("runtime/logs", exist_ok=True)

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

    file_handler = logging.FileHandler(
        f"runtime/logs/{name}.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger
```

Sau đó các module sẽ log đồng thời ra:

```text
stdout
runtime/logs/<name>.log
```

---

## 5. File cần sửa

## 5.1. Sửa `apps/ingestion/topcv_crawler.py`

### Việc cần làm

Bỏ cấu hình logger riêng trong crawler.

Tìm đoạn kiểu:

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

### Có cần xóa `import logging` và `import sys` không?

Nếu trong `topcv_crawler.py` không còn dùng trực tiếp `logging` hoặc `sys`, có thể xóa:

```python
import logging
import sys
```

Không được sửa phần extract/parser dữ liệu nếu không cần.

### Lưu ý

Giữ nguyên các file error/data-quality hiện tại:

```python
FAILED_LINKS_FILE = "runtime/crawler/failed_links.json"
MISSING_JOBS_LOG_FILE = "runtime/crawler/missing_jobs.log"
COOKIE_CACHE_FILE = "runtime/crawler/cookie_cache.json"
```

Logger chung chỉ thay cách ghi log vận hành, không thay cách ghi `failed_links.json` và `missing_jobs.log`.

---

## 5.2. Sửa `apps/producer/file_to_kafka.py`

### Việc cần làm

Thêm:

```python
from apps.common.logger import get_logger

logger = get_logger("producer")
```

Sau đó đổi `print(...)` thành logger.

Ví dụ:

```python
print(f"[KAFKA] Publishing file: {file_path}")
```

đổi thành:

```python
logger.info(f"[KAFKA] Publishing file: {file_path}")
```

```python
print(f"[SKIP] Missing job_id {file_path}:{line_no}")
```

đổi thành:

```python
logger.warning(f"[SKIP] Missing job_id {file_path}:{line_no}")
```

```python
print(f"[KAFKA ERROR] Delivery failed: {err}")
```

đổi thành:

```python
logger.error(f"[KAFKA ERROR] Delivery failed: {err}")
```

### Có nên thêm failed Kafka log không?

Nên thêm nếu muốn chắc hơn:

```text
runtime/producer/failed_kafka_messages.jsonl
```

Dùng khi Kafka gửi fail hoặc record không hợp lệ.

Ví dụ helper:

```python
import json
import os
from datetime import datetime

FAILED_KAFKA_FILE = "runtime/producer/failed_kafka_messages.jsonl"


def log_failed_kafka_message(record, reason):
    os.makedirs(os.path.dirname(FAILED_KAFKA_FILE), exist_ok=True)

    row = {
        "time": datetime.now().isoformat(),
        "reason": str(reason),
        "job_id": record.get("job_id") if isinstance(record, dict) else None,
        "record": record,
    }

    with open(FAILED_KAFKA_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
```

Phần này optional. Nếu chưa muốn phức tạp, chỉ cần đổi `print` sang `logger`.

---

## 5.3. Sửa hoặc xóa `logger.py` ở root

Nếu repo đang có:

```text
logger.py
```

ở root, không nên giữ song song 2 logger.

Có 2 lựa chọn:

### Lựa chọn A — Xóa root `logger.py`

Khuyến nghị:

```bash
git rm logger.py
```

Sau đó chỉ dùng:

```text
apps/common/logger.py
```

### Lựa chọn B — Giữ root `logger.py`

Chỉ giữ nếu có module khác đang import:

```python
from logger import get_logger
```

Nhưng nên sửa dần về:

```python
from apps.common.logger import get_logger
```

---

## 5.4. Sửa `scripts/run_speed_pipeline.sh`

Mục tiêu: log toàn bộ pipeline speed vào file theo ngày.

Mẫu:

```bash
#!/usr/bin/env bash
set -euo pipefail

mkdir -p runtime/logs

TODAY="$(date +%F)"
LOG_FILE="runtime/logs/speed_pipeline_${TODAY}.log"

{
  echo "[SPEED] Start at $(date)"

  uv run python -m apps.ingestion.run_crawler --mode speed

  LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=${TODAY}/jobs_speed_*.jsonl | head -n 1)"

  echo "[SPEED] Latest file: $LATEST_FILE"

  uv run python -m apps.producer.file_to_kafka \
    --input "$LATEST_FILE" \
    --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}" \
    --topic "${KAFKA_TOPIC_JOBS_RAW:-jobs_raw}"

  echo "[SPEED] Done at $(date)"
} >> "$LOG_FILE" 2>&1
```

---

## 5.5. Sửa `scripts/run_batch_pipeline.sh`

Mục tiêu: log toàn bộ pipeline batch vào file theo ngày.

```bash
#!/usr/bin/env bash
set -euo pipefail

mkdir -p runtime/logs

TODAY="$(date +%F)"
LOG_FILE="runtime/logs/batch_pipeline_${TODAY}.log"

{
  echo "[BATCH] Start at $(date)"

  uv run python -m apps.ingestion.run_crawler --mode batch

  LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=${TODAY}/jobs_batch_*.jsonl | head -n 1)"

  echo "[BATCH] Latest file: $LATEST_FILE"

  scripts/upload_to_hdfs.sh "$LATEST_FILE" "$TODAY"

  echo "[BATCH] Done at $(date)"
} >> "$LOG_FILE" 2>&1
```

---

## 5.6. Sửa `scripts/resume_batch_if_needed.sh`

Mục tiêu:

- log resume riêng;
- không chạy đè nếu batch chính vẫn đang chạy;
- chỉ resume khi checkpoint `completed=false`.

```bash
#!/usr/bin/env bash
set -euo pipefail

mkdir -p runtime/logs runtime/crawler

TODAY="$(date +%F)"
LOG_FILE="runtime/logs/batch_resume_${TODAY}.log"
LOCK_FILE="runtime/crawler/batch_pipeline.lock"
CHECKPOINT_FILE="runtime/crawler/batch_checkpoint.json"

{
  echo "[RESUME] Check at $(date)"

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

  uv run python -m apps.ingestion.run_crawler --mode batch --resume

  LATEST_FILE="$(ls -t data/raw/jobs/source=topcv/ingest_date=${TODAY}/jobs_batch_*.jsonl | head -n 1)"

  scripts/upload_to_hdfs.sh "$LATEST_FILE" "$TODAY"

  echo "[RESUME] Done at $(date)"
} >> "$LOG_FILE" 2>&1
```

---

## 5.7. Sửa `scripts/upload_to_hdfs.sh`

Mục tiêu: log upload HDFS riêng.

```bash
#!/usr/bin/env bash
set -euo pipefail

mkdir -p runtime/logs

LOCAL_FILE="${1:-}"
INGEST_DATE="${2:-$(date +%F)}"
LOG_FILE="runtime/logs/hdfs_upload_${INGEST_DATE}.log"

{
  echo "[HDFS] Start upload at $(date)"
  echo "[HDFS] Local file: $LOCAL_FILE"
  echo "[HDFS] Ingest date: $INGEST_DATE"

  if [ -z "$LOCAL_FILE" ]; then
    echo "[HDFS] ERROR: missing local file"
    exit 1
  fi

  if [ ! -f "$LOCAL_FILE" ]; then
    echo "[HDFS] ERROR: file not found: $LOCAL_FILE"
    exit 1
  fi

  HDFS_DIR="/raw/jobs/source=topcv/ingest_date=${INGEST_DATE}"
  FILENAME="$(basename "$LOCAL_FILE")"

  kubectl cp "$LOCAL_FILE" hdfs/hdfs-namenode-0:/tmp/"$FILENAME"

  kubectl exec hdfs-namenode-0 -n hdfs -- \
    hdfs dfs -mkdir -p "$HDFS_DIR"

  kubectl exec hdfs-namenode-0 -n hdfs -- \
    hdfs dfs -put -f /tmp/"$FILENAME" "$HDFS_DIR/"

  kubectl exec hdfs-namenode-0 -n hdfs -- \
    hdfs dfs -ls "$HDFS_DIR"

  echo "[HDFS] Done at $(date)"
} >> "$LOG_FILE" 2>&1
```

Nếu server có lệnh `hdfs` trực tiếp, thay phần `kubectl cp/kubectl exec` bằng `hdfs dfs`.

---

## 5.8. Sửa `.gitignore`

Cần chắc chắn có:

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

Vì logger chung sẽ tạo:

```text
runtime/logs/*.log
```

và crawler tạo:

```text
runtime/crawler/*.json
runtime/crawler/*.log
```

Toàn bộ `runtime/` không nên commit.

---

## 6. Tóm tắt file cần thêm/sửa

| File | Thao tác | Bắt buộc? | Lý do |
|---|---|---:|---|
| `apps/common/__init__.py` | Thêm mới | Có | Biến `apps/common` thành package |
| `apps/common/logger.py` | Thêm mới | Có | Logger chung |
| `apps/ingestion/topcv_crawler.py` | Sửa | Có | Bỏ `basicConfig`, dùng `get_logger("crawler")` |
| `apps/producer/file_to_kafka.py` | Sửa | Có | Đổi `print` sang `logger` |
| `logger.py` ở root | Xóa hoặc bỏ dùng | Nên | Tránh 2 chuẩn logger |
| `scripts/run_speed_pipeline.sh` | Sửa | Nên | Log speed pipeline theo ngày |
| `scripts/run_batch_pipeline.sh` | Sửa | Nên | Log batch pipeline theo ngày |
| `scripts/resume_batch_if_needed.sh` | Sửa | Nên | Log resume và dùng lock |
| `scripts/upload_to_hdfs.sh` | Sửa | Nên | Log HDFS upload |
| `.gitignore` | Sửa | Có | Không commit runtime logs/state |

---

## 7. Thứ tự sửa an toàn

Làm theo thứ tự này:

```text
1. Tạo apps/common/__init__.py
2. Tạo apps/common/logger.py
3. Sửa topcv_crawler.py dùng get_logger("crawler")
4. Sửa file_to_kafka.py dùng get_logger("producer")
5. Sửa .gitignore có runtime/
6. Test crawler speed --max-pages 1
7. Test producer với file speed
8. Sửa scripts/*.sh để log vào runtime/logs/
9. Test run_speed_pipeline.sh
10. Test run_batch_pipeline.sh với --max-pages nhỏ nếu cần
11. Xóa logger.py root nếu không còn dùng
12. Commit
```

---

## 8. Lệnh test sau khi sửa

Test import logger:

```bash
uv run python -c "from apps.common.logger import get_logger; logger=get_logger('test'); logger.info('logger ok')"
```

Kiểm tra file:

```bash
cat runtime/logs/test.log
```

Test crawler:

```bash
uv run python -m apps.ingestion.run_crawler --mode speed --max-pages 1
```

Kiểm tra:

```bash
cat runtime/logs/crawler.log
```

Test producer:

```bash
uv run python -m apps.producer.file_to_kafka \
  --input "data/raw/jobs/source=topcv/ingest_date=$(date +%F)/jobs_speed_*.jsonl" \
  --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}" \
  --topic jobs_raw
```

Kiểm tra:

```bash
cat runtime/logs/producer.log
```

---

## 9. Commit gợi ý

```bash
git add apps/common/__init__.py
git add apps/common/logger.py
git add apps/ingestion/topcv_crawler.py
git add apps/producer/file_to_kafka.py
git add scripts/run_speed_pipeline.sh
git add scripts/run_batch_pipeline.sh
git add scripts/resume_batch_if_needed.sh
git add scripts/upload_to_hdfs.sh
git add .gitignore

git commit -m "Add shared logger and standardize crawler pipeline logs"
```

Nếu xóa root logger:

```bash
git rm logger.py
git commit -m "Remove root logger after moving to apps/common"
```

---

## 10. Kết luận

Nếu dùng logger chung, bản cuối nên là:

```text
apps/common/logger.py
    → quản lý log format + stdout + runtime/logs/<name>.log

topcv_crawler.py
    → logger = get_logger("crawler")

file_to_kafka.py
    → logger = get_logger("producer")

scripts/*.sh
    → redirect log pipeline vào runtime/logs/*_YYYY-MM-DD.log
```

Chiến lược này giúp TV5 dễ deploy/monitor hơn, còn TV2 vẫn giữ crawler core sạch và không trộn operational logs vào raw job data.
