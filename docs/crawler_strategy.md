# TopCV Crawler Strategy

## 1. Mục tiêu thiết kế

Crawler thuộc phần **TV2 — Data Sources Owner**. Mục tiêu là tạo đầu vào ổn định cho Lambda architecture của hệ thống Job Market:

```text
TopCV
  ├── Speed mode  → JSONL speed → Kafka jobs_raw → Spark Streaming
  └── Batch mode  → JSONL batch → HDFS raw zone → Spark Batch ETL
```

Crawler **không trực tiếp xử lý phân tích**. Nó chỉ đảm nhiệm:

- Crawl job từ TopCV.
- Chuẩn hóa record raw theo data contract.
- Ghi JSONL local.
- Tạo metadata: `job_id`, `hash_content`, `ingest_ts`, `event_ts`, `source`.
- Ghi log lỗi và data quality.
- Hỗ trợ speed producer và HDFS uploader chạy bên ngoài.

Thiết kế này giúp crawler độc lập với Kafka/HDFS. Nếu Kafka hoặc HDFS lỗi, crawler vẫn có thể tạo JSONL để replay sau.

---

## 2. Vai trò trong Lambda Architecture

| Layer | Cách crawler hỗ trợ |
|---|---|
| Raw | Tạo JSONL raw, giữ payload gốc và metadata ingest |
| Batch | Batch crawler tạo `jobs_batch_*.jsonl` để upload vào HDFS raw zone |
| Speed | Speed crawler tạo `jobs_speed_*.jsonl` để Kafka producer gửi vào `jobs_raw` |
| Serving | Không ghi trực tiếp vào Cassandra/Elasticsearch; việc này thuộc batch/speed ETL |
| Ops | Có runtime log, checkpoint, cache, failed link, missing data log |

---

## 3. Hai mode crawl

### 3.1. Speed mode

Speed mode phục vụ dữ liệu gần realtime.

Cấu hình mặc định:

```text
max_pages = 15
updated_within_minutes = 30
speed_processed_cache_ttl_days = 29
```

Luồng xử lý:

```text
1. Quét các page đầu của TopCV.
2. Lấy các job có updated_time >= now - 30 phút.
3. Tính job_id từ normalized URL.
4. Nếu job_id đã có trong speed cache 29 ngày → bỏ qua.
5. Nếu chưa có → crawl detail.
6. Ghi record vào jobs_speed_*.jsonl.
7. Mark job_id vào speed cache sau khi ghi thành công.
8. Kafka producer đọc file speed và gửi vào topic jobs_raw.
```

Speed mode **không cố bắt mọi update của job cũ**. Nó ưu tiên giảm request vào TopCV, tránh bắn trùng Kafka, và đưa dữ liệu mới vào streaming layer.

### 3.2. Batch mode

Batch mode phục vụ dữ liệu lịch sử/đầy đủ hơn.

Cấu hình mặc định:

```text
days = 7
max_pages = 0  # dùng total pages thật từ TopCV
use_processed_cache = false
```

Luồng xử lý:

```text
1. Lấy tổng số page.
2. Quét từ page 1.
3. Lọc job có updated_time >= now - 7 ngày.
4. Không dùng speed cache.
5. Crawl lại cả job đã thấy trước đó nếu nó còn nằm trong threshold.
6. Ghi record vào jobs_batch_*.jsonl.
7. Upload JSONL batch vào HDFS raw zone.
```

Batch **không dùng processed cache**, vì batch cần bắt được job đã cập nhật lại trong vòng 7 ngày. Nếu một job đã crawl hôm trước nhưng hôm nay công ty sửa salary/description/deadline, batch phải crawl lại để downstream dùng `job_id + hash_content` phát hiện version mới.

---

## 4. Ý nghĩa của log listing page

Ví dụ:

```text
cards=50 | parse_time=50 | fresh=50 | old/skip=0 | no_url=0 | no_time=0
```

| Field | Ý nghĩa |
|---|---|
| `cards` | Số job card tìm thấy trên listing page |
| `parse_time` | Số card parse được `updated_time` |
| `fresh` | Số job nằm trong cửa sổ thời gian cần crawl |
| `old/skip` | Số job cũ hơn threshold thời gian |
| `no_url` | Số card không lấy được URL detail |
| `no_time` | Số card không parse được updated time |

`fresh` chưa chắc được crawl detail. Sau `fresh`, speed còn lọc tiếp bằng speed cache.

Ví dụ:

```text
fresh=50
skipped_by_speed_cache=44
link_mới=6
```

Nghĩa là 50 job mới theo thời gian, nhưng 44 job đã crawl/bắn rồi, chỉ còn 6 job mới đưa vào queue.

---

## 5. URL discovery strategy

TopCV có ít nhất 2 dạng URL detail hợp lệ:

```text
https://www.topcv.vn/viec-lam/<slug>/<id>.html
https://www.topcv.vn/brand/<brand>/tuyen-dung/<slug>-j<id>.html
```

Crawler phải nhận cả hai dạng này.

Logic lấy URL:

1. Quét các attribute có thể chứa link:
   - `data-redirect-to`
   - `data-url`
   - `data-href`
   - `data-link`
   - `data-target`
2. Fallback sang `a[href]`.
3. Chuẩn hóa URL.
4. Chỉ nhận URL job detail:
   - path chứa `/viec-lam/` và kết thúc `.html`, hoặc
   - path chứa `/brand/` + `/tuyen-dung/` và kết thúc `.html`.

Có thể bật debug card links:

```bash
uv run python -m apps.ingestion.run_crawler --mode speed --max-pages 1 --debug-card-links
```

Output debug:

```text
runtime/crawler/debug_card_links/card_links_page_1_*.json
```

---

## 6. `event_ts` và `ingest_ts`

Crawler dùng 2 loại thời gian:

| Field | Ý nghĩa |
|---|---|
| `event_ts` | Thời gian job được TopCV cập nhật trên listing page nếu parse được; fallback có thể là `datePosted` |
| `ingest_ts` | Thời điểm crawler ghi nhận record |

Với speed layer, `event_ts` quan trọng vì Spark Structured Streaming dùng để watermark/window/dedup.

`stream_ingest_ts` không được thêm vào crawler vì trong nhóm có thể coi nó tương đương `ingest_ts`. Nếu speed layer yêu cầu field này, producer hoặc transform layer có thể map:

```text
stream_ingest_ts = ingest_ts
```

---

## 7. Job identity và versioning

Crawler tạo:

```text
job_id = sha256(source + normalized_source_url)
```

Dùng để định danh job ổn định.

Crawler tạo:

```text
hash_content = sha256(title + company + location + salary + requirements + employment_type)
```

Dùng để phát hiện nội dung thay đổi.

| Tình huống | Ý nghĩa |
|---|---|
| same `job_id`, same `hash_content` | Trùng record |
| same `job_id`, different `hash_content` | Cùng job nhưng có version/content mới |
| different `job_id` | Job khác |

---

## 8. Anti-bot và giảm request

TopCV có Cloudflare, nên crawler ưu tiên giảm request thay vì crawl mù quáng.

Chiến lược:

1. Dùng cookie cache:
   ```text
   runtime/crawler/cookie_cache.json
   ```
2. Chỉ gọi browser/nodriver khi chưa có cookie, cookie hết hạn, hoặc request bị block.
3. Speed dùng cache 29 ngày để không crawl lại detail job đã xử lý.
4. Batch không dùng cache, nhưng dùng threshold 7 ngày để dừng sớm khi page không còn job fresh.
5. Không rotate socket/session định kỳ sau 40 request.
6. Chỉ tạo session mới khi bị block, lấy cookie mới, thử lại cookie mới trên session hiện tại vẫn bị block.
7. Có cooldown sau mỗi 40 request để giảm áp lực.

---

## 9. Recovery khi bị block

Khi request bị block hoặc response rỗng:

```text
1. Sleep 180–300 giây.
2. Mở nodriver để lấy cookie mới.
3. Thử cookie mới trên session hiện tại.
4. Nếu vẫn block → đóng session cũ, tạo session mới.
5. Thử lại một lần.
6. Nếu vẫn lỗi → ghi failed link.
```

Crawler không đảm bảo vượt Cloudflare 100%. Mục tiêu là hồi phục lỗi nhẹ, không spam request, và ghi lại lỗi để retry/audit.

---

## 10. Checkpoint strategy

### 10.1. Speed

Speed không dùng page checkpoint.

Lý do: job mới luôn nằm ở page đầu. Nếu resume từ page cũ, có thể bỏ lỡ job mới.

Speed dùng cache 29 ngày để chống trùng.

### 10.2. Batch

Batch có checkpoint JSON để resume nếu batch bị chết giữa chừng:

```text
runtime/crawler/batch_checkpoint.json
```

Checkpoint lưu:

```json
{
  "mode": "batch",
  "run_id": "batch_YYYYMMDD_HHMMSS",
  "created_at": "...",
  "updated_at": "...",
  "threshold_time": "...",
  "next_page": 36,
  "output_file": ".../jobs_batch_YYYYMMDD_HHMMSS.jsonl",
  "completed": false
}
```

TTL checkpoint:

```text
2 ngày
```

Nếu quá 2 ngày, checkpoint bị coi là cũ và không resume nữa.

`--resume` chỉ dùng khi batch bị chết giữa chừng, không dùng để retry từng failed link.

---

## 11. Output contract

Crawler ghi JSONL, mỗi dòng là một JSON object.

Output speed:

```text
data/raw/jobs/source=topcv/ingest_date=YYYY-MM-DD/jobs_speed_YYYYMMDD_HHMMSS.jsonl
```

Output batch:

```text
data/raw/jobs/source=topcv/ingest_date=YYYY-MM-DD/jobs_batch_YYYYMMDD_HHMMSS.jsonl
```

Các field chính:

```text
source
source_url
normalized_source_url
crawl_version
ingest_ts
event_ts
job_id
hash_content
payload
quality_flags
```

`payload` chứa nội dung job gốc và các field đã extract:

```text
title
company_name
salary
location
monthOfExperience
deadline
description
requirements
benefits
skillsNeeded
skillsShouldHave
sectionsByHeading
pageText
json_ld
meta_tags
```

---

## 12. Logging strategy

Crawler có 2 nhóm log.

### 12.1. Runtime logs

Dùng để theo dõi vận hành:

```text
runtime/logs/speed_pipeline_YYYY-MM-DD.log
runtime/logs/batch_pipeline_YYYY-MM-DD.log
runtime/logs/kafka_producer.log
runtime/logs/hdfs_upload.log
```

### 12.2. Data quality/error logs

```text
runtime/crawler/failed_links.json
runtime/crawler/missing_jobs.log
```

`failed_links.json` ghi HTTP 403/401, request fail, parse fail, detail response rỗng.

`missing_jobs.log` ghi job thiếu field quan trọng:

```text
salary
location
experience
requirements
description
benefits
```

Không đưa 2 file này vào `jobs_raw` bình thường. Nếu cần audit lâu dài, có thể upload vào:

```text
/raw/errors/source=topcv/ingest_date=YYYY-MM-DD/
```

---

## 13. Tích hợp Speed layer

Speed pipeline:

```text
run_crawler --mode speed
  → jobs_speed_*.jsonl
  → file_to_kafka.py
  → Kafka topic jobs_raw
  → Spark Structured Streaming
```

Kafka producer nên:

- đọc JSONL;
- key = `job_id`;
- value = raw JSON string;
- topic default = `jobs_raw`;
- không sửa schema ngoài việc có thể thêm `stream_ingest_ts = ingest_ts` nếu TV4 cần.

---

## 14. Tích hợp Batch layer

Batch pipeline:

```text
run_crawler --mode batch
  → jobs_batch_*.jsonl
  → upload_to_hdfs.sh
  → HDFS /raw/jobs/source=topcv/ingest_date=YYYY-MM-DD/
  → Spark Batch ETL raw → bronze → silver → gold
```

Nếu HDFS chạy trong Minikube, uploader cần:

1. `kubectl cp` file vào NameNode pod.
2. `kubectl exec` để chạy `hdfs dfs -mkdir -p`.
3. `kubectl exec` để chạy `hdfs dfs -put -f`.

---

## 15. Automation strategy

Trên server có thể dùng cron:

```cron
*/15 * * * * cd /path/to/bigdata-job-market && scripts/run_speed_pipeline.sh >> runtime/logs/speed_cron.log 2>&1

0 2 * * 0 cd /path/to/bigdata-job-market && scripts/run_batch_pipeline.sh >> runtime/logs/batch_cron.log 2>&1

0 4-23/2 * * 0 cd /path/to/bigdata-job-market && scripts/resume_batch_if_needed.sh >> runtime/logs/batch_resume_cron.log 2>&1
```

Batch và resume phải dùng chung lock file:

```text
runtime/crawler/batch_pipeline.lock
```

Nếu batch chính còn chạy, resume tự skip để tránh crawl song song.

---

## 16. K8s deployment strategy

Có 2 cách triển khai.

### 16.1. Cách đơn giản

Crawler chạy bằng cron trên server. Kafka/HDFS/Spark chạy trong Kubernetes/Minikube. Cách này phù hợp khi crawler cần Chrome/nodriver và server đã có Chrome.

### 16.2. Cách đầy đủ trên K8s

Đóng gói crawler vào Docker image có Python, project code, dependencies, Chrome/Chromium, và quyền ghi vào volume runtime.

Tạo Kubernetes CronJob:

- speed CronJob chạy mỗi 15 phút;
- batch CronJob chạy mỗi tuần;
- resume CronJob chạy sau batch chính và dùng lock/checkpoint.

Nếu dùng Minikube, cần build image bên trong Docker daemon của Minikube trước khi apply CronJob.

---

## 17. Quy tắc commit

Không commit runtime data:

```gitignore
runtime/
data/raw/jobs/
__pycache__/
*.pyc
.venv/
debug_page_*.html
```

Nên commit:

```text
apps/ingestion/topcv_crawler.py
apps/ingestion/run_crawler.py
apps/producer/file_to_kafka.py
scripts/run_speed_pipeline.sh
scripts/run_batch_pipeline.sh
scripts/resume_batch_if_needed.sh
scripts/upload_to_hdfs.sh
pyproject.toml
uv.lock
docs/crawler_strategy.md
apps/ingestion/README.md
```

---

## 18. Checklist vận hành

```text
[ ] Speed chạy được với --max-pages 1.
[ ] Debug card links cho page 1 có no_url = 0 hoặc rất thấp.
[ ] Batch chạy được với --max-pages 2.
[ ] Output JSONL đúng path.
[ ] Producer gửi được Kafka topic jobs_raw.
[ ] Batch JSONL upload được vào HDFS raw zone.
[ ] failed_links.json được tạo khi có HTTP/parse fail.
[ ] missing_jobs.log được tạo khi thiếu critical fields.
[ ] runtime/ và data/raw/jobs/ nằm trong .gitignore.
```
