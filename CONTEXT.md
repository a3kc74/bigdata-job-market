# CONTEXT — Big Data Job Market (TopCV)

> Đọc file này để nắm toàn bộ bối cảnh dự án trước khi làm việc.
> Architecture chi tiết: `PROJECT_ARCHITECTURE.md`

---

## Mục tiêu dự án

Crawl và phân tích dữ liệu thị trường lao động từ **TopCV** (job listings đa lĩnh vực). Áp dụng **Lambda Architecture** gồm Batch Layer + Speed Layer + Serving Layer, triển khai trên **Kubernetes (Minikube)**.

---

## Tech Stack tóm tắt

| Layer | Tech |
|---|---|
| Crawler | Python (Requests, BeautifulSoup), Kafka Producer |
| Batch | PySpark, HDFS, Parquet |
| Speed | Kafka, Spark Structured Streaming |
| Serving | Elasticsearch, Kibana |
| Ops | Docker, Kubernetes (Minikube) |

---

## Mô hình dữ liệu — Medallion 4 lớp

```
Raw (JSONL) → Bronze (Parquet) → Silver (Parquet) → Gold (Parquet/Elasticsearch)
```

| Layer | HDFS Path | Mô tả |
|---|---|---|
| **Raw** | `/raw/jobs/ingest_date=YYYY-MM-DD/` | Crawler output, immutable, passthrough |
| **Bronze** | `/bronze/jobs/ingest_date=YYYY-MM-DD/` | Flatten + cast types + dedup + count metrics |
| **Silver** | `/silver/jobs/ingest_date=YYYY-MM-DD/` | Parse json_ld → `ld_*` fields; salary → VNĐ; location normalize; dedup per `job_id` |
| **Gold** | `/gold/` | Denormalized table for Serving (job_market_index) |

HDFS NameNode URL trong K8s: `hdfs://hdfs-namenode.hdfs.svc:9000`

---

## Raw Schema (source of truth: `data/raw/raw_data_format.md`)

**Root fields (Data Contract — không được tự ý sửa):**

| Field | Type | Ghi chú |
|---|---|---|
| `source` | String | Luôn là `"topcv"` |
| `source_url` | String | URL gốc |
| `normalized_source_url` | String | URL bỏ tracking params |
| `crawl_version` | Integer | Phiên bản crawler, hiện tại `1` |
| `ingest_ts` | Long (Unix ms) | Thời điểm crawler chạy |
| `event_ts` | Long (Unix ms) | Thời điểm đăng bài, nullable |
| `job_id` | String | SHA256(source + normalized_url), NOT NULL |
| `hash_content` | String | SHA256 nội dung lõi, phát hiện thay đổi |
| `payload` | Object | Toàn bộ business data (xem bên dưới) |
| `quality_flags` | Object | Map[String, Boolean] đánh giá chất lượng |

**Payload fields (chính):** `title`, `company_name`, `company_details` (Object: `scale`, `field`, `address`), `salary` (String), `location` (Array[String]), `monthOfExperience` (Integer `3` hoặc String `"Không yêu cầu"`), `deadline` (Long Unix ms), `occupationalCategory`, `education`, `employmentType`, `openings` (Integer), `description/requirements/benefits` (String, nhiều dòng phân cách bằng `\n`), `income` (Array[String]), `schedule`, `skillsNeeded` (Array[String]), `skillsShouldHave` (Array[String]), `specialty` (Array[String]), `extra_inf` (String), `meta_tags` (Map), `json_ld` (Object — nested JSON-LD), `pageText` (String)

---

## Bronze Schema (source of truth: `data/bronze/bronze_data_format.md`)

**Nguyên tắc:**
- Passthrough 100% tên field từ Raw (không đổi tên, không thêm suffix)
- `company_details` (Object) → flatten thành `company_scale`, `company_field`, `company_address`
- `monthOfExperience`: mixed type (Integer/String) → đọc như String, giữ nguyên giá trị
- `openings`: Integer → cast String
- `json_ld`: Object trong raw → trích xuất bằng `get_json_object` → JSON string tại Bronze
- `job_id`, `hash_content` giữ nguyên từ crawler, không tính lại
- Cast: `ingest_ts`, `event_ts`, `deadline` từ Long (Unix ms) → Timestamp
- `description`, `requirements`, `benefits`: giữ dạng String (nhiều dòng, `\n` phân cách)
- `skillsNeeded` + `skillsShouldHave` gộp thành `skills` (array_distinct)
- `json_ld`, `extra_inf` giữ dạng String tại Bronze — Silver mới parse
- Null String fields → empty string `""`
- Không có business canonicalization tại Bronze

**Fields thêm mới bởi Bronze ETL:**

| Field | Type | Mô tả |
|---|---|---|
| `skills` | Array[String] | Gộp từ skillsNeeded + skillsShouldHave |
| `record_version` | Integer | dense_rank theo (job_id, ingest_ts) — version tăng khi content thay đổi |
| `is_deleted` | Boolean | Mặc định false |
| `crawl_domain` | String | Parse từ source_url → `"www.topcv.vn"` |
| `*_count` | Integer | description/requirements/benefits: đếm số dòng (`split('\n')`); income/skills/specialty: `size()` |
| `ingest_date` | String | Partition column: `date_format(ingest_ts, 'yyyy-MM-dd')` |

**Dedup:** Cùng `(job_id, hash_content)` → giữ bản có `ingest_ts` mới nhất.

---

## Silver Schema (source of truth: `data/silver/silver_data_format.md`)

**Nguyên tắc:**
- Passthrough các field từ Bronze — không đổi tên, không xóa (ngoại trừ đổi `event_ts` thành `date_posted`)
- `json_ld` parse bằng `get_json_object` → các field tương ứng (đã bỏ các field bị lặp như `deadline`, `occupationalCategory`)
- Salary quy về VNĐ/tháng: primary `salary_min/max`, fallback regex `salary`
- Location: parse `location_detail` từ `location` Bronze + `has_remote`
- Không thêm field tự quy ước (enum tự định nghĩa, threshold tùy chỉnh)
- Dedup theo `job_id` — giữ `record_version` cao nhất (1 bản per job)

**Fields thêm mới bởi Silver ETL:**

| Field | Type | Nguồn | Mô tả |
|---|---|---|---|
| `company_url` | String | `$.hiringOrganization.sameAs` | Website công ty (đã unescape `\/`) |
| `company_logo` | String | `$.hiringOrganization.logo` | URL logo (đã unescape `\/`) |
| `work_country` | String | `$.jobLocation.address.addressCountry` | Mã quốc gia ISO |
| `job_location_type` | String | `$.jobLocationType` | `"TELECOMMUTE"` = remote |
| `salary_currency` | String | `$.baseSalary.currency` | `"VND"` / `"USD"` |
| `salary_min` | Double | `$.baseSalary.value.minValue` | Raw từ JSON-LD |
| `salary_max` | Double | `$.baseSalary.value.maxValue` | Raw từ JSON-LD |
| `salary_unit` | String | `$.baseSalary.value.unitText` | `"MONTH"` / `"YEAR"` |
| `job_id_platform` | String | `$.identifier.value` | TopCV internal ID (hoạt động như ID công ty) |
| `salary_min_vnd` | Long | `salary_min` → regex | Lương tối thiểu (VNĐ/tháng) |
| `salary_max_vnd` | Long | `salary_max` → regex | Lương tối đa (VNĐ/tháng) |
| `salary_is_negotiable` | Boolean | Derived | True nếu string chứa "Thỏa thuận" hoặc min/max null (baseSalary luôn tồn tại, negotiable khi thiếu min/max) |
| `location_detail` | Array[Struct] | Derived | Parse trực tiếp từ `location` của Bronze thành struct (city, address) |
| `location_count` | Integer | Derived | `size(location)` |
| `has_remote` | Boolean | `job_location_type` | True khi `= "TELECOMMUTE"` |
| `experience_required` | Boolean | Derived | False nếu `monthOfExperience` là "Thỏa thuận", mặc định True |
| `has_remote` | Boolean | `ld_job_location_type` | True khi `= "TELECOMMUTE"` |

---

## Gold Schema (Denormalized)

**Nguyên tắc:**
- Tối ưu cho Elasticsearch: Phi chuẩn hóa (Denormalized). Gộp chung thông tin của `job` và `company` vào một bản ghi duy nhất.
- Bỏ qua pre-aggregation ở Batch Layer, giao việc tính toán metric cho Kibana xử lý on-the-fly.

**Fields chính (job_market_index):**
- **Meta:** `job_id`, `company_id`, `source_url`, `date_posted`, `deadline`, `ingest_date`, `is_active`
- **Job Info:** `title`, `description`, `requirements`, `benefits`, `occupationalCategory`, `employmentType`, `education`
- **Filters/Categorical:** `has_remote`, `experience_required`, `salary_is_negotiable`, `is_weekend_free`, `schedule_type`, `skills`, `specialty`, `location`, `location_detail`
- **Metrics:** `salary_min_vnd`, `salary_max_vnd`, `salary_currency`, `salary_unit`, `monthOfExperience`, `openings`, các trường `*_count`
- **Raw/Display:** `salary`, `schedule`
- **Company Info (Embedded):** `company_name`, `company_url`, `company_logo`, `company_scale`, `company_field`, `company_address`
*(Chi tiết xem thêm tại `data/gold/gold_data_format.md`)*

---

## ETL Jobs đã implement

### `apps/batch/jobs/raw_to_bronze.py` ✅
- Đọc JSONL từ HDFS raw, output Parquet Bronze
- Chạy local: `spark-submit raw_to_bronze.py --date 2026-04-30`
- Chạy K8s: CronJob `batch-etl-raw-to-bronze` (daily 02:00 AM, namespace `spark`)
- Trigger thủ công: `kubectl create job --from=cronjob/batch-etl-raw-to-bronze manual-DATE -n spark`

### `apps/batch/jobs/bronze_to_silver.py` ✅
- Parse `json_ld` → các field tương ứng bằng `get_json_object`
- Salary → `salary_min/max_vnd` (primary json_ld, fallback regex); `salary_is_negotiable`
- Location → `location_detail`, `has_remote`
- Dedup: `job_id` giữ `record_version` max
- Chạy local: `spark-submit bronze_to_silver.py --date 2026-04-30`
### `apps/stream_etl/stream_main.py` — Spark Structured Streaming (Speed Layer)

**Kafka → Spark Structured Streaming → Elasticsearch**

- Consumes `jobs_raw` topic
- Parses raw JSON schema
- Normalizes event time, salary, location, skills
- Applies watermarking (1 hour) + deduplication by job_id
- Writes clean jobs to `jobs_clean` (Kafka) and Elasticsearch `realtime_jobs_v1`
- Computes 3 realtime aggregations:
  - Jobs per 10-minute window → `realtime_job_counts_10m_v1`
  - Top 10 skills per hour → `realtime_top_skills_hourly_v1`
  - Salary bins per hour → `realtime_salary_bins_hourly_v1`
- All outputs written to Elasticsearch only (ES-only architecture)
- Checkpoints for crash recovery
- Trigger: K8s Job `speed-stream-es-job.yaml`

---

## Kubernetes Namespaces

| Namespace | Services |
|---|---|
| `spark` | Driver/Executor Pods, CronJobs (batch + streaming) |
| `hdfs` | NameNode (port 9000), DataNode |
| `kafka` | Kafka broker (KRaft mode), Topics |
| `search` | Elasticsearch, Kibana |
| `serving` | FastAPI search endpoint |
| `airflow` | Airflow orchestration |

**Lưu ý Minikube:** Image bị mất sau khi tắt máy. Rebuild bắt buộc sau mỗi lần restart:
```bash
minikube start
eval $(minikube docker-env)   # hoặc PowerShell equivalent
docker build -f infra/spark/Dockerfile -t bigdata-job-market/spark-etl:latest .
```

---

## Cấu trúc thư mục quan trọng

```
bigdata-job-market/
├── apps/batch/jobs/raw_to_bronze.py     # ETL job ✅
├── apps/batch/jobs/bronze_to_silver.py  # ETL job ✅
├── data/raw/raw_data_format.md          # Raw schema spec
├── data/bronze/bronze_data_format.md    # Bronze schema spec
├── data/silver/silver_data_format.md    # Silver schema spec
├── data/gold/gold_data_format.md        # Gold schema spec
├── infra/spark/Dockerfile               # Spark image (base: apache/spark:4.1.1-python3)
├── infra/spark/rbac.yaml                # K8s RBAC cho Spark
├── infra/spark/raw-to-bronze-cronjob.yaml # CronJob daily raw-to-bronze ETL
├── docs/raw_to_bronze_runbook.md        # Hướng dẫn chạy ETL
├── docs/spark_on_minikube.md            # Setup + ops Spark trên Minikube
├── docs/hdfs_data_ingestion.md          # Nạp raw data vào HDFS
└── PROJECT_ARCHITECTURE.md              # Architecture diagram chi tiết
```

---

## Các quyết định kỹ thuật đã chốt

| Vấn đề | Quyết định |
|---|---|
| `json_ld` type tại Bronze | `StringType()` — giữ nguyên JSON string, Silver parse bằng `get_json_object` (không dùng `from_json` fixed schema) |
| `extra_inf` type | `StringType()` — schema không cố định |
| `deadline` type | `LongType()` (Unix ms) trong schema Spark, cast → Timestamp sau |
| Skills merge | `array_distinct(concat(coalesce(skillsNeeded,[]), coalesce(skillsShouldHave,[])))` |
| `crawl_domain` | Dùng `F.parse_url(col, lit("HOST"))` — native Spark, không dùng Python UDF |
| Partition format | `ingest_date=YYYY-MM-DD` (Hive-style) |
| Bronze write mode | `append` + partitionBy — không xóa data cũ khi chạy lại |
| Bronze dedup key | `(job_id, hash_content)` — giữ bản `ingest_ts` mới nhất |
| `record_version` | `dense_rank()` over `(job_id)` orderBy `ingest_ts` |
| Silver write mode | `overwrite` + `partitionOverwriteMode=dynamic` — chỉ replace partition đang write |
| Silver dedup key | `job_id` — giữ `record_version` max (1 bản canonical per job) |
| Silver salary source | `ld_salary_min/max` từ json_ld là primary; regex `salary` string là fallback |
| Silver tỉ giá USD | Hằng số `USD_TO_VND = 25_000` trong code, cập nhật định kỳ |
| `findspark` | Chỉ dùng khi dev local; không cần trong K8s (Dockerfile đã set `PYSPARK_PYTHON`) |

---

## Salary Prediction Contract

- Shared model code: `apps/ml/salary_prediction.py`
- Train job: `apps/batch/jobs/train_salary_model.py`
- Batch scoring: `apps/batch/jobs/silver_to_gold.py`
- Speed scoring: `apps/stream_etl/stream_main.py`

**Model family**
- Spark ML `GBTRegressor`
- Label: `log1p(mid_salary_vnd)`

**Feature groups**
- Text: `title`, `skills`
- Categorical: `company_name`, `employmentType`, `education`, `occupationalCategory`, `company_field`, `company_scale`, `primary_city`
- Numeric / Boolean: `experience_months`, `experience_required`, `has_remote`, `location_count`

**Notes**
- `company_name` vẫn là feature chính, không bucket.
- `primary_city` là 1 city canonical đại diện cho job; lấy từ `city`, `location_detail[0].city`, hoặc phần city của `location`.
- Speed layer hiện không dùng embedding; title vẫn đi theo hướng bag-of-words trong Spark ML.
- Trigger hiện tại của speed stream job: `TRIGGER_SECONDS=30`.
