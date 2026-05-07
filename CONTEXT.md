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
| Serving | Cassandra, Elasticsearch, FastAPI, Kibana, Grafana |
| Ops | Docker, Kubernetes (Minikube) |

---

## Mô hình dữ liệu — Medallion 4 lớp

```
Raw (JSONL) → Bronze (Parquet) → Silver (Parquet) → Gold (Parquet/Cassandra)
```

| Layer | HDFS Path | Mô tả |
|---|---|---|
| **Raw** | `/raw/jobs/ingest_date=YYYY-MM-DD/` | Crawler output, immutable, passthrough |
| **Bronze** | `/bronze/jobs/ingest_date=YYYY-MM-DD/` | Flatten + cast types + dedup + count metrics |
| **Silver** | `/silver/jobs/ingest_date=YYYY-MM-DD/` | Parse json_ld → `ld_*` fields; salary → VNĐ; location normalize; dedup per `job_id` |
| **Gold** | `/gold/` | Aggregated analytics tables (TODO) |

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
- Passthrough 100% tên field từ Bronze — không đổi tên, không xóa
- `json_ld` parse bằng `get_json_object` → 14 field `ld_*`
- Salary quy về VNĐ/tháng: primary `ld_salary_min/max`, fallback regex `salary`
- Location chuẩn hóa chính tả: `location_normalized` + `has_remote`
- Không thêm field tự quy ước (enum tự định nghĩa, threshold tùy chỉnh)
- Dedup theo `job_id` — giữ `record_version` cao nhất (1 bản per job)

**Fields thêm mới bởi Silver ETL:**

| Field | Type | Nguồn | Mô tả |
|---|---|---|---|
| `ld_deadline` | Timestamp | `$.validThrough` | Hạn tuyển từ JSON-LD |
| `ld_company_url` | String | `$.hiringOrganization.sameAs` | Website công ty |
| `ld_company_logo` | String | `$.hiringOrganization.logo` | URL logo |
| `ld_work_locality` | String | `$.jobLocation.address.addressLocality` | Quận/huyện nơi làm việc |
| `ld_work_region` | String | `$.jobLocation.address.addressRegion` | Tỉnh/thành nơi làm việc |
| `ld_work_country` | String | `$.jobLocation.address.addressCountry` | Mã quốc gia ISO |
| `ld_job_location_type` | String | `$.jobLocationType` | `"TELECOMMUTE"` = remote |
| `ld_salary_currency` | String | `$.baseSalary.currency` | `"VND"` / `"USD"` |
| `ld_salary_min` | Double | `$.baseSalary.value.minValue` | Raw từ JSON-LD |
| `ld_salary_max` | Double | `$.baseSalary.value.maxValue` | Raw từ JSON-LD |
| `ld_salary_unit` | String | `$.baseSalary.value.unitText` | `"MONTH"` / `"YEAR"` |
| `ld_experience_months` | Integer | `$.experienceRequirements.monthsOfExperience` | Fallback: regex `monthOfExperience` |
| `ld_job_id_platform` | String | `$.identifier.value` | TopCV internal ID |
| `ld_occupational_category` | String | `$.occupationalCategory` | Ngành nghề theo JSON-LD |
| `salary_min_vnd` | Long | `ld_salary_min` → regex | Lương tối thiểu (VNĐ/tháng) |
| `salary_max_vnd` | Long | `ld_salary_max` → regex | Lương tối đa (VNĐ/tháng) |
| `salary_is_negotiable` | Boolean | `salary` string | True nếu chứa "Thỏa thuận" |
| `location_normalized` | Array[String] | `location` | Tên tỉnh/thành chuẩn hóa chính tả |
| `location_count` | Integer | Derived | `size(location_normalized)` |
| `has_remote` | Boolean | `ld_job_location_type` | True khi `= "TELECOMMUTE"` |

---

## ETL Jobs đã implement

### `apps/batch/jobs/raw_to_bronze.py` ✅
- Đọc JSONL từ HDFS raw, output Parquet Bronze
- Chạy local: `spark-submit raw_to_bronze.py --date 2026-04-30`
- Chạy K8s: CronJob `batch-etl-raw-to-bronze` (daily 02:00 AM, namespace `spark`)
- Trigger thủ công: `kubectl create job --from=cronjob/batch-etl-raw-to-bronze manual-DATE -n spark`

### `apps/batch/jobs/bronze_to_silver.py` ✅
- Parse `json_ld` → 14 field `ld_*` bằng `get_json_object`
- Salary → `salary_min/max_vnd` (primary json_ld, fallback regex); `salary_is_negotiable`
- Location → `location_normalized`, `has_remote`
- Dedup: `job_id` giữ `record_version` max
- Chạy local: `spark-submit bronze_to_silver.py --date 2026-04-30`
- Trigger K8s: CronJob `batch-etl-bronze-to-silver` (TODO: tạo CronJob)

### `apps/batch/jobs/silver_to_gold.py` ❌ TODO
### `apps/spark/kafka_to_cassandra_es.py` — Structured Streaming (Speed Layer)

---

## Kubernetes Namespaces

| Namespace | Services |
|---|---|
| `spark` | Driver/Executor Pods, CronJobs |
| `hdfs` | NameNode (port 9000), DataNode |
| `kafka` | Broker, Zookeeper |
| `cassandra` | StatefulSet |
| `elastic` | Elasticsearch, Kibana |

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
├── infra/spark/Dockerfile               # Spark image (base: apache/spark:4.1.1-python3)
├── infra/spark/10-rbac.yaml            # K8s RBAC cho Spark
├── infra/kubernetes/batch-etl-cronjob.yaml  # CronJob daily ETL
├── docs/raw_to_bronze_runbook.md        # Hướng dẫn chạy ETL
├── docs/spark_on_minikube.md            # Setup + ops Spark trên Minikube
├── docs/hdfs_data_ingestion.md          # Nạp raw data vào HDFS
└── PROJECT_ARCHITECTURE.md             # Architecture diagram chi tiết
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
