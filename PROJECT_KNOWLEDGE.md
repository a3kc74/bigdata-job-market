# PROJECT KNOWLEDGE — Big Data Job Market (TopCV)

> Tài liệu tổng hợp toàn bộ thông tin về project để AI có thể trả lời mọi câu hỏi liên quan.
> Cập nhật lần cuối: 2026-06-06

---

## 1. TỔNG QUAN DỰ ÁN

### Mục tiêu
Crawl và phân tích dữ liệu thị trường lao động từ **TopCV** (website tuyển dụng lớn tại Việt Nam). Áp dụng **Lambda Architecture** gồm:
- **Batch Layer**: xử lý dữ liệu lịch sử đầy đủ
- **Speed Layer**: xử lý dữ liệu gần realtime
- **Serving Layer**: cung cấp dữ liệu cho dashboard & search

Toàn bộ hệ thống triển khai trên **Kubernetes (Minikube)** cho môi trường dev local.

### Tech Stack

| Layer | Technology |
|---|---|
| Crawler | Python (Requests, BeautifulSoup), JSONL output |
| Ingestion (Speed) | Kafka Producer (đọc JSONL → Kafka) |
| Batch Layer | PySpark, HDFS, Parquet (Snappy) |
| Speed Layer | Kafka (KRaft mode), Spark Structured Streaming |
| ML | Spark ML GBTRegressor (salary prediction) |
| Serving Layer | Elasticsearch, Kibana, FastAPI |
| Orchestration | Apache Airflow (DAG trên Kubernetes) |
| Platform/Ops | Docker, Kubernetes (Minikube) |

---

## 2. KIẾN TRÚC HỆ THỐNG

### Sơ đồ tổng thể

```
DATA SOURCES
├── Historical JSON Files (batch input)
└── TopCV Crawler (realtime + batch)
         │
         ├─────────────────────────────────────────────┐
         ▼                                             ▼
   BATCH LAYER                                  SPEED LAYER
   ┌──────────────────────┐            ┌─────────────────────────┐
   │  HDFS /raw/jobs       │            │  Crawler (speed mode)   │
   │         ↓             │            │          ↓              │
   │  Spark raw_to_bronze  │            │  JSONL Producer         │
   │         ↓             │            │          ↓              │
   │  HDFS /bronze/jobs    │            │  Kafka (jobs_raw)       │
   │         ↓             │            │          ↓              │
   │  Spark bronze_to_     │            │  Spark Structured       │
   │  silver               │            │  Streaming              │
   │         ↓             │            │          ↓              │
   │  Spark train ML model │            │  Load Salary Model      │
   │         ↓             │            │          ↓              │
   │  Spark silver_to_gold │            │  Write Elasticsearch    │
   │         ↓             │            └─────────────────────────┘
   │  gold_to_elasticsearch│
   └──────────────────────┘
                   │                              │
                   └──────────────────────────────┘
                                 ▼
                         SERVING LAYER
                    Elasticsearch + Kibana
                    FastAPI Search Endpoint
```

### Lambda Architecture — vai trò từng layer

- **Batch Layer**: Xử lý toàn bộ lịch sử, đảm bảo chính xác, output chuẩn → `gold-jobs-flat` ES index
- **Speed Layer**: Xử lý dữ liệu mới gần realtime, output → `realtime_jobs_v1` + các aggregate ES indexes
- **Serving Layer**: Elasticsearch merge dữ liệu từ cả batch và speed. Kibana dashboard, FastAPI search

---

## 3. DATA MODEL — MEDALLION 4 LỚP

```
Raw (JSONL) → Bronze (Parquet) → Silver (Parquet) → Gold (Parquet + Elasticsearch)
```

### Tổng quan các lớp

| Layer | Format | HDFS Path | Mô tả |
|---|---|---|---|
| **Raw** | JSONL | `/raw/jobs/ingest_date=YYYY-MM-DD/` | Crawler output, immutable, passthrough |
| **Bronze** | Parquet (Snappy) | `/bronze/jobs/ingest_date=YYYY-MM-DD/` | Flatten + cast types + dedup + count metrics |
| **Silver** | Parquet (Snappy) | `/silver/jobs/ingest_date=YYYY-MM-DD/` | json_ld parse + salary VNĐ + location normalize + dedup per job_id |
| **Gold** | Parquet + Elasticsearch | `/gold/jobs/job_market_index/ingest_date=YYYY-MM-DD/` | Denormalized, ML enriched, serving-ready |

**HDFS NameNode URL trong K8s:** `hdfs://hdfs-namenode.hdfs.svc:9000`

---

## 4. RAW SCHEMA (Data Contract v1.0)

**Định dạng:** JSONL (mỗi dòng 1 JSON object độc lập)

### Root fields (Data Contract — KHÔNG tự ý sửa)

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `source` | String | Có | Luôn là `"topcv"` |
| `source_url` | String | Có | URL gốc của job |
| `normalized_source_url` | String | Có | URL đã bỏ tracking params (`?ta_source=...`) |
| `crawl_version` | Integer | Có | Phiên bản crawler, hiện tại `1` |
| `ingest_ts` | Long (Unix ms) | Có | Thời điểm crawler chạy thành công |
| `event_ts` | Long (Unix ms) | Không | Thời điểm đăng bài từ JSON-LD, nullable |
| `job_id` | String | Có | SHA256(source + normalized_url), NOT NULL |
| `hash_content` | String | Có | SHA256 nội dung lõi, dùng phát hiện thay đổi |
| `payload` | Object | Có | Toàn bộ business data |
| `quality_flags` | Object | Có | Map[String, Boolean] đánh giá chất lượng |

### Payload fields chính

| Field | Type | Mô tả |
|---|---|---|
| `title` | String | Tiêu đề job |
| `company_name` | String | Tên công ty |
| `company_details` | Object | `{ scale, field, address }` |
| `salary` | String | Chuỗi lương gốc (VD: `"10 - 15 Triệu"`, `"Thỏa thuận"`) |
| `location` | Array[String] | Danh sách tỉnh/thành phố |
| `monthOfExperience` | Integer hoặc String | Số tháng kinh nghiệm hoặc `"Không yêu cầu"`, nullable |
| `deadline` | Long (Unix ms) | Hạn nộp hồ sơ |
| `occupationalCategory` | String | Cấp bậc (VD: `"Nhân viên"`, `"Trưởng phòng"`) |
| `education` | String | Yêu cầu học vấn (VD: `"Đại Học trở lên"`) |
| `employmentType` | String | Hình thức làm việc (VD: `"FULL_TIME"`) |
| `openings` | Integer | Số lượng tuyển dụng, nullable |
| `description` | String | Mô tả job (multi-line, `\n` phân cách) |
| `requirements` | String | Yêu cầu ứng viên (multi-line, `\n` phân cách) |
| `benefits` | String | Quyền lợi (multi-line, `\n` phân cách) |
| `income` | Array[String] | Phụ cấp/thu nhập thêm |
| `schedule` | String | Thời gian làm việc |
| `skillsNeeded` | Array[String] | Kỹ năng BẮT BUỘC |
| `skillsShouldHave` | Array[String] | Kỹ năng NÊN CÓ |
| `specialty` | Array[String] | Chuyên môn job |
| `extra_inf` | String/Object | Form tùy chỉnh |
| `meta_tags` | Object | Meta tags trang web |
| `json_ld` | Object | Toàn bộ khối JSON-LD (schema.org JobPosting) |
| `pageText` | String | Toàn bộ văn bản thô của trang |

### Quality flags

| Flag | Ý nghĩa khi `true` |
|---|---|
| `has_json_ld` | Trang có JSON-LD schema |
| `has_page_text` | Có nội dung text |
| `has_structured_company_name_conflict` | Tên công ty JSON-LD khác HTML |
| `has_valid_posting_date` | `event_ts` parse thành công |
| `has_valid_deadline` | `deadline` parse thành công |
| `has_salary_info` | Có thông tin lương |
| `has_location_info` | Có thông tin địa điểm |
| `has_experience_info` | Có thông tin kinh nghiệm |
| `has_requirements` | Có `requirements` |
| `has_description` | Có `description` |
| `has_benefits` | Có `benefits` |
| `has_skills_info` | Có kỹ năng |
| `has_education_info` | Có thông tin học vấn |
| `has_specialty` | Có chuyên môn |
| `has_schedule` | Có thông tin lịch làm việc |
| `has_employment_type` | Có hình thức làm việc |
| `has_income` | Có thu nhập phụ |
| `has_extra_info` | Có extra form data |

---

## 5. BRONZE SCHEMA (v1.0)

**Nguồn vào:** Raw JSONL  
**Output:** Parquet (Snappy)  
**Partition key:** `ingest_date`  
**Dedup key:** `(job_id, hash_content)` — giữ `ingest_ts` mới nhất

### Nguyên tắc
- Passthrough 100% tên field từ Raw (không đổi tên, không thêm suffix)
- `company_details` (Object) → flatten thành `company_scale`, `company_field`, `company_address`
- `monthOfExperience`: mixed type (Integer/String) → đọc như **String**, giữ nguyên giá trị
- `openings`: Integer → cast **String**
- `json_ld`: Object trong raw → trích xuất bằng `get_json_object` → **JSON string** tại Bronze
- `job_id`, `hash_content` passthrough từ crawler, **không tính lại**
- Cast: `ingest_ts`, `event_ts`, `deadline` từ Long (Unix ms) → **Timestamp**
- `skillsNeeded` + `skillsShouldHave` → gộp thành `skills` (array_distinct)
- Null String fields → empty string `""`
- **Không có business canonicalization tại Bronze**

### Fields passthrough từ Root

| Field | Type (Bronze) | Từ Raw |
|---|---|---|
| `source` | String | Passthrough |
| `source_url` | String | Passthrough |
| `normalized_source_url` | String | Passthrough |
| `crawl_version` | Integer | Passthrough |
| `ingest_ts` | Timestamp | Cast từ Long ms |
| `event_ts` | Timestamp | Cast từ Long ms, nullable |
| `job_id` | String | Passthrough |
| `hash_content` | String | Passthrough |

### Fields từ payload (flatten)

| Field | Type | Ghi chú |
|---|---|---|
| `title` | String | |
| `company_name` | String | |
| `company_scale` | String | Flatten từ `company_details.scale` |
| `company_field` | String | Flatten từ `company_details.field` |
| `company_address` | String | Flatten từ `company_details.address` |
| `salary` | String | |
| `location` | Array\<String\> | |
| `monthOfExperience` | String | Mixed type → đọc như String |
| `deadline` | Timestamp | Cast từ Unix ms |
| `occupationalCategory` | String | |
| `education` | String | |
| `employmentType` | String | Enum audit only tại Bronze |
| `openings` | String | Cast từ Integer |
| `description` | String | Multi-line, `\n` phân cách |
| `requirements` | String | Multi-line, `\n` phân cách |
| `benefits` | String | Multi-line, `\n` phân cách |
| `income` | Array\<String\> | |
| `schedule` | String | |
| `skills` | Array\<String\> | `array_union(skillsNeeded, skillsShouldHave)` |
| `specialty` | Array\<String\> | |
| `extra_inf` | String | Raw string |
| `meta_tags` | Map\<String, String\> | |
| `json_ld` | String | JSON string (dùng `get_json_object`) |
| `pageText` | String | |
| `quality_flags` | Map\<String, Boolean\> | Passthrough |

### Fields thêm mới bởi Bronze ETL

| Field | Type | Mô tả |
|---|---|---|
| `record_version` | Integer | `dense_rank()` over `(job_id)` orderBy `ingest_ts` |
| `is_deleted` | Boolean | Mặc định `false` |
| `crawl_domain` | String | Parse từ `source_url` bằng `parse_url(col, 'HOST')` → `"www.topcv.vn"` |
| `description_count` | Integer | `size(split(description, '\n'))` |
| `requirements_count` | Integer | `size(split(requirements, '\n'))` |
| `benefits_count` | Integer | `size(split(benefits, '\n'))` |
| `income_count` | Integer | `size(income)` |
| `skills_count` | Integer | `size(skills)` |
| `specialty_count` | Integer | `size(specialty)` |
| `ingest_date` | String | `date_format(ingest_ts, 'yyyy-MM-dd')` — partition column |

### Deduplication tại Bronze
- Cùng `(job_id, hash_content)` → giữ bản có `ingest_ts` mới nhất
- Khác `hash_content` với cùng `job_id` → tăng `record_version`

---

## 6. SILVER SCHEMA (v1.0)

**Nguồn vào:** Bronze Parquet  
**Output:** Parquet (Snappy)  
**Partition key:** `ingest_date`  
**Dedup key:** `job_id` — giữ `record_version` cao nhất (1 bản per job)  
**Write mode:** `overwrite` + `partitionOverwriteMode=dynamic`

### Nguyên tắc
- Passthrough **tất cả** fields từ Bronze (không đổi tên, không xóa)
- Ngoại trừ: `event_ts` → đổi tên thành `date_posted`
- Thêm các field mới từ `json_ld` parsing bằng `get_json_object` (không dùng `from_json` fixed schema)
- Salary → VNĐ/tháng: primary từ json_ld, fallback regex trên `salary` string
- Location → `location_detail` (struct array), `has_remote`
- Không thêm field tự quy ước (enum tự định nghĩa, threshold tùy chỉnh)
- Chỉ dùng **native Spark functions**, không dùng Python UDF
- Null convention: derived fields dùng `null` cho "không có thông tin"

### Fields thêm mới bởi Silver ETL

#### A. Từ `json_ld` parsing

| Field | Type | JSON-LD path | Ghi chú |
|---|---|---|---|
| `company_url` | String | `$.hiringOrganization.sameAs` | Website công ty (đã unescape `\/`) |
| `company_logo` | String | `$.hiringOrganization.logo` | URL logo (đã unescape `\/`) |
| `work_country` | String | `$.jobLocation.address.addressCountry` | Mã quốc gia ISO (thường `"VN"`) |
| `job_location_type` | String | `$.jobLocationType` | `"TELECOMMUTE"` = remote |
| `salary_currency` | String | `$.baseSalary.currency` | `"VND"` hoặc `"USD"` |
| `salary_min` | Double | `$.baseSalary.value.minValue` | Raw từ JSON-LD, null nếu vắng mặt |
| `salary_max` | Double | `$.baseSalary.value.maxValue` | Raw từ JSON-LD, null nếu vắng mặt |
| `salary_unit` | String | `$.baseSalary.value.unitText` | `"MONTH"`, `"YEAR"`, `"HOUR"` |
| `job_id_platform` | String | `$.identifier.value` | TopCV internal ID (hoạt động như company ID) |

#### B. Salary canonical

| Field | Type | Mô tả |
|---|---|---|
| `salary_min_vnd` | Long | Lương tối thiểu (VNĐ/tháng), null nếu không xác định |
| `salary_max_vnd` | Long | Lương tối đa (VNĐ/tháng), null nếu không xác định |
| `salary_is_negotiable` | Boolean | `true` nếu `salary` chứa "Thỏa thuận" hoặc cả min/max đều null |

**Tỉ giá:** `USD_TO_VND = 25_000` (hằng số trong code)  
**Primary source:** `salary_min/max` từ json_ld  
**Fallback:** regex trên `salary` string

#### C. Location canonical

| Field | Type | Mô tả |
|---|---|---|
| `location_count` | Integer | `size(location)` |
| `location_detail` | Array\<Struct\<city: String, address: String\>\> | Tách tại `:` đầu tiên — trước là `city`, sau là `address` |
| `has_remote` | Boolean | `true` khi `job_location_type = "TELECOMMUTE"` |

#### D. Experience

| Field | Type | Mô tả |
|---|---|---|
| `experience_required` | Boolean | `false` nếu `monthOfExperience` là "Thỏa thuận", mặc định `true` |

### Deduplication tại Silver
- Input: nhiều record per `job_id`, nhiều `record_version`
- Output: 1 record per `job_id`, giữ `record_version` cao nhất

---

## 7. GOLD SCHEMA (v1.0) — `job_market_index`

**Nguồn vào:** Silver Parquet  
**Output:** Parquet (HDFS) + Elasticsearch index `job_market_index`  
**HDFS path:** `/gold/jobs/job_market_index/ingest_date=YYYY-MM-DD/`  
**ES Index batch:** `gold-jobs-flat`

### Nguyên tắc
- Denormalized: gộp job + company info vào 1 record
- Bỏ các cột ETL nội bộ: `json_ld`, `pageText`, `hash_content`, `record_version`, `quality_flags`, `description_count`, `income_count`
- Tối ưu cho Elasticsearch search/filter/aggregation

### Cấu trúc bảng `job_market_index`

#### A. Meta & Dates

| Field | Type | ES type | Mô tả |
|---|---|---|---|
| `job_id` | String | Keyword | Primary Key |
| `company_id` | String | Keyword | Rename từ `job_id_platform` |
| `source_url` | String | Keyword | URL gốc trên TopCV |
| `date_posted` | Timestamp | Date | Ngày đăng (từ `event_ts`) |
| `deadline` | Timestamp | Date | Hạn nộp hồ sơ |
| `ingest_date` | String | Keyword | Ngày crawl (partition key) |
| `is_active` | Boolean | Boolean | Mặc định `true` hoặc tính từ `deadline` |

#### B. Full-Text Search

| Field | Type | ES type |
|---|---|---|
| `title` | String | Text |
| `company_name` | String | Text |
| `description` | String | Text |
| `requirements` | String | Text |
| `benefits` | String | Text |
| `company_scale` | String | Text |

#### C. Categorical & Filters

| Field | Type | ES type |
|---|---|---|
| `company_field` | String | Keyword |
| `work_country` | String | Keyword |
| `occupationalCategory` | String | Keyword |
| `employmentType` | String | Keyword |
| `education` | String | Keyword |
| `salary_currency` | String | Keyword |
| `salary_unit` | String | Keyword |
| `skills` | Array[String] | Keyword |
| `specialty` | Array[String] | Keyword |
| `location` | Array[String] | Text/Keyword |
| `location_detail` | Array[Struct] | Nested/Keyword |

#### D. Boolean Filters

| Field | Mô tả |
|---|---|
| `has_remote` | Remote không |
| `experience_required` | Có yêu cầu kinh nghiệm |
| `salary_is_negotiable` | Lương thỏa thuận không |
| `is_weekend_free` | Nghỉ cuối tuần không |

#### E. Schedule

| Field | Type | Mô tả |
|---|---|---|
| `schedule_type` | String | `"T2-T6"`, `"T2-T7"`, `"T2-CN"`, `"Flexible"`, `"Other"` |
| `schedule` | String | Text gốc để hiển thị |

#### F. Numerical Metrics

| Field | Type | Mô tả |
|---|---|---|
| `salary_min_vnd` | Long | Lương min thật (VNĐ/tháng) |
| `salary_max_vnd` | Long | Lương max thật (VNĐ/tháng) |
| `monthOfExperience` | Integer | Tháng kinh nghiệm |
| `openings` | Integer | Số vị trí cần tuyển |
| `benefits_count` | Integer | |
| `requirements_count` | Integer | |
| `location_count` | Integer | |
| `skills_count` | Integer | |
| `specialty_count` | Integer | |

#### G. Raw Display

| Field | Mô tả |
|---|---|
| `salary` | Chuỗi lương gốc (không index) |
| `company_logo` | URL logo (không index) |
| `company_address` | Địa chỉ trụ sở (không index) |
| `company_url` | Website công ty (không index) |

### Salary Prediction Fields (thêm bởi Spark ML)

| Field | Type | Mô tả |
|---|---|---|
| `salary_prediction_applied` | Boolean | `true` nếu job không có min/max thật và model đã predict |
| `salary_display_min_vnd` | Long | Lương min cho UI/API (thật nếu có, predict nếu không) |
| `salary_display_avg_vnd` | Long | Lương avg cho sort/chart |
| `salary_display_max_vnd` | Long | Lương max cho UI/API |
| `salary_source` | String | `parsed_range` / `parsed_min_only` / `parsed_max_only` / `predicted` / `unknown` |
| `salary_predicted_min_vnd` | Long | Cận dưới predict (90% của avg predict) |
| `salary_predicted_avg_vnd` | Long | Avg do model predict |
| `salary_predicted_max_vnd` | Long | Cận trên predict (110% của avg predict) |
| `salary_prediction_model_version` | String | Version model đã dùng (`spark_ml_salary_gbt_v1`) |

---

## 8. ETL JOBS — BATCH LAYER

### `apps/batch/jobs/raw_to_bronze.py` ✅
- Đọc JSONL từ HDFS `/raw/jobs/`, output Parquet Bronze
- **Chạy local:** `spark-submit raw_to_bronze.py --date 2026-04-30`
- **Chạy K8s:** CronJob `batch-etl-raw-to-bronze` (daily 02:00 AM, namespace `spark`)
- **Trigger thủ công:** `kubectl create job --from=cronjob/batch-etl-raw-to-bronze manual-DATE -n spark`
- **Write mode:** `append` + `partitionBy('ingest_date')`

### `apps/batch/jobs/bronze_to_silver.py` ✅
- Parse `json_ld` → các field tương ứng bằng `get_json_object`
- Salary → `salary_min/max_vnd` (primary json_ld, fallback regex)
- Location → `location_detail`, `has_remote`
- Dedup: `job_id` giữ `record_version` max
- **Chạy local:** `spark-submit bronze_to_silver.py --date 2026-04-30`
- **Write mode:** `overwrite` + `partitionOverwriteMode=dynamic`

### `apps/batch/jobs/train_salary_model.py` ✅
- Train Spark ML `GBTRegressor` từ **toàn bộ Silver** (không theo --date)
- Ghi model vào `hdfs://hdfs-namenode.hdfs.svc:9000/models/salary_prediction/latest`
- Ghi metrics vào `...metrics/latest`
- Mỗi lần batch có dữ liệu mới → train lại + overwrite model cũ

### `apps/batch/jobs/silver_to_gold.py` ✅
- Load model từ HDFS, apply salary prediction
- Parse `schedule` → `schedule_type` + `is_weekend_free`
- Đổi tên `job_id_platform` → `company_id`
- Chọn cột Gold, ghi HDFS Parquet

### `apps/batch/jobs/gold_to_elasticsearch.py` ✅
- Đọc Gold Parquet, ghi vào ES index `gold-jobs-flat`

---

## 9. SPEED LAYER — SPARK STRUCTURED STREAMING

### File chính: `apps/stream_etl/stream_main.py`

**Luồng:**
```
Kafka (jobs_raw)
  → parse raw JSON schema
  → normalize event time, salary, location, skills
  → watermarking (1 hour) + dedup by job_id
  → write to jobs_clean (Kafka)
  → write to realtime_jobs_v1 (Elasticsearch)
  → 3 realtime aggregations → Elasticsearch
```

### Kafka Topics

| Topic | Mục đích | Key |
|---|---|---|
| `jobs_raw` | Raw job events từ crawler | `job_id` |
| `jobs_clean` | Validated, normalized jobs | `job_id` |
| `jobs_dead_letter` | Malformed / invalid records | original key |

### Realtime Aggregations

| Aggregation | Window | ES Index |
|---|---|---|
| Jobs per 10 minutes (by source, city, company_field) | 10 phút | `realtime_job_counts_10m_v1` |
| Top 10 skills per hour | 1 giờ | `realtime_top_skills_hourly_v1` |
| Skill counts per hour | 1 giờ | `realtime_skill_counts_hourly_v1` |
| Salary bins per hour (by city, category) | 1 giờ | `realtime_salary_bins_hourly_v1` |

### Elasticsearch Indexes — Speed Layer

| Index | Loại data | Dùng để |
|---|---|---|
| `realtime_jobs_v1` | Job-level (1 doc = 1 job) | Search, filter, hiển thị job realtime |
| `realtime_job_counts_10m_v1` | Aggregate (1 doc = 1 window 10 phút) | Chart số job theo thời gian |
| `realtime_skill_counts_hourly_v1` | Aggregate (1 doc = 1 skill/giờ) | Skill trending |
| `realtime_top_skills_hourly_v1` | Aggregate (1 doc = top skill ranked) | Top skill theo giờ |
| `realtime_salary_bins_hourly_v1` | Aggregate (1 doc = salary bin/giờ) | Phân bố lương realtime |

### Fault Tolerance

| Cơ chế | Mô tả |
|---|---|
| Checkpointing | Per-query checkpoint dirs (PVC trên K8s) |
| Dead-letter topic | Record lỗi → `jobs_dead_letter` |
| Watermark | 1 giờ late-data tolerance trên `event_ts` |
| Dedup | `dropDuplicates(["job_id"])` trong watermark window |
| Idempotent sinks | Deterministic document IDs cho ES upserts |

### Speed Layer Trigger
- `TRIGGER_SECONDS=30` (mặc định)
- Producer poll JSONL mỗi 2 giây
- Crawler ghi JSONL dần, producer đẩy Kafka dần → stream xử lý dần

---

## 10. CRAWLER

### Các file crawler chính

| File | Mục đích |
|---|---|
| `apps/ingestion/topcv_crawler.py` | Logic crawl thật: quét list page, parse detail, ghi JSONL |
| `apps/ingestion/run_crawler.py` | CLI wrapper — chọn mode `speed` hoặc `batch` |
| `apps/ingestion/batch_crawler.py` | Wrapper batch cho HDFS ingestion |
| `apps/producer/crawler_jsonl_producer.py` | Watch JSONL file → publish Kafka `jobs_raw` |
| `apps/producer/kafka_job_producer.py` | Live crawl → Kafka trực tiếp |

### Speed mode vs Batch mode

| Tiêu chí | Speed mode | Batch mode |
|---|---|---|
| Lệnh | `--mode speed` | `--mode batch` |
| Phạm vi | Chỉ các job mới trong vài chục phút gần nhất | Job trong vài ngày gần nhất |
| Max pages | 15 page đầu | Toàn bộ |
| Dedup cache | Có (`speed_processed_jobs_29d.json`, TTL 29 ngày) | Không |
| Mục tiêu | Nhanh, gần realtime | Đầy đủ, chính xác |

### Speed mode config

| Biến | Giá trị mặc định | Ý nghĩa |
|---|---|---|
| `SPEED_CRAWL_MAX_PAGES` | `15` | Tối đa 15 page đầu |
| `SPEED_UPDATED_WITHIN_MINUTES` | `30` | Chỉ lấy job update trong 30 phút gần nhất |
| `SPEED_LIST_PAGES_PER_CHUNK` | `5` | Quét 5 page rồi xử lý detail, rồi quét tiếp |
| `SPEED_DETAIL_BATCH_SIZE` | `40` | Mỗi đợt xử lý tối đa 40 detail page |

### Speed processed cache
- File: `runtime/crawler/speed_processed_jobs_29d.json`
- TTL: 29 ngày
- Bỏ qua job nếu: `job_id` đã có trong cache VÀ `listing_updated_time` không mới hơn lần xử lý trước
- Vẫn crawl lại nếu: `listing_updated_time` mới hơn (nội dung có thể đã thay đổi)

### Crawler timing
- Sleep giữa các list page: 1.5–3.0 giây
- Sleep giữa các detail page: 1.5–3.5 giây
- Cooldown sau mỗi 40 request: 20–30 giây
- 1 job detail ≈ 2.5–5 giây (bình thường)

---

## 11. SPARK ML — SALARY PREDICTION

### Mục tiêu nghiệp vụ

Dự đoán lương cho các job ghi `"Thỏa thuận"` hoặc chỉ có 1 cận lương, để dashboard có thể sort/filter/visualize.

### Nguyên tắc cốt lõi

```
CHỈ predict khi:
  salary_min_vnd IS NULL
  AND salary_max_vnd IS NULL
  AND model load thành công
```

| Dữ liệu crawl | Có predict không |
|---|---|
| `10 - 60 triệu` (có range thật) | Không |
| `Tới 40 triệu` (max only) | Không |
| `Lớn hơn 40 triệu` (min only) | Không |
| `Thỏa thuận` nhưng JSON-LD có min/max | Không |
| `Thỏa thuận` nhưng chỉ có min | Không |
| `Thỏa thuận` và min/max đều null | **Có** |

> `salary_is_negotiable` chỉ là cờ mô tả text nguồn, KHÔNG được dùng để ép model predict khi min/max đã có số.

### Model

| Tham số | Giá trị |
|---|---|
| Algorithm | Spark ML `GBTRegressor` |
| Label | `log1p(mid_salary_vnd)` |
| Model version | `spark_ml_salary_gbt_v1` |
| maxIter | 60 |
| maxDepth | 5 |
| maxBins | 64 |
| minInstancesPerNode | 5 |
| stepSize | 0.05 |
| subsamplingRate | 0.8 |
| seed | 42 |

### Feature Set

| Feature | Nguồn batch | Nguồn speed |
|---|---|---|
| `title` (RegexTokenizer → CountVectorizer) | Silver `title` | Clean stream `title` |
| `skills` (CountVectorizer) | Silver `skills` | Clean stream `skills` |
| `company_name` (StringIndexer → OHE) | Silver `company_name` | Clean stream `company_name` |
| `employmentType` (StringIndexer → OHE) | Silver `employmentType` | Clean stream |
| `education` (StringIndexer → OHE) | Silver `education` | Clean stream |
| `occupationalCategory` (StringIndexer → OHE) | Silver | Clean stream |
| `company_field` (StringIndexer → OHE) | Silver | Clean stream |
| `company_scale` (StringIndexer → OHE) | Silver | Clean stream |
| `primary_city` (StringIndexer → OHE) | Derived | Clean stream |
| `experience_months` (numeric) | Parse `monthOfExperience` | Clean stream |
| `experience_required` (boolean) | Silver | Clean stream |
| `has_remote` (boolean) | Silver | Clean stream |
| `location_count` (numeric) | Silver | Clean stream |

### Serving rules

| Trường hợp | salary_source | salary_display_* |
|---|---|---|
| Cả min và max thật | `parsed_range` | Lương thật |
| Chỉ min thật | `parsed_min_only` | min thật + predict max |
| Chỉ max thật | `parsed_max_only` | max thật + predict min |
| Không có min/max thật, model OK | `predicted` | Từ model |
| Không có min/max thật, model lỗi | `unknown` | null |

### Model paths (HDFS)
- Model: `hdfs://hdfs-namenode.hdfs.svc:9000/models/salary_prediction/latest`
- Metrics: `hdfs://hdfs-namenode.hdfs.svc:9000/models/salary_prediction/metrics/latest`

---

## 12. KUBERNETES INFRASTRUCTURE

### Namespaces

| Namespace | Services |
|---|---|
| `spark` | Spark Driver/Executor Pods, CronJobs |
| `hdfs` | HDFS NameNode (port 9000), DataNode |
| `kafka` | Kafka broker (KRaft mode), Topics |
| `search` | Elasticsearch, Kibana |
| `serving` | FastAPI search endpoint |
| `airflow` | Airflow Webserver, Scheduler, Postgres |

### Kubernetes CronJobs (namespace `spark`)

| CronJob | Trigger | Job |
|---|---|---|
| `batch-etl-crawl-jobs` | Airflow | Batch crawler |
| `batch-etl-raw-to-bronze` | Daily 02:00 / Airflow | raw_to_bronze.py |
| `batch-etl-bronze-to-silver` | Airflow | bronze_to_silver.py |
| `batch-etl-train-salary-model` | Airflow | train_salary_model.py |
| `batch-etl-silver-to-gold` | Airflow | silver_to_gold.py |
| `batch-etl-gold-to-elasticsearch` | Airflow | gold_to_elasticsearch.py |

> Các CronJob để `suspend: true` vì Airflow là scheduler chính.

### K8s Infrastructure Files

```
infra/
├── namespaces/all.yaml           # Tạo tất cả namespaces
├── hdfs/hdfs.yaml                # HDFS NameNode + DataNode
├── kafka/kafka-cluster.yaml      # Kafka KRaft mode
├── kafka/jobs-topics.yaml        # Kafka topics
├── search/elasticsearch-statefulset.yaml
├── search/elasticsearch-service.yaml
├── search/kibana-deployment.yaml
├── search/kibana-service.yaml
├── serving/job-search-api-deployment.yaml
├── serving/job-search-api-service.yaml
├── spark/Dockerfile              # Spark image (base: apache/spark:4.1.1-python3)
├── spark/rbac.yaml               # RBAC cho Spark
├── spark/raw-to-bronze-cronjob.yaml
├── spark/bronze-to-silver-cronjob.yaml
├── spark/silver-to-gold-cronjob.yaml
├── spark/gold-to-elasticsearch-cronjob.yaml
├── spark/salary-model-train-cronjob.yaml
├── spark/speed-stream-es-job.yaml
├── spark/speed-checkpoint-pvc.yaml
├── spark/batch-crawler-checkpoint-pvc.yaml
├── airflow/Dockerfile
├── airflow/airflow.yaml
├── airflow/airflow-rbac.yaml
├── airflow/airflow-postgres.yaml
└── airflow/dags/
    ├── job_market_batch_pipeline.py
    ├── job_market_speed_layer_bootstrap.py
    └── job_market_speed_real_crawler.py
```

### Minikube Profile
- Profile name: `job-market`
- Khởi động: `minikube start -p job-market --driver=docker --cpus=8 --memory=10000 --disk-size=40g`
- **LƯU Ý:** Image bị mất sau khi tắt máy → phải rebuild sau mỗi restart

---

## 13. AIRFLOW — ORCHESTRATION

### DAG chính: `job_market_batch_pipeline`

```
check_hdfs + check_elasticsearch
    ↓
crawl_jobs
    ↓
raw_to_bronze
    ↓
bronze_to_silver
    ↓
train_salary_model
    ↓
silver_to_gold
    ↓
gold_to_elasticsearch
```

### Airflow endpoints
- **UI:** `http://localhost:8082` (sau `kubectl port-forward svc/airflow-webserver 8082:8080 -n airflow`)
- **Login:** `admin / admin`

---

## 14. QUYẾT ĐỊNH KỸ THUẬT ĐÃ CHỐT

| Vấn đề | Quyết định | Lý do |
|---|---|---|
| `json_ld` type tại Bronze | `StringType()` | Giữ nguyên JSON string, Silver mới parse bằng `get_json_object` |
| `extra_inf` type | `StringType()` | Schema không cố định |
| `deadline` type | `LongType()` trong schema Spark, cast → Timestamp sau | |
| Skills merge | `array_distinct(concat(coalesce(skillsNeeded,[]), coalesce(skillsShouldHave,[])))` | |
| `crawl_domain` | `parse_url(col, 'HOST')` — native Spark | Không dùng Python UDF |
| Partition format | `ingest_date=YYYY-MM-DD` (Hive-style) | |
| Bronze write mode | `append` + `partitionBy` | Không xóa data cũ khi chạy lại |
| Bronze dedup key | `(job_id, hash_content)` | Giữ bản `ingest_ts` mới nhất |
| `record_version` | `dense_rank()` over `(job_id)` orderBy `ingest_ts` | |
| Silver write mode | `overwrite` + `partitionOverwriteMode=dynamic` | Chỉ replace partition đang write |
| Silver dedup key | `job_id` | Giữ `record_version` max (1 bản canonical per job) |
| Silver salary source | `ld_salary_min/max` từ json_ld là primary | Fallback regex `salary` string |
| Silver tỉ giá USD | `USD_TO_VND = 25_000` (hằng số, cập nhật định kỳ) | |
| `findspark` | Chỉ dùng khi dev local | Không cần trong K8s |
| Salary prediction | Chỉ predict khi cả min/max đều null | Không ghi đè lương thật |
| `salary_is_negotiable` | Chỉ là cờ mô tả, không điều khiển prediction | |
| Speed trigger | `TRIGGER_SECONDS=30` | Producer poll mỗi 2s, record xuất hiện mỗi vài giây |
| ML model | GBTRegressor thay Linear Regression | Salary không tuyến tính |
| `company_name` | Feature chính, không bucket | |
| `primary_city` | Dùng `city`, `location_detail[0].city`, hoặc parse từ `location` | |
| Speed layer embedding | Không dùng embedding | Giữ 1 artifact cho batch và speed |

---

## 15. CẤU TRÚC THƯ MỤC

```
bigdata-job-market/
├── CONTEXT.md                          # Context tóm tắt dự án
├── PROJECT_ARCHITECTURE.md             # Architecture diagram
├── PROJECT_KNOWLEDGE.md                # File tổng hợp này
├── Makefile                            # Các lệnh make nhanh
├── pyproject.toml                      # Python project config
│
├── apps/
│   ├── api/
│   │   ├── Dockerfile
│   │   └── search_api.py              # FastAPI search server
│   ├── batch/
│   │   └── jobs/
│   │       ├── raw_to_bronze.py       # ETL Raw → Bronze ✅
│   │       ├── bronze_to_silver.py    # ETL Bronze → Silver ✅
│   │       ├── silver_to_gold.py      # ETL Silver → Gold + ML ✅
│   │       ├── gold_to_elasticsearch.py # Load Gold → ES ✅
│   │       └── train_salary_model.py  # Train GBTRegressor ✅
│   ├── common/                        # Shared utilities
│   ├── ingestion/
│   │   ├── topcv_crawler.py          # Core crawler logic
│   │   ├── batch_crawler.py          # Batch crawler wrapper
│   │   ├── run_crawler.py            # CLI entry point
│   │   └── CRAWLER_LOGIC.md          # Crawler logic doc
│   ├── ml/
│   │   ├── salary_prediction.py      # GBTRegressor pipeline, feature eng, scoring
│   │   └── IMPLEMENTATION_PLAN.md    # ML implementation plan
│   ├── producer/
│   │   ├── kafka_job_producer.py     # Live crawl → Kafka
│   │   └── crawler_jsonl_producer.py # JSONL → Kafka (watch file)
│   ├── serving/
│   │   └── api.py                    # Serving endpoint helper
│   └── stream_etl/
│       ├── stream_main.py            # Spark Structured Streaming main
│       ├── normalizers.py            # Salary/skill normalization cho speed
│       ├── transform.py              # Streaming transformations
│       ├── schemas/
│       │   └── raw_job_schema.py     # PySpark schema cho raw job
│       ├── sinks/
│       │   ├── elasticsearch_sink.py       # → realtime_jobs_v1
│       │   ├── jobs_per_10m_sink.py        # → realtime_job_counts_10m_v1
│       │   ├── kafka_sink.py               # → jobs_clean
│       │   ├── salary_bins_realtime_sink.py # → realtime_salary_bins_hourly_v1
│       │   └── top_skills_hourly_sink.py   # → realtime_skill_counts/top_skills
│       └── stateful_jobs/
│           ├── jobs_per_10m.py
│           ├── salary_bins_realtime.py
│           └── top_skills_hourly.py
│
├── configs/
│   ├── settings.py                   # SALARY_MODEL_PATH, v.v.
│   ├── streaming.dev.yaml
│   ├── kafka.dev.yaml
│   └── app.dev.yaml
│
├── data/
│   ├── raw/raw_data_format.md        # Raw schema spec (source of truth)
│   ├── bronze/bronze_data_format.md  # Bronze schema spec
│   ├── silver/silver_data_format.md  # Silver schema spec
│   └── gold/gold_data_format.md      # Gold schema spec
│
├── docs/                             # Runbooks, hướng dẫn vận hành
│   ├── salary_prediction_runbook.md  # End-to-end batch+speed runbook
│   ├── spark_ml_salary_prediction_readme.md
│   ├── airflow_batch_pipeline.md
│   ├── streaming_elasticsearch_indexes.md
│   ├── raw_to_bronze_runbook.md
│   ├── bronze_to_silver_runbook.md
│   ├── hdfs_data_ingestion.md
│   ├── run_pipeline_to_elasticsearch.md
│   ├── run_speed_minikube.md
│   ├── minikube_setup.md
│   ├── kibana_dashboard_import.md
│   └── integration_deliverable_v1.md
│
├── infra/                            # Tất cả K8s manifests + Dockerfiles
├── runtime/                          # Runtime data (crawler cache, v.v.)
├── scripts/                          # Shell/PowerShell scripts tiện ích
├── shared/
│   ├── quality/streaming_quality_rules.py
│   ├── udfs/salary_parser.py        # Shared Spark UDFs
│   └── schemas.py
└── tests/                            # Unit tests
```

---

## 16. PORT FORWARD NHANH

| Service | Lệnh | URL |
|---|---|---|
| Elasticsearch | `kubectl port-forward svc/elasticsearch 9200:9200 -n search` | `http://localhost:9200` |
| Kibana | `kubectl port-forward -n search svc/kibana 5601:5601` | `http://localhost:5601` |
| Airflow | `kubectl port-forward svc/airflow-webserver 8082:8080 -n airflow` | `http://localhost:8082` |
| HDFS Web | `kubectl port-forward -n hdfs svc/hdfs-namenode 9870:9870` | `http://localhost:9870` |
| FastAPI | `kubectl port-forward -n serving svc/job-search-api 8001:8000` | `http://localhost:8001` |
| Kafka | `kubectl port-forward -n kafka svc/my-cluster-kafka-bootstrap 9092:9092` | `localhost:9092` |

---

## 17. LỆNH THAO TÁC PHỔ BIẾN

### Chạy batch job thủ công (PowerShell helper)

```powershell
function Run-BatchJob {
  param([string]$CronJob, [string]$JobName, [string]$Timeout = "7200s")
  kubectl delete job -n spark $JobName --ignore-not-found=true
  kubectl create job $JobName --from=cronjob/$CronJob -n spark
  kubectl wait --for=condition=complete job/$JobName -n spark --timeout=$Timeout
  kubectl logs -n spark -l job-name=$JobName --all-containers=true --tail=300
}

# Chạy từng bước
Run-BatchJob "batch-etl-crawl-jobs" "manual-crawl-jobs" "108000s"
Run-BatchJob "batch-etl-raw-to-bronze" "manual-raw-to-bronze" "7200s"
Run-BatchJob "batch-etl-bronze-to-silver" "manual-bronze-to-silver" "7200s"
Run-BatchJob "batch-etl-train-salary-model" "manual-train-salary-model" "7200s"
Run-BatchJob "batch-etl-silver-to-gold" "manual-silver-to-gold" "7200s"
Run-BatchJob "batch-etl-gold-to-elasticsearch" "manual-gold-to-elasticsearch" "7200s"
```

### Rebuild Spark image

```powershell
minikube -p job-market image build -f infra\spark\Dockerfile -t spark-job-market:latest .
```

### Upload raw JSONL lên HDFS

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -mkdir -p /raw/jobs/ingest_date=2026-05-08
kubectl cp data/raw/jobs/jobs_2026-05-08.jsonl hdfs/hdfs-namenode-0:/tmp/jobs_2026-05-08.jsonl
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -put -f /tmp/jobs_2026-05-08.jsonl /raw/jobs/ingest_date=2026-05-08/
```

### Kiểm tra HDFS

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /raw/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /silver/jobs
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -ls -R /models/salary_prediction
```

### Start speed layer

```powershell
kubectl delete job -n spark speed-stream-es-submit --ignore-not-found=true
kubectl apply -f infra\spark\speed-stream-es-job.yaml
kubectl logs -n spark -l app=speed-stream --all-containers=true --tail=300 -f
```

### HDFS permissions (nếu bị lỗi)

```powershell
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /raw
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /bronze
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /silver
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /gold
kubectl exec -n hdfs hdfs-namenode-0 -- hdfs dfs -chmod -R 777 /models
```

---

## 18. ELASTICSEARCH INDEXES — ĐẦY ĐỦ

### Batch Layer
| Index | Mô tả |
|---|---|
| `gold-jobs-flat` | Batch Gold jobs đã enrich bởi Spark ML |

### Speed Layer
| Index | Mô tả | Type |
|---|---|---|
| `realtime_jobs_v1` | Job realtime đã clean (1 doc = 1 job) | Job-level |
| `realtime_job_counts_10m_v1` | Số job theo window 10 phút | Aggregate |
| `realtime_skill_counts_hourly_v1` | Số job theo skill/giờ | Aggregate |
| `realtime_top_skills_hourly_v1` | Top skills theo giờ (ranked) | Aggregate |
| `realtime_salary_bins_hourly_v1` | Phân bố lương theo giờ | Aggregate |

### Code writers

| Index | Writer file |
|---|---|
| `gold-jobs-flat` | `apps/batch/jobs/gold_to_elasticsearch.py` |
| `realtime_jobs_v1` | `apps/stream_etl/sinks/elasticsearch_sink.py` |
| `realtime_job_counts_10m_v1` | `apps/stream_etl/sinks/jobs_per_10m_sink.py` |
| `realtime_skill_counts_hourly_v1` | `apps/stream_etl/sinks/top_skills_hourly_sink.py` |
| `realtime_top_skills_hourly_v1` | `apps/stream_etl/sinks/top_skills_hourly_sink.py` |
| `realtime_salary_bins_hourly_v1` | `apps/stream_etl/sinks/salary_bins_realtime_sink.py` |

---

## 19. KIBANA — SỬ DỤNG

### Data Views cần tạo

1. `gold-jobs-flat` — time field: `date_posted`
2. `realtime_jobs_v1` — time field: `date_posted`

### KQL hữu ích

```text
# Kiểm tra salary prediction
salary_source: "predicted"
salary_source: "parsed_range"
salary_source: "parsed_min_only"
salary_source: "parsed_max_only"
salary: "Thỏa thuận" and salary_prediction_applied: true
salary_min_vnd >= 30000000 and salary_prediction_applied: false

# Kiểm tra lỗi (không nên có):
salary: "Thỏa thuận" and salary_min_vnd >= 0 and salary_prediction_applied: true
```

### Recommended fields trong Discover
```
title, company_name, salary, salary_min_vnd, salary_max_vnd,
salary_display_avg_vnd, salary_prediction_applied, salary_source,
salary_prediction_model_version, date_posted, has_remote, skills
```

---

## 20. TROUBLESHOOTING PHỔ BIẾN

| Vấn đề | Cách xử lý |
|---|---|
| `cluster "minikube" does not exist` khi build | Dùng `minikube -p job-market image build ...` |
| Spark job không tạo driver pod | `kubectl get pods -n spark`, `kubectl describe job -n spark <job>` |
| HDFS permission denied (user `spark`) | `hdfs dfs -chmod -R 777 /bronze /silver /gold /models` |
| Train model lỗi thiếu `numpy` | Rebuild Spark image (Dockerfile đã cài numpy) |
| Không thấy model | Chạy `batch-etl-train-salary-model`, kiểm tra `/models/salary_prediction` |
| Gold không có `salary_source` | Rebuild image → chạy lại `silver_to_gold` → `gold_to_elasticsearch` |
| Kibana không thấy field mới | Stack Management → Data Views → Refresh field list |
| Kafka không reachable | `docker ps` — đảm bảo kafka pod Running |
| Checkpoint conflicts (speed) | Xóa PVC rồi apply lại `speed-checkpoint-pvc.yaml` |
| Speed không dùng model mới | Restart `speed-stream-es-submit` job |
| ES không có index | Kiểm tra log `gold_to_elasticsearch` hoặc speed sink |

---

## 21. GHI CHÚ QUAN TRỌNG

1. **Salary precedence:** `salary_min_vnd / salary_max_vnd` là lương thật. `salary_display_*` là lương cuối dùng cho UI/API. `salary_predicted_*` chỉ có ý nghĩa khi `salary_prediction_applied=true`.

2. **Không merge batch và speed thành 1 index:** Batch → `gold-jobs-flat`, Speed → `realtime_jobs_v1`. Đây là 2 luồng khác nhau về độ chính xác và latency.

3. **realtime_jobs_v1 không có tất cả field của Gold:** Các field chỉ parse được từ `json_ld` (như `company_id`, `company_logo`, `company_url`, `work_country`) đã bị bỏ khỏi speed schema vì nguồn speed không đủ tin cậy.

4. **Model không train theo `--date`:** Luôn train trên toàn bộ Silver để tránh thiếu dữ liệu và lệch phân phối.

5. **Bronze dedup key khác Silver dedup key:**
   - Bronze: `(job_id, hash_content)` — nhiều bản cho cùng job (lịch sử thay đổi)
   - Silver: `job_id` — 1 bản canonical mới nhất per job

6. **`json_ld` không parse tại Bronze:** Chỉ lưu dạng JSON string. Silver mới parse bằng `get_json_object`.

7. **USD → VND = 25,000:** Hằng số cứng trong code, cập nhật định kỳ theo tỉ giá thực tế.

8. **Airflow là scheduler chính:** CronJobs đều để `suspend: true`. Airflow trigger chúng theo thứ tự DAG.

9. **Image bị mất sau restart Minikube:** Phải rebuild `spark-job-market:latest` sau mỗi lần `minikube start`.

10. **Speed layer load model một lần khi start:** Nếu batch train model mới, cần restart speed job để load model mới.
