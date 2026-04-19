# Data Contract Specification

**Document ID:** DC-TOPCV-JOBS-V1
**Version:** 1.0.0
**Status:** Draft for implementation
**Scope:** TopCV ingestion -> raw -> bronze -> silver -> gold

---

## 1. Mục tiêu

Đặc tả này định nghĩa data contract chuẩn cho pipeline job posting, bao gồm:

* schema theo từng layer
* quy tắc sinh key và versioning
* quy tắc canonicalization
* quy tắc dedup
* naming convention cho storage và downstream
* tương thích giữa batch và realtime

Phạm vi áp dụng:

* raw events
* bronze normalized records
* silver canonical records
* gold aggregates
* Kafka topics
* HDFS/Parquet layout
* Cassandra tables
* Elasticsearch indexes

---

## 2. Nguyên tắc thiết kế

1. Một job posting phải có một `job_id` ổn định xuyên suốt các layer.
2. Raw phải giữ nguyên để audit và replay.
3. Silver là layer canonical cho business fields.
4. Thay đổi nội dung phải được theo dõi bằng `hash_content` và `record_version`.
5. Batch gold và realtime aggregate phải dùng tên field thống nhất.
6. Normalization phải mang tính xác định.
7. Breaking change bắt buộc tăng version và có migration note.

---

## 3. Phân loại nguồn dữ liệu

### 3.1 Nguồn vào

* source: `topcv`
* content_type: `JobPosting`
* extraction_mode: crawler + structured metadata enrichment

### 3.2 Structured evidence available

* JSON-LD `JobPosting`
* page text
* sections by heading
* meta tags

### 3.3 Chất lượng dữ liệu phát hiện trong sample

* `company_name` top-level xung đột với `json_ld.hiringOrganization.name`
* `job_type` top-level là nhiễu UI/payment
* `skills` top-level chứa nhiều token nhiễu
* `deadline` top-level không canonical bằng `json_ld.validThrough`

---

## 4. Canonicalization Rules

### 4.1 Source

```text
source = "topcv"
```

### 4.2 URL normalization

Quy tắc cho `normalized_source_url`:

1. lowercase scheme và host
2. giữ nguyên path
3. loại query params dùng cho tracking/navigation
4. loại fragment
5. giữ đuôi `.html`

Ví dụ:

```text
input:
https://www.topcv.vn/viec-lam/tour-operator-dieu-hanh-tour-inbound-thu-nhap-tu-us-700-us-1500-thang/2111673.html?ta_source=...&u_sr_id=...

normalized:
https://www.topcv.vn/viec-lam/tour-operator-dieu-hanh-tour-inbound-thu-nhap-tu-us-700-us-1500-thang/2111673.html
```

### 4.3 Source priority

| Business field                                          | Priority 1                                          | Priority 2                    | Priority 3               |
| ------------------------------------------------------- | --------------------------------------------------- | ----------------------------- | ------------------------ |
| `company_name`                                          | `json_ld.hiringOrganization.name`                   | company block from page text  | top-level `company_name` |
| `salary_min`, `salary_max`, `currency`, `salary_period` | `json_ld.baseSalary`                                | regex từ `salary_raw`         | regex từ benefits text   |
| `employment_type`                                       | `json_ld.employmentType`                            | section `Hình thức làm việc`  | page text                |
| `posting_date`                                          | `json_ld.datePosted`                                | page text                     | null                     |
| `valid_through_ts`                                      | `json_ld.validThrough`                              | explicit deadline parse       | null                     |
| `experience_months_min`                                 | `json_ld.experienceRequirements.monthsOfExperience` | parse `experience_raw`        | parse requirements text  |
| `city`, `district`, `ward`, `postal_code`               | `json_ld.jobLocation.address` + section parse       | page text                     | top-level `location`     |
| `skills`                                                | requirement sections + skill blocks                 | filtered top-level candidates | NLP fallback             |
| `level`                                                 | `json_ld.occupationalCategory`                      | company info section          | page text                |
| `openings`                                              | `json_ld.totalJobOpenings`                          | company info section          | null                     |

---

## 5. Key Strategy

### 5.1 Business key

```text
job_id = sha256(lower(trim(source)) + "|" + normalize_url(source_url))
```

Sample:

```text
678bd9b075bde10570f67fefdef1f94c8ff169ee7732f95a533a5ca15e4b0d15
```

### 5.2 Content hash

```text
hash_content = sha256(
  normalize_text(job_title_raw) + "|" +
  normalize_text(company_name_raw) + "|" +
  normalize_text(location_raw) + "|" +
  normalize_text(salary_raw) + "|" +
  normalize_text(requirements_raw)
)
```

Sample:

```text
7f4506c38669f1e9733bdc063a739160a3bdf39e0c232419e95d2be42c62fc9e
```

### 5.3 Versioning

* cùng `job_id` và cùng `hash_content` -> duplicate event
* cùng `job_id` nhưng khác `hash_content` -> tăng `record_version`
* latest state -> max(`record_version`), tie-breaker max(`ingest_ts`)

---

## 6. Deduplication Policy

### Raw

* append-only
* không hard delete

### Bronze

Technical dedup key:

```text
(job_id, hash_content, ingest_date)
```

### Silver

Business dedup key:

```text
job_id
```

Selection:

* highest `record_version`
* tie-breaker: latest `ingest_ts`

### Gold

Aggregate từ:

* silver latest snapshot, hoặc
* canonicalized streaming events đã dedup

---

## 7. Layer Specifications

## 7.1 Raw

### Mục tiêu

Giữ dữ liệu gần nguồn nhất để audit, replay, debug parser.

### Raw envelope

```json
{
  "source": "topcv",
  "source_url": "...",
  "normalized_source_url": "...",
  "crawl_version": 1,
  "ingest_ts": "2026-04-19T14:53:01.653181",
  "event_ts": "2026-04-19T14:53:01.653181",
  "payload": {
    "domain": "www.topcv.vn",
    "fetch_method": "playwright",
    "title": "...",
    "company_name": "HA TRAVEL",
    "salary": "Mức lương 700 - 1,500 USD",
    "location": "Hà Nội",
    "experience": "Kinh nghiệm 1 năm",
    "deadline": "(Còn 18 ngày)",
    "description": "...",
    "requirements": "...",
    "benefits": "...",
    "description_items": [],
    "requirement_items": [],
    "benefit_items": [],
    "skills": [],
    "categories": [],
    "meta_tags": {},
    "json_ld": [],
    "sections_by_heading": {},
    "page_text": "..."
  }
}
```

### Raw quality flags

```yaml
has_json_ld: boolean
has_page_text: boolean
has_structured_salary: boolean
has_structured_company_name_conflict: boolean
has_noisy_skill_list: boolean
has_noisy_job_type: boolean
has_valid_posting_date: boolean
has_valid_deadline: boolean
```

## 7.2 Bronze

### Mục tiêu

Thêm metadata kỹ thuật, key, quality flags; chưa canonicalize business semantics quá sớm.

### Bronze schema

```yaml
job_id: string
hash_content: string
record_version: int
is_deleted: boolean
source: string
source_url: string
normalized_source_url: string
ingest_ts: timestamp
event_ts: timestamp
crawl_version: int
crawl_domain: string
fetch_method: string
job_title_raw: string
company_name_raw: string
salary_raw: string
location_raw: string
level_raw: string
experience_raw: string
deadline_raw: string
job_type_raw: string
quantity_raw: string
description_raw: string
requirements_raw: string
benefits_raw: string
description_items_raw_count: int
requirement_items_raw_count: int
benefit_items_raw_count: int
skills_raw_count: int
categories_raw_count: int
json_ld_present: boolean
meta_tags_present: boolean
sections_by_heading_present: boolean
page_text_present: boolean
quality_issue_flags: map<string, boolean>
```

### Bronze usage policy

* `job_type_raw` không dùng cho employment classification
* top-level `skills` không dùng trực tiếp cho analytics
* `company_name_raw` phải giữ lại cho audit và lineage

## 7.3 Silver

### Mục tiêu

Sinh bản ghi canonical, typed, analytics-ready.

### Silver schema

```yaml
job_id: string
hash_content: string
record_version: int
source: string
source_url: string
normalized_source_url: string
ingest_ts: timestamp
event_ts: timestamp
job_title: string
job_title_display: string
company_name: string
company_name_display: string
company_aliases: array<string>
city: string
district: string
ward: string
location_display: string
country_code: string
postal_code: string
level: string
level_normalized: string
employment_type: string
employment_type_vi: string
experience_text: string
experience_months_min: int
experience_months_max: int
education_min: string
education_display: string
salary_min: double
salary_max: double
currency: string
salary_period: string
salary_is_negotiable: boolean
salary_display: string
posting_date: date
valid_through_ts: timestamp
status: string
openings: int
industry_primary: string
industry_secondary: array<string>
category_level_1: string
category_level_2: string
category_level_3: string
language_requirements: array<string>
other_language_preference: boolean
skills: array<string>
soft_skills: array<string>
hard_skills: array<string>
benefits_tags: array<string>
work_schedule_text: string
work_schedule_type: string
requires_weekend_rotation: boolean
description_text: string
requirements_text: string
benefits_text: string
data_quality_score: double
quality_notes: array<string>
```

### Normalization rules

* `job_title`: bỏ salary phrase trong ngoặc
* `job_title_display`: giữ title gốc
* `company_name`: ưu tiên structured organization name
* `company_name_display`: giữ label raw
* `salary`: ưu tiên `json_ld.baseSalary`
* không convert VND ở silver gốc
* `employment_type`: map từ structured/page section, không dùng `job_type_raw`
* `skills`: lọc qua taxonomy, loại UI tokens, breadcrumb, location-only tokens
* `location`: chuẩn hóa thành `city`, `district`, `ward`, `location_display`, `postal_code`, `country_code`

### Enum mapping tối thiểu

**Employment type**

* `FULL_TIME` -> `Toàn thời gian`
* `PART_TIME` -> `Bán thời gian`
* `CONTRACTOR` -> `Hợp đồng`
* `INTERN` -> `Thực tập`
* `TEMPORARY` -> `Thời vụ`
* `OTHER` -> `Khác`

**Level**

* `Nhân viên` -> `staff`
* `Chuyên viên` -> `specialist`
* `Quản lý` -> `manager`
* `Trưởng nhóm` -> `lead`
* `Giám đốc` -> `director`

## 7.4 Gold

### Mục tiêu

Expose aggregate datasets cho dashboard, API, serving layer.

### Core gold tables

#### `gold_job_facts_daily`

```yaml
date_key: date
source: string
city: string
category_level_1: string
category_level_2: string
category_level_3: string
level_normalized: string
employment_type: string
job_count: bigint
distinct_company_count: bigint
avg_salary_min: double
avg_salary_max: double
median_salary_min: double
median_salary_max: double
english_required_ratio: double
benefit_coverage_ratio: double
```

#### `gold_skill_counts_daily`

```yaml
date_key: date
source: string
skill: string
city: string
category_level_3: string
job_count: bigint
distinct_company_count: bigint
avg_salary_min: double
avg_salary_max: double
skill_rank_in_day: int
skill_rank_in_city: int
```

#### `gold_salary_stats_by_skill_month`

```yaml
month_key: string
skill: string
city: string
category_level_3: string
job_count: bigint
avg_salary_min: double
avg_salary_max: double
median_salary_min: double
median_salary_max: double
p25_salary_min: double
p75_salary_max: double
```

#### `gold_company_hiring_by_month`

```yaml
month_key: string
company_name: string
city: string
job_count: bigint
distinct_job_title_count: bigint
avg_salary_min: double
avg_salary_max: double
hiring_rank_in_city: int
```

#### `gold_skill_cooccurrence_weekly`

```yaml
week_key: string
skill_a: string
skill_b: string
cooccurrence_count: bigint
lift_score: double
jaccard_score: double
```

### Primary gold features

* `job_count`
* `distinct_company_count`
* `avg_salary_min`
* `avg_salary_max`
* `median_salary_min`
* `median_salary_max`
* `top_skill_rank`
* `demand_trend_7d`
* `demand_trend_30d`
* `new_job_ratio`
* `updated_job_ratio`
* `english_required_ratio`
* `experience_bucket_count`
* `benefit_coverage_ratio`
* `skill_cooccurrence_count`
* `company_hiring_intensity`
* `data_quality_coverage`

### Optional feature groups

* currency normalization
* text richness
* job quality heuristics
* graph/network metrics
* rolling trends
* repost/update behavior
* search/recommendation vectors
* governance/lineage metrics

---

## 8. Batch and Realtime Alignment

### Alignment policy

Batch gold và realtime aggregates phải dùng cùng tên field khi cùng semantic.

### Mapping matrix

| Use case                 | Batch dataset                      | Realtime dataset          | Shared fields                               |
| ------------------------ | ---------------------------------- | ------------------------- | ------------------------------------------- |
| job counts over time     | `gold_job_facts_daily`             | `realtime_job_counts_10m` | `time_bucket`, `job_count`                  |
| top skills               | `gold_skill_counts_daily`          | `realtime_skill_counts`   | `time_bucket`, `skill`, `job_count`         |
| company hiring intensity | `gold_company_hiring_by_month`     | future optional           | `company_name`, `job_count`                 |
| salary by skill          | `gold_salary_stats_by_skill_month` | not required initially    | `skill`, `avg_salary_min`, `avg_salary_max` |

### Shared fields bắt buộc ổn định

* `time_bucket`
* `skill`
* `job_count`
* `company_name`
* `avg_salary_min`
* `avg_salary_max`

---

## 9. Storage and Naming Convention

### Field naming

* lowercase only
* `snake_case`
* không viết tắt mơ hồ
* thêm suffix version cho breaking index/schema

### HDFS paths

```text
/raw/jobs/source=topcv/ingest_date=2026-04-19/
/bronze/jobs/source=topcv/ingest_date=2026-04-19/
/silver/jobs/posting_date=2026-04-07/city=ha_noi/
/gold/job_facts_daily/date_key=2026-04-07/
/gold/skill_counts_daily/date_key=2026-04-07/
/gold/company_hiring_by_month/month_key=2026-04/
```

### Kafka topics

```text
jobs_raw
jobs_clean
jobs_dead_letter
```

Message key:

```text
job_id
```

### Cassandra tables

```text
jobs_by_day
jobs_by_skill
salary_stats_by_skill_month
company_stats_by_month
realtime_skill_counts
realtime_job_counts_10m
```

### Elasticsearch indexes

```text
jobs_silver_v1
jobs_realtime_v1
gold_skill_counts_daily_v1
gold_company_hiring_by_month_v1
```

---

## 10. Compatibility Policy

### Non-breaking

* thêm nullable field
* thêm optional aggregate field
* thêm optional quality flag

### Breaking

* đổi tên field hiện có
* đổi field type
* đổi business meaning
* đổi logic sinh `job_id`
* đổi source priority của core fields
* đổi shared field names giữa batch/realtime

### Versioning

* `v1.0.x`: patch clarification
* `v1.x.0`: additive compatible change
* `v2.0.0`: breaking change

### Required artifacts khi đổi schema

* updated schema definitions
* updated data dictionary
* updated mapping documentation
* migration note
* validation tests

---

## 11. Data Quality Requirements

### Required validations

* `source` not null
* `source_url` not null
* `normalized_source_url` not null
* `job_id` not null
* `hash_content` not null
* `ingest_ts` not null
* structured salary parse phải nhất quán nếu có
* company conflict resolution phải được log
* noisy skill tokens không được xuất hiện ở silver `skills`

### Recommended metrics

* structured salary coverage
* structured company coverage
* skill parse confidence
* location parse success rate
* canonicalization conflict rate
* raw-to-silver field coverage ratio

---

## 12. Reference Sample Record

```yaml
source: topcv
normalized_source_url: https://www.topcv.vn/viec-lam/tour-operator-dieu-hanh-tour-inbound-thu-nhap-tu-us-700-us-1500-thang/2111673.html
job_id: 678bd9b075bde10570f67fefdef1f94c8ff169ee7732f95a533a5ca15e4b0d15
hash_content: 7f4506c38669f1e9733bdc063a739160a3bdf39e0c232419e95d2be42c62fc9e
company_name: Công ty TNHH Du Lịch Authentic Asia
company_name_display: HA TRAVEL
job_title: Tour Operator/ Điều Hành Tour Inbound
salary_min: 700.0
salary_max: 1500.0
currency: USD
salary_period: month
employment_type: FULL_TIME
posting_date: 2026-04-07
valid_through_ts: 2026-05-07T23:59:59+07:00
city: Hà Nội
district: Hoàng Mai
ward: Phường Hoàng Mai
openings: 1
```

---

## 13. Implementation Notes

1. `company_name` conflict resolution phải deterministic và được log.
2. `job_type_raw` chỉ tồn tại ở raw/bronze, không dùng làm canonical employment field.
3. Không convert salary sang VND trong silver gốc nếu không có exchange-rate lineage.
4. Top-level `skills` chỉ là candidate input, không phải authoritative output.
5. Batch và streaming phải dùng lại cùng canonical field names.

---

## 14. Approval Gate

Spec được xem là sẵn sàng implement khi hoàn thành:

* review schema definitions
* validate key-generation logic
* pass sample record tests
* confirm downstream naming alignment
* attach PR checklist vào workflow
