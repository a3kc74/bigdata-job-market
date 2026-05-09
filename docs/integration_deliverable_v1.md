# Integration Deliverable Specification

**Document ID:** ID-TOPCV-JOBS-V1
**Version:** 1.0.0
**Status:** Draft for implementation
**Related document:** `data_contract.md`
**Scope:** Integration outputs, mappings, downstream contracts, validation, handoff

---

## 1. Mục tiêu

Đặc tả này định nghĩa các deliverable tích hợp cần có để đưa data contract vào triển khai thực tế, bao gồm:

* canonical outputs theo layer
* mapping raw -> bronze -> silver -> gold
* integration targets cho storage và downstream
* acceptance criteria
* validation tests
* handoff checklist

---

## 2. Deliverable Set

### 2.1 Mandatory deliverables

1. canonical raw envelope definition
2. bronze normalized output schema
3. silver canonical output schema
4. gold aggregate definitions
5. field-level mapping matrix
6. key-generation rules and sample validations
7. batch/realtime alignment matrix
8. storage target specification
9. downstream consumer contract summary
10. acceptance criteria
11. validation test checklist
12. operational handoff checklist

### 2.2 Recommended implementation artifacts

* `raw_jobs.schema.json`
* `silver_jobs_schema.py`
* `gold_tables.yaml`
* `taxonomy/skills.yml`
* `tests/test_job_id_generation.py`
* `tests/test_company_name_resolution.py`
* `tests/test_salary_parsing.py`
* `tests/test_skill_filtering.py`
* `tests/test_batch_realtime_alignment.py`

---

## 3. Canonical Decisions for Sample Record

### 3.1 Business identity

```yaml
source: "topcv"
normalized_source_url: "https://www.topcv.vn/viec-lam/tour-operator-dieu-hanh-tour-inbound-thu-nhap-tu-us-700-us-1500-thang/2111673.html"
job_id: "678bd9b075bde10570f67fefdef1f94c8ff169ee7732f95a533a5ca15e4b0d15"
hash_content: "7f4506c38669f1e9733bdc063a739160a3bdf39e0c232419e95d2be42c62fc9e"
record_version: 1
```

### 3.2 Business resolution

```yaml
job_title: "Tour Operator/ Điều Hành Tour Inbound"
job_title_display: "Tour Operator/ Điều Hành Tour Inbound (Thu Nhập Từ $US 700 – $US 1500/Tháng)"
company_name: "Công ty TNHH Du Lịch Authentic Asia"
company_name_display: "HA TRAVEL"
employment_type: "FULL_TIME"
employment_type_vi: "Toàn thời gian"
salary_min: 700.0
salary_max: 1500.0
currency: "USD"
salary_period: "month"
posting_date: 2026-04-07
valid_through_ts: 2026-05-07T23:59:59+07:00
city: "Hà Nội"
district: "Hoàng Mai"
ward: "Phường Hoàng Mai"
openings: 1
```

### 3.3 Explicit exclusions

* `job_type` -> loại khỏi canonical layer do nhiễu UI/payment
* top-level `skills` -> không dùng làm authoritative skill source; chỉ dùng như candidate input đã lọc
* relative deadline `(Còn 18 ngày)` -> không dùng làm canonical deadline source

---

## 4. Transformation Mapping

## 4.1 Raw -> Bronze

| Raw field             | Bronze field                  | Rule                | Notes                  |
| --------------------- | ----------------------------- | ------------------- | ---------------------- |
| `source_url`          | `source_url`                  | passthrough         | preserve original URL  |
| `source_url`          | `normalized_source_url`       | normalize URL       | remove tracking params |
| `domain`              | `crawl_domain`                | passthrough         | provenance             |
| `crawled_at`          | `ingest_ts`                   | cast timestamp      | ingestion time         |
| `crawled_at`          | `event_ts`                    | fallback assignment | event time unavailable |
| `title`               | `job_title_raw`               | passthrough         | raw title              |
| `company_name`        | `company_name_raw`            | passthrough         | raw label              |
| `salary`              | `salary_raw`                  | passthrough         | raw salary text        |
| `location`            | `location_raw`                | passthrough         | raw location           |
| `level`               | `level_raw`                   | passthrough         | raw level              |
| `experience`          | `experience_raw`              | passthrough         | raw experience         |
| `deadline`            | `deadline_raw`                | passthrough         | raw deadline           |
| `job_type`            | `job_type_raw`                | passthrough         | audit only             |
| `quantity`            | `quantity_raw`                | passthrough         | raw quantity           |
| `description`         | `description_raw`             | passthrough         | raw description        |
| `requirements`        | `requirements_raw`            | passthrough         | raw requirements       |
| `benefits`            | `benefits_raw`                | passthrough         | raw benefits           |
| `description_items`   | `description_items_raw_count` | count               | summary metric         |
| `requirement_items`   | `requirement_items_raw_count` | count               | summary metric         |
| `benefit_items`       | `benefit_items_raw_count`     | count               | summary metric         |
| `skills`              | `skills_raw_count`            | count               | summary metric         |
| `categories`          | `categories_raw_count`        | count               | summary metric         |
| `json_ld`             | `json_ld_present`             | boolean             | structured evidence    |
| `meta_tags`           | `meta_tags_present`           | boolean             | metadata flag          |
| `sections_by_heading` | `sections_by_heading_present` | boolean             | parsing support        |
| `page_text`           | `page_text_present`           | boolean             | parsing support        |

## 4.2 Bronze -> Silver

| Bronze field(s)                                        | Silver field                                             | Rule                                |
| ------------------------------------------------------ | -------------------------------------------------------- | ----------------------------------- |
| `job_title_raw`                                        | `job_title_display`                                      | passthrough                         |
| `job_title_raw` + regex cleanup                        | `job_title`                                              | remove salary phrase in parentheses |
| `company_name_raw` + `json_ld.hiringOrganization.name` | `company_name`                                           | structured source priority          |
| `company_name_raw`                                     | `company_name_display`                                   | passthrough                         |
| raw + structured names                                 | `company_aliases`                                        | distinct merge                      |
| `normalized_source_url`                                | `job_id`                                                 | hash generation                     |
| selected content fields                                | `hash_content`                                           | content hash                        |
| `json_ld.baseSalary.minValue`                          | `salary_min`                                             | structured parse                    |
| `json_ld.baseSalary.maxValue`                          | `salary_max`                                             | structured parse                    |
| `json_ld.baseSalary.currency`                          | `currency`                                               | structured parse                    |
| `json_ld.baseSalary.value.unitText`                    | `salary_period`                                          | lowercase normalize                 |
| `json_ld.employmentType`                               | `employment_type`                                        | enum normalize                      |
| `employment_type`                                      | `employment_type_vi`                                     | enum mapping                        |
| `json_ld.datePosted`                                   | `posting_date`                                           | cast date                           |
| `json_ld.validThrough`                                 | `valid_through_ts`                                       | cast timestamp                      |
| `json_ld.jobLocation.address.addressRegion`            | `city`                                                   | structured parse                    |
| section/location parse                                 | `district`                                               | normalize district                  |
| `json_ld.jobLocation.address.addressLocality`          | `ward`                                                   | structured parse                    |
| parsed location pieces                                 | `location_display`                                       | canonical render                    |
| `json_ld.jobLocation.address.addressCountry`           | `country_code`                                           | passthrough                         |
| `json_ld.jobLocation.address.postalCode`               | `postal_code`                                            | cast string                         |
| `json_ld.occupationalCategory`                         | `level`                                                  | structured parse                    |
| `level`                                                | `level_normalized`                                       | mapping table                       |
| `json_ld.experienceRequirements.monthsOfExperience`    | `experience_months_min`                                  | direct parse                        |
| `experience_raw`                                       | `experience_text`                                        | passthrough                         |
| requirements + skill blocks                            | `skills`                                                 | taxonomy filter                     |
| skill taxonomy                                         | `hard_skills`                                            | subset                              |
| soft-skill taxonomy                                    | `soft_skills`                                            | subset                              |
| benefit extraction                                     | `benefits_tags`                                          | taxonomy filter                     |
| schedule section                                       | `work_schedule_text`                                     | parse string                        |
| schedule parse                                         | `requires_weekend_rotation`                              | infer boolean                       |
| cleaned section texts                                  | `description_text`, `requirements_text`, `benefits_text` | canonical text                      |

## 4.3 Silver -> Gold

| Silver field(s)                                          | Gold dataset                       | Output field(s)                       | Rule                          |
| -------------------------------------------------------- | ---------------------------------- | ------------------------------------- | ----------------------------- |
| `posting_date`, `city`, `employment_type`, salary fields | `gold_job_facts_daily`             | `job_count`, salary aggregates        | daily aggregate               |
| `skills`, `posting_date`, `city`                         | `gold_skill_counts_daily`          | `job_count`, ranks, salary aggregates | explode skills then aggregate |
| `skills`, `posting_date`, salary fields                  | `gold_salary_stats_by_skill_month` | salary distribution metrics           | month + skill aggregate       |
| `company_name`, `posting_date`, `city`, salary fields    | `gold_company_hiring_by_month`     | hiring intensity metrics              | month + company aggregate     |
| `skills` pairs                                           | `gold_skill_cooccurrence_weekly`   | pair metrics                          | pairwise weekly aggregate     |

---

## 5. Expected Outputs

### 5.1 Raw

* 1 append-only raw envelope record
* original payload preserved
* source-normalized metadata attached

### 5.2 Bronze

* 1 deterministic bronze record
* keys and quality flags attached
* raw business strings retained
* no irreversible business canonicalization

### 5.3 Silver

* 1 canonical analytics-ready record
* typed salary, company, location, title, experience, skills
* crawler/UI noise removed

### 5.4 Gold

Sample record đóng góp vào:

* `gold_job_facts_daily` cho `2026-04-07`
* `gold_skill_counts_daily` theo từng canonical skill
* `gold_salary_stats_by_skill_month` cho `2026-04`
* `gold_company_hiring_by_month` cho `2026-04`
* `gold_skill_cooccurrence_weekly` cho `2026-W15`

---

## 6. Gold Feature Delivery Plan

### 6.1 Primary features

**Volume and demand**

* `job_count`
* `distinct_company_count`
* `distinct_job_title_count`
* `new_job_count`
* `updated_job_count`
* `active_job_count`

**Salary**

* `avg_salary_min`
* `avg_salary_max`
* `median_salary_min`
* `median_salary_max`
* `salary_disclosed_ratio`

**Skill demand**

* `skill_job_count`
* `skill_distinct_company_count`
* `skill_rank_overall`
* `skill_rank_by_city`
* `skill_growth_7d`
* `skill_growth_30d`

**Company hiring**

* `company_job_count`
* `company_distinct_roles`
* `company_avg_salary_min`
* `company_avg_salary_max`
* `company_hiring_intensity`

**Experience and language**

* `experience_bucket_job_count`
* `english_required_ratio`
* `junior_ratio`
* `mid_ratio`
* `senior_ratio`

**Benefits**

* `social_insurance_ratio`
* `training_ratio`
* `team_building_ratio`
* `travel_benefit_ratio`
* `bonus_13th_month_ratio`
* `benefit_coverage_ratio`

**Data quality**

* `structured_salary_coverage`
* `structured_company_coverage`
* `skill_parse_confidence_avg`
* `record_quality_score_avg`

### 6.2 Optional features

* currency normalization
* text richness
* quality heuristics
* graph/network metrics
* time-series metrics
* search/recommendation features
* governance metrics

---

## 7. Storage Targets

### HDFS

```text
/raw/jobs/source=topcv/ingest_date=2026-04-19/
/bronze/jobs/source=topcv/ingest_date=2026-04-19/
/silver/jobs/posting_date=2026-04-07/city=ha_noi/
/gold/job_facts_daily/date_key=2026-04-07/
/gold/skill_counts_daily/date_key=2026-04-07/
/gold/salary_stats_by_skill_month/month_key=2026-04/
/gold/company_hiring_by_month/month_key=2026-04/
/gold/skill_cooccurrence_weekly/week_key=2026-W15/
```

### Kafka

```text
topic.jobs_raw
topic.jobs_clean
topic.jobs_dead_letter
```

Message key:

```text
job_id
```

### Cassandra

```text
jobs_by_day
jobs_by_skill
salary_stats_by_skill_month
company_stats_by_month
realtime_skill_counts
realtime_job_counts_10m
```

### Elasticsearch

```text
jobs_silver_v1
gold_skill_counts_daily_v1
gold_company_hiring_by_month_v1
jobs_realtime_v1
```

---

## 8. Downstream Consumer Contract

### Dashboard

Stable fields:

* `time_bucket`
* `city`
* `skill`
* `company_name`
* `job_count`
* `avg_salary_min`
* `avg_salary_max`
* `median_salary_min`
* `median_salary_max`

### API

Serving/API layer có thể phụ thuộc vào:

* canonical `company_name`
* canonical `job_title`
* normalized location fields
* typed salary fields
* stable aggregate field names giữa batch và realtime

### Search

Elasticsearch indexing có thể phụ thuộc vào:

* `job_title_display`
* `company_name`
* `location_display`
* `skills`
* `category_level_1/2/3`
* `benefits_tags`
* `status`

---

## 9. Acceptance Criteria

### 9.1 Record-level

Sample record được chấp nhận nếu:

* `job_id = 678bd9b075bde10570f67fefdef1f94c8ff169ee7732f95a533a5ca15e4b0d15`
* `normalized_source_url` đã loại tracking params
* `company_name = Công ty TNHH Du Lịch Authentic Asia`
* `company_name_display = HA TRAVEL`
* `employment_type = FULL_TIME`
* `job_type_raw` không tham gia canonical employment mapping
* `salary_min = 700.0`
* `salary_max = 1500.0`
* `currency = USD`
* `salary_period = month`
* `posting_date = 2026-04-07`
* `valid_through_ts = 2026-05-07T23:59:59+07:00`
* silver `skills` không chứa noisy UI tokens

### 9.2 Gold-level

* sample đóng góp đúng 1 count vào `gold_job_facts_daily` cho `2026-04-07`
* sample đóng góp 1 count cho mỗi canonical skill trong `gold_skill_counts_daily`
* sample đóng góp 1 count vào `gold_company_hiring_by_month` cho `2026-04`
* sample đóng góp đúng tập skill pairs duy nhất vào `gold_skill_cooccurrence_weekly`

### 9.3 Alignment

* batch và realtime reuse stable field names cho cùng semantic
* dashboard/API không cần semantic remapping cho shared fields

---

## 10. Validation Tests

### Key and URL tests

* normalize tracking URL to canonical URL
* generate stable `job_id` from canonical URL
* generate stable `hash_content` from canonical content inputs

### Canonical field resolution tests

* company conflict resolution prefers structured source
* salary parsing prefers JSON-LD `baseSalary`
* employment type ignores noisy `job_type_raw`
* deadline parsing prefers structured `validThrough`

### Taxonomy tests

* remove noisy skill tokens như `Xem thêm`, `Nổi bật`, `Quyền lợi:`
* keep valid skill concepts như `mice`, `tour guide`, `tiếng anh`, `điều hành tour`
* separate `soft_skills` và `hard_skills`

### Aggregate tests

* skill explosion count deterministic
* company aggregate count deterministic
* weekly co-occurrence pairs unique và reproducible
* batch/realtime aligned field names unchanged

---

## 11. Operational Handoff Checklist

### Engineering

* [ ] canonical URL normalization function implemented
* [ ] `job_id` generation function implemented
* [ ] `hash_content` generation function implemented
* [ ] structured-source priority resolver implemented
* [ ] salary parser implemented
* [ ] company-name conflict resolver implemented
* [ ] skill taxonomy filter implemented
* [ ] gold aggregates implemented

### Data governance

* [ ] field dictionary reviewed
* [ ] compatibility policy reviewed
* [ ] schema version recorded
* [ ] breaking-change procedure documented
* [ ] PR checklist published

### Platform

* [ ] Kafka topics created
* [ ] HDFS partition layout created
* [ ] Cassandra tables created
* [ ] Elasticsearch indexes created
* [ ] batch and streaming sinks aligned

### QA

* [ ] sample record output snapshot approved
* [ ] automated schema tests added
* [ ] aggregate tests added
* [ ] regression test set created for noisy crawler cases

---

## 12. Implementation Boundary

### In scope

* deterministic transformation of the sample TopCV record
* technical contract for layer outputs
* integration targets for batch, streaming, serving, and search
* gold feature prioritization
* consumer-facing naming alignment

### Out of scope (initial delivery)

* currency conversion based on external FX service
* embedding generation
* advanced graph scoring
* ML-based quality scoring
* realtime salary analytics

---

## 13. Final Integration Output Summary

### Mandatory layer outputs

```yaml
raw_output: 1 event
bronze_output: 1 normalized record
silver_output: 1 canonical record
gold_daily_job_facts_contribution: 1
gold_skill_daily_contributions: N canonical skills
gold_company_monthly_contribution: 1
gold_skill_pair_weekly_contributions: M canonical skill pairs
```

### Target status

```yaml
identity_stable: true
canonical_company_resolved: true
salary_structured: true
employment_type_resolved: true
location_structured: true
skill_noise_filtered: true
batch_realtime_names_aligned: true
gold_ready: true
```
