# Tài liệu Đặc tả Cấu trúc Dữ liệu Silver (Silver Schema v1.0)

**Nguồn vào:** Bronze Parquet (`/bronze/jobs/ingest_date=YYYY-MM-DD/`)
**Định dạng lưu trữ:** Parquet (nén Snappy)
**HDFS path:** `/silver/jobs/ingest_date=YYYY-MM-DD/`
**Partition key:** `ingest_date`
**Dedup key:** `job_id` — giữ `record_version` cao nhất (1 bản duy nhất per job)

---

## Nguyên tắc thiết kế

- **Silver = Bronze + json_ld parsing + salary VNĐ normalization + location normalization + Dedup theo job_id**
- Passthrough 100% tên field từ Bronze — không đổi tên, không xóa
- Chỉ thêm field mới: prefix `ld_` (từ json_ld) hoặc suffix mô tả rõ ràng
- Không thêm field "tự quy ước" (enum tự định nghĩa, threshold tùy chỉnh)
- Chỉ dùng **native Spark functions** — không dùng Python UDF

---

## Passthrough từ Bronze (giữ nguyên 100%)

| Tên trường | Kiểu |
| :--- | :--- |
| `source` | String |
| `source_url` | String |
| `normalized_source_url` | String |
| `crawl_version` | Integer |
| `ingest_ts` | Timestamp |
| `event_ts` | Timestamp |
| `job_id` | String |
| `hash_content` | String |
| `title` | String |
| `company_name` | String |
| `company_scale` | String |
| `company_field` | String |
| `company_address` | String |
| `salary` | String |
| `location` | Array\<String\> |
| `monthOfExperience` | String |
| `deadline` | Timestamp |
| `occupationalCategory` | String |
| `education` | String |
| `employmentType` | String |
| `openings` | String |
| `description` | Array\<String\> |
| `requirements` | Array\<String\> |
| `benefits` | Array\<String\> |
| `income` | Array\<String\> |
| `schedule` | String |
| `skills` | Array\<String\> |
| `specialty` | Array\<String\> |
| `extra_inf` | String |
| `meta_tags` | Map\<String, String\> |
| `json_ld` | String |
| `pageText` | String |
| `quality_flags` | Map\<String, Boolean\> |
| `record_version` | Integer |
| `is_deleted` | Boolean |
| `crawl_domain` | String |
| `description_count` | Integer |
| `requirements_count` | Integer |
| `benefits_count` | Integer |
| `income_count` | Integer |
| `skills_count` | Integer |
| `specialty_count` | Integer |
| `ingest_date` | String |

---

## Thêm mới bởi Silver ETL

### A. Từ `json_ld` parsing (prefix `ld_`)

Dùng `F.get_json_object(col("json_ld"), "$.path")` — không dùng `from_json` fixed schema để tránh schema evolution.

| Tên trường | Kiểu | JSON-LD path | Ghi chú |
| :--- | :--- | :--- | :--- |
| `ld_deadline` | Timestamp | `$.validThrough` | Hạn tuyển dụng chuẩn từ JSON-LD |
| `ld_company_url` | String | `$.hiringOrganization.sameAs` | Website chính thức công ty |
| `ld_company_logo` | String | `$.hiringOrganization.logo` | URL logo công ty |
| `ld_work_locality` | String | `$.jobLocation.address.addressLocality` | Quận/huyện nơi làm việc |
| `ld_work_region` | String | `$.jobLocation.address.addressRegion` | Tỉnh/thành nơi làm việc |
| `ld_work_country` | String | `$.jobLocation.address.addressCountry` | Mã quốc gia ISO (thường `"VN"`) |
| `ld_job_location_type` | String | `$.jobLocationType` | `"TELECOMMUTE"` = remote, null = onsite |
| `ld_salary_currency` | String | `$.baseSalary.currency` | `"VND"` hoặc `"USD"` |
| `ld_salary_min` | Double | `$.baseSalary.value.minValue` | Raw từ JSON-LD, đơn vị theo `ld_salary_unit` |
| `ld_salary_max` | Double | `$.baseSalary.value.maxValue` | Raw từ JSON-LD |
| `ld_salary_unit` | String | `$.baseSalary.value.unitText` | `"MONTH"`, `"YEAR"`, `"HOUR"` |
| `ld_experience_months` | Integer | `$.experienceRequirements.monthsOfExperience` | Fallback: regex trên `monthOfExperience` |
| `ld_job_id_platform` | String | `$.identifier.value` | TopCV internal job ID |
| `ld_occupational_category` | String | `$.occupationalCategory` | Ngành nghề theo JSON-LD |

### B. Salary canonical

Primary source: `ld_salary_min/max` từ json_ld. Fallback: regex trên `salary` string.
Tất cả giá trị đã quy đổi về **VNĐ/tháng**.

| Tên trường | Kiểu | Mô tả |
| :--- | :--- | :--- |
| `salary_min_vnd` | Long | Lương tối thiểu (VNĐ/tháng). Null nếu không xác định được. |
| `salary_max_vnd` | Long | Lương tối đa (VNĐ/tháng). Null nếu không xác định được. |
| `salary_is_negotiable` | Boolean | `true` nếu `salary` string chứa "Thỏa thuận" |

**Tỉ giá quy đổi:** `USD → VND = 25,000` (hằng số, cập nhật định kỳ trong code)

### C. Location canonical

| Tên trường | Kiểu | Mô tả |
| :--- | :--- | :--- |
| `location_normalized` | Array\<String\> | Tên tỉnh/thành chuẩn hóa chính tả từ `location` Bronze |
| `location_count` | Integer | `size(location_normalized)` |
| `has_remote` | Boolean | `true` khi `ld_job_location_type = "TELECOMMUTE"` |

---

## Dedup Policy tại Silver

```
Input: Bronze (nhiều record per job_id, nhiều record_version)
Output: Silver (1 record per job_id, record_version cao nhất)
```

Khác với Bronze dedup theo `(job_id, hash_content)`, Silver dedup theo **`job_id`** — chỉ giữ snapshot mới nhất.

---

## Usage Policy

- `ld_salary_min/max` — raw số từ JSON-LD, đơn vị theo `ld_salary_unit`. Dùng `salary_min/max_vnd` cho analytics.
- `ld_experience_months` — số tháng integer sạch. Nếu null = không có thông tin (không tự suy ra).
- `json_ld` — giữ nguyên raw string để audit/re-parse khi cần.
- `location` Bronze — giữ nguyên, không bị xóa. `location_normalized` là bản clean thêm vào.
