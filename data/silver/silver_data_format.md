# Tài liệu Đặc tả Cấu trúc Dữ liệu Silver (Silver Schema v1.0)

**Nguồn vào:** Bronze Parquet (`/bronze/jobs/ingest_date=YYYY-MM-DD/`)
**Định dạng lưu trữ:** Parquet (nén Snappy)
**HDFS path:** `/silver/jobs/ingest_date=YYYY-MM-DD/`
**Partition key:** `ingest_date`
**Dedup key:** `job_id` — giữ `record_version` cao nhất (1 bản duy nhất per job)

---

## Nguyên tắc thiết kế

- **Silver = Bronze + json_ld parsing + salary VNĐ normalization + location normalization + Dedup theo job_id**
- Passthrough các field từ Bronze — không đổi tên, không xóa (ngoại trừ đổi `event_ts` thành `date_posted`)
- Chỉ thêm field mới: prefix `ld_` (từ json_ld) hoặc suffix mô tả rõ ràng
- Không thêm field "tự quy ước" (enum tự định nghĩa, threshold tùy chỉnh)
- Chỉ dùng **native Spark functions** — không dùng Python UDF
- **Null convention:** derived fields (`ld_*`, `location_detail`, `salary_*_vnd`) dùng `null` cho "không có thông tin"

---

## Passthrough từ Bronze (giữ nguyên 100%)

| Tên trường | Kiểu |
| :--- | :--- |
| `source` | String |
| `source_url` | String |
| `normalized_source_url` | String |
| `crawl_version` | Integer |
| `ingest_ts` | Timestamp |
| `date_posted` | Timestamp | (Đổi tên từ `event_ts` của Bronze) |
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
| `description` | String | (multi-line, `\n` separated) |
| `requirements` | String | (multi-line, `\n` separated) |
| `benefits` | String | (multi-line, `\n` separated) |
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
| `ld_company_url` | String | `$.hiringOrganization.sameAs` | Website chính thức công ty (đã unescape `\/`) |
| `ld_company_logo` | String | `$.hiringOrganization.logo` | URL logo công ty (đã unescape `\/`) |
| `ld_work_country` | String | `$.jobLocation.address.addressCountry` | Mã quốc gia ISO (thường `"VN"`) |
| `ld_job_location_type` | String | `$.jobLocationType` | `"TELECOMMUTE"` = remote, null = onsite |
| `ld_salary_currency` | String | `$.baseSalary.currency` | `"VND"` hoặc `"USD"` |
| `ld_salary_min` | Double | `$.baseSalary.value.minValue` | Raw từ JSON-LD, đơn vị theo `ld_salary_unit`. Null nếu vắng mặt. |
| `ld_salary_max` | Double | `$.baseSalary.value.maxValue` | Raw từ JSON-LD. Null nếu vắng mặt. |
| `ld_salary_unit` | String | `$.baseSalary.value.unitText` | `"MONTH"`, `"YEAR"`, `"HOUR"` |
| `ld_job_id_platform` | String | `$.identifier.value` | TopCV internal job ID — hoạt động như ID công ty, dùng để xây bảng dimension |

### B. Salary canonical

Primary source: `ld_salary_min/max` từ json_ld. Fallback: regex trên `salary` string.
Tất cả giá trị đã quy đổi về **VNĐ/tháng**.

| Tên trường | Kiểu | Mô tả |
| :--- | :--- | :--- |
| `salary_min_vnd` | Long | Lương tối thiểu (VNĐ/tháng). Null nếu không xác định được. |
| `salary_max_vnd` | Long | Lương tối đa (VNĐ/tháng). Null nếu không xác định được. |
| `salary_is_negotiable` | Boolean | `true` nếu `salary` string chứa "Thỏa thuận" hoặc cả `ld_salary_min` và `ld_salary_max` đều null (vì baseSalary luôn tồn tại, negotiable khi thiếu min/max) |

**Tỉ giá quy đổi:** `USD → VND = 25,000` (hằng số, cập nhật định kỳ trong code)

### C. Location canonical

| Tên trường | Kiểu | Mô tả |
| :--- | :--- | :--- |
| `location_count` | Integer | `size(location)` |
| `location_detail` | Array\<Struct\<city: String, address: String\>\> | Parse trực tiếp từ `location` của Bronze. Tách tại dấu `:` đầu tiên: `city` = phần trước, `address` = phần sau. Nếu không có `:` thì `city` = toàn bộ chuỗi, `address` = null. |
| `has_remote` | Boolean | `true` khi `ld_job_location_type = "TELECOMMUTE"` |

### D. Experience

| Tên trường | Kiểu | Mô tả |
| :--- | :--- | :--- |
| `experience_required` | Boolean | `false` nếu `monthOfExperience` là "Thỏa thuận", mặc định `true`. |

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
- `experience_required` — `false` nếu không yêu cầu kinh nghiệm, ngược lại `true`.
- `json_ld` — giữ nguyên raw string để audit/re-parse khi cần.
- `location` Bronze — giữ nguyên, không bị xóa. Dùng `location_detail` cho phân tích vị trí.
