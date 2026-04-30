# Tài liệu Đặc tả Cấu trúc Dữ liệu Bronze (Bronze Schema v1.0)

**Nguồn vào:** Raw JSONL (`/raw/jobs/ingest_date=YYYY-MM-DD/`)
**Định dạng lưu trữ:** Parquet (nén Snappy)
**HDFS path:** `/bronze/jobs/ingest_date=YYYY-MM-DD/`
**Partition key:** `ingest_date`
**Dedup key:** `(job_id, hash_content, ingest_date)`
**Owner:** Thành viên 3 (Batch Layer)

---

## Nguyên tắc thiết kế

- **Bronze = Flatten Raw + Cast Types + Thêm metadata kỹ thuật + Count metrics**
- Fields giữ nguyên từ raw → **giữ nguyên tên** (không thêm `_raw`, không rename)
- `job_id` và `hash_content` passthrough 100% từ crawler, không tính lại
- `json_ld` lưu dạng raw JSON string — Silver mới dùng `from_json()` để parse
- `skillsNeeded` + `skillsShouldHave` gộp thành `skills` tại Bronze
- Không có business canonicalization ở Bronze

---

## Schema

### Từ raw root — passthrough, chỉ cast type

| Tên trường | Kiểu (Bronze) | Kiểu (Raw) | Ghi chú |
| :--- | :--- | :--- | :--- |
| `source` | String | String | Passthrough |
| `source_url` | String | String | Passthrough |
| `normalized_source_url` | String | String | Passthrough |
| `crawl_version` | Integer | Integer | Passthrough |
| `ingest_ts` | Timestamp | Long (Unix ms) | Cast: ms → timestamp |
| `event_ts` | Timestamp | Long (Unix ms) | Cast: ms → timestamp, nullable |
| `job_id` | String | String | Passthrough — không tính lại |
| `hash_content` | String | String | Passthrough — không tính lại |

### Từ payload — flatten, giữ nguyên tên

| Tên trường | Kiểu | Ghi chú |
| :--- | :--- | :--- |
| `title` | String | |
| `company_name` | String | |
| `company_scale` | String | |
| `company_field` | String | |
| `company_address` | String | |
| `salary` | String | |
| `location` | Array\<String\> | |
| `monthOfExperience` | String | |
| `deadline` | Timestamp | Cast: Unix ms → timestamp |
| `occupationalCategory` | String | |
| `education` | String | |
| `employmentType` | String | JSON-LD enum: `"FULL_TIME"`, `"PART_TIME"`... Audit only tại Bronze |
| `openings` | String | |
| `description` | Array\<String\> | |
| `requirements` | Array\<String\> | |
| `benefits` | Array\<String\> | |
| `income` | Array\<String\> | |
| `schedule` | String | |
| `skills` | Array\<String\> | **Gộp:** `array_union(skillsNeeded, skillsShouldHave)` |
| `specialty` | Array\<String\> | |
| `extra_inf` | String | Custom form data, giữ raw string |
| `meta_tags` | Map\<String, String\> | |
| `json_ld` | String | Raw JSON string — Silver dùng `from_json()` |
| `pageText` | String | |

### Từ quality_flags — passthrough

| Tên trường | Kiểu | Ghi chú |
| :--- | :--- | :--- |
| `quality_flags` | Map\<String, Boolean\> | Giữ nguyên tên và cấu trúc từ raw |

### Thêm mới bởi Bronze ETL

| Tên trường | Kiểu | Mô tả |
| :--- | :--- | :--- |
| `record_version` | Integer | Dedup: tăng khi `hash_content` đổi với cùng `job_id`. Mặc định `1` |
| `is_deleted` | Boolean | Mặc định `false` |
| `crawl_domain` | String | Parse từ `source_url` → `"www.topcv.vn"` |

### Count Metrics — thêm mới

| Tên trường | Kiểu | Công thức |
| :--- | :--- | :--- |
| `description_count` | Integer | `size(description)` |
| `requirements_count` | Integer | `size(requirements)` |
| `benefits_count` | Integer | `size(benefits)` |
| `income_count` | Integer | `size(income)` |
| `skills_count` | Integer | `size(skills)` (sau khi gộp) |
| `specialty_count` | Integer | `size(specialty)` |

### Partition column

| Tên trường | Kiểu | Công thức |
| :--- | :--- | :--- |
| `ingest_date` | String | `date_format(ingest_ts, 'yyyy-MM-dd')` |

---

## Deduplication Policy

Khi cùng `job_id` xuất hiện nhiều lần trong cùng `ingest_date`:
- Cùng `hash_content` → duplicate event → giữ 1 bản (bỏ trùng)
- Khác `hash_content` → nội dung thay đổi → tăng `record_version`

Silver sẽ dedup theo `job_id`, giữ `record_version` cao nhất.

---

## Usage Policy

- `employmentType` chỉ dùng cho audit — **không** làm canonical employment field
- `json_ld` chỉ lưu, không parse tại Bronze
- `extra_inf` giữ nguyên raw string, không parse
