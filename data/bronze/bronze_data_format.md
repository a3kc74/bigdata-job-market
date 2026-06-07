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
| `company_scale` | String | Flatten từ `company_details.scale` |
| `company_field` | String | Flatten từ `company_details.field` |
| `company_address` | String | Flatten từ `company_details.address` |
| `salary` | String | |
| `location` | Array\<String\> | |
| `monthOfExperience` | String | Mixed type trong raw (Integer/String) → đọc như String |
| `deadline` | Timestamp | Cast: Unix ms → timestamp |
| `occupationalCategory` | String | |
| `education` | String | |
| `employmentType` | String | JSON-LD enum: `"FULL_TIME"`, `"PART_TIME"`... Audit only tại Bronze |
| `openings` | String | Cast từ Integer → String |
| `description` | String | Multi-line text, các dòng phân cách bằng `\n` |
| `requirements` | String | Multi-line text, các dòng phân cách bằng `\n` |
| `benefits` | String | Multi-line text, các dòng phân cách bằng `\n` |
| `income` | Array\<String\> | |
| `schedule` | String | |
| `skills` | Array\<String\> | **Gộp:** `array_union(skillsNeeded, skillsShouldHave)` |
| `specialty` | Array\<String\> | |
| `extra_inf` | String | Custom form data, giữ raw string |
| `meta_tags` | Map\<String, String\> | |
| `json_ld` | String | Trích xuất từ Object bằng `get_json_object` → JSON string |
| `pageText` | String | |

### Từ quality_flags — passthrough

| Tên trường | Kiểu | Ghi chú |
| :--- | :--- | :--- |
| `quality_flags` | Map\<String, Boolean\> | Giữ nguyên tên và cấu trúc từ raw |

### Thêm mới bởi Bronze ETL

| Tên trường | Kiểu | Mô tả chi tiết & Ý nghĩa nghiệp vụ/kỹ thuật |
| :--- | :--- | :--- |
| `record_version` | Integer | **Phiên bản của bản ghi**: Được tính toán tự động bằng cách xếp hạng theo thời gian nạp `dense_rank() over (partition by job_id order by ingest_ts)`. Khi nội dung của cùng một công việc thay đổi (nhận diện qua `hash_content` khác biệt), phiên bản này sẽ tự động tăng lên (mặc định bắt đầu từ `1`). Điều này giúp hệ thống lưu trữ lịch sử các lần chỉnh sửa tin tuyển dụng của công ty thay vì ghi đè trực tiếp. |
| `is_deleted` | Boolean | **Cờ xóa mềm (Soft Delete flag)**: Mặc định được gán giá trị `false`. Được dùng làm chỉ mục kỹ thuật để đánh dấu trạng thái tin tuyển dụng đã bị đóng hoặc bị gỡ bỏ khỏi hệ thống nguồn (TopCV) mà không cần xóa vật lý bản ghi Parquet trên HDFS, bảo toàn tính bất biến của kho dữ liệu. |
| `crawl_domain` | String | **Tên miền nguồn thu thập**: Được trích xuất tự động từ `source_url` thông qua hàm native `parse_url(col, 'HOST')` (Ví dụ: `"www.topcv.vn"`). Trường này đóng vai trò quan trọng trong việc mở rộng kiến trúc dữ liệu sang mô hình đa nguồn (multi-source ingestion) trong tương lai, giúp lọc và phân loại dữ liệu theo website tuyển dụng gốc một cách nhanh chóng. |

### Count Metrics — thêm mới (Chỉ số đo lường văn bản)

*Các trường này hỗ trợ giám sát chất lượng dữ liệu (Data Quality Monitoring) và đánh giá độ chi tiết/chất lượng của tin tuyển dụng.*

| Tên trường | Kiểu | Công thức | Ý nghĩa & Vai trò |
| :--- | :--- | :--- | :--- |
| `description_count` | Integer | `size(split(description, '\n'))` | **Số dòng mô tả**: Đếm số lượng dòng trong văn bản mô tả công việc (phân cách bởi dấu xuống dòng `\n`). Dùng để đo lường độ dài và tính chi tiết của phần mô tả công việc. |
| `requirements_count` | Integer | `size(split(requirements, '\n'))` | **Số dòng yêu cầu**: Đếm số dòng trong phần yêu cầu ứng viên. Giúp đánh giá mức độ khắt khe hoặc chi tiết của tiêu chí tuyển dụng. |
| `benefits_count` | Integer | `size(split(benefits, '\n'))` | **Số dòng quyền lợi**: Đếm số dòng trong phần quyền lợi được hưởng. Dùng để đo lường sự hấp dẫn của vị trí tuyển dụng. |
| `income_count` | Integer | `size(income)` | **Số lượng khoản thu nhập phụ**: Đếm số lượng phần tử trong mảng `income` (các phụ cấp/khoản thu nhập thêm tách rời khỏi lương chính). |
| `skills_count` | Integer | `size(skills)` | **Số lượng kỹ năng**: Đếm tổng số kỹ năng yêu cầu (cả kỹ năng bắt buộc và kỹ năng ưu tiên sau khi đã thực hiện hợp và loại trùng mảng). |
| `specialty_count` | Integer | `size(specialty)` | **Số lượng chuyên môn**: Đếm số lượng chuyên môn được gắn thẻ cho tin tuyển dụng. |

### Partition column (Trường phân vùng dữ liệu)

| Tên trường | Kiểu | Công thức | Ý nghĩa & Vai trò |
| :--- | :--- | :--- | :--- |
| `ingest_date` | String | `date_format(ingest_ts, 'yyyy-MM-dd')` | **Ngày nạp dữ liệu**: Trích xuất từ timestamp thu thập dữ liệu `ingest_ts` dưới dạng chuỗi `YYYY-MM-DD`. Đây là cột phân vùng vật lý (Partition column) trên HDFS, giúp tối ưu hóa việc lưu trữ dữ liệu theo ngày và cải thiện đáng kể tốc độ truy vấn của Spark SQL khi lọc dữ liệu theo thời gian. |

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
