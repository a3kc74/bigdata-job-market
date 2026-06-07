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
- Chỉ thêm field mới: lấy từ json_ld hoặc suffix mô tả rõ ràng
- Không thêm field "tự quy ước" (enum tự định nghĩa, threshold tùy chỉnh)
- Chỉ dùng **native Spark functions** — không dùng Python UDF
- **Null convention:** derived fields (`location_detail`, `salary_*_vnd`) dùng `null` cho "không có thông tin"

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

### A. Từ `json_ld` parsing

*Sử dụng hàm native `F.get_json_object(col("json_ld"), "$.path")` để trích xuất trực tiếp dữ liệu có cấu trúc từ schema.org JobPosting, giúp tránh lỗi do thay đổi schema (schema evolution) của trường JSON.*

| Tên trường | Kiểu | JSON-LD path | Mô tả chi tiết & Ý nghĩa nghiệp vụ/kỹ thuật |
| :--- | :--- | :--- | :--- |
| `company_url` | String | `$.hiringOrganization.sameAs` | **Website doanh nghiệp**: URL website chính thức của công ty tuyển dụng. Hệ thống tự động unescape ký tự `\/` để trả về định dạng URL sạch, phục vụ việc hiển thị liên kết trực tiếp trên UI tin tuyển dụng. |
| `company_logo` | String | `$.hiringOrganization.logo` | **Logo doanh nghiệp**: URL hình ảnh biểu trưng của doanh nghiệp. Dùng để làm phong phú giao diện người dùng (hiển thị logo công ty bên cạnh tin tuyển dụng). |
| `work_country` | String | `$.jobLocation.address.addressCountry` | **Quốc gia làm việc**: Mã quốc gia theo tiêu chuẩn ISO (thường là `"VN"`). Dùng để phân loại địa lý ở cấp quốc gia khi hệ thống mở rộng quy mô. |
| `job_location_type` | String | `$.jobLocationType` | **Hình thức địa điểm**: Phân loại môi trường làm việc vật lý từ nguồn. Giá trị `"TELECOMMUTE"` biểu thị vị trí làm việc từ xa (remote), các giá trị khác hoặc `null` tương ứng làm việc tại văn phòng (onsite). |
| `salary_currency` | String | `$.baseSalary.currency` | **Đơn vị tiền tệ**: Đơn vị tiền tệ của mức lương gốc đăng ký trong JSON-LD (Ví dụ: `"VND"`, `"USD"`), phục vụ làm căn cứ quy đổi tỷ giá. |
| `salary_min` | Double | `$.baseSalary.value.minValue` | **Lương tối thiểu thô**: Giá trị lương tối thiểu nhận diện trực tiếp từ JSON-LD. Đơn vị đo lường phụ thuộc vào `salary_unit` và cần quy đổi ở bước sau. `null` nếu thiếu thông tin. |
| `salary_max` | Double | `$.baseSalary.value.maxValue` | **Lương tối đa thô**: Mức lương tối đa nhận diện trực tiếp từ JSON-LD. Đơn vị phụ thuộc vào `salary_unit`. `null` nếu thiếu thông tin. |
| `salary_unit` | String | `$.baseSalary.value.unitText` | **Chu kỳ lương**: Đơn vị chu kỳ thanh toán lương thô từ nguồn (Ví dụ: `"MONTH"` - theo tháng, `"YEAR"` - theo năm, `"HOUR"` - theo giờ). |
| `job_id_platform` | String | `$.identifier.value` | **ID nội bộ nền tảng**: Mã định danh tin tuyển dụng hoặc mã nội bộ của TopCV. Được dùng làm khóa liên kết doanh nghiệp để gom nhóm hoặc xây dựng bảng chiều (Dimension) về thông tin công ty. |

### B. Salary canonical (Chuẩn hóa thông tin Lương)

*Tập hợp các quy tắc nghiệp vụ giúp đưa toàn bộ thông tin tiền lương về một đại lượng số học thống nhất là **VNĐ/tháng**.*

| Tên trường | Kiểu | Mô tả chi tiết & Quy tắc chuẩn hóa |
| :--- | :--- | :--- |
| `salary_min_vnd` | Long | **Lương tối thiểu chuẩn hóa (VNĐ/tháng)**: <br>- *Nguồn ưu tiên (Primary)*: Quy đổi từ số `salary_min` trong JSON-LD dựa trên đơn vị chu kỳ `salary_unit` (nếu chu kỳ là `"YEAR"` thì chia 12, `"HOUR"` thì nhân với số giờ làm tiêu chuẩn) và nhân với tỷ giá quy đổi ngoại tệ nếu tiền tệ là USD.<br>- *Nguồn dự phòng (Fallback)*: Áp dụng Regex bóc tách các con số từ chuỗi văn bản thô `salary` của Bronze (Ví dụ: `"15 - 20 Triệu"` tách ra cận dưới là `15000000`). Trả về `null` nếu tin tuyển dụng thuộc dạng thỏa thuận và không thể bóc tách số liệu cụ thể. |
| `salary_max_vnd` | Long | **Lương tối đa chuẩn hóa (VNĐ/tháng)**: Cách thức quy đổi tương tự như trường `salary_min_vnd` nhưng áp dụng cho cận trên của khoảng lương. |
| `salary_is_negotiable` | Boolean | **Cờ lương thỏa thuận**: Nhận giá trị `true` nếu chuỗi lương thô `salary` chứa các từ khóa biểu thị sự thỏa thuận (Ví dụ: `"Thỏa thuận"`, `"Cạnh tranh"`) HOẶC cả hai cận `salary_min_vnd` và `salary_max_vnd` đều bằng `null` (do trường `baseSalary` luôn tồn tại trong cấu trúc JSON-LD nhưng bị thiếu giá trị cụ thể). Cờ này là tín hiệu quan trọng điều khiển luồng dự báo lương bằng học máy (Machine Learning) ở tầng Gold. |

* **Tỉ giá quy đổi ngoại tệ cố định:** `USD → VND = 25,000` (được khai báo dạng hằng số trong mã nguồn và cập nhật định kỳ).

### C. Location canonical (Chuẩn hóa Địa điểm)

| Tên trường | Kiểu | Mô tả chi tiết & Quy tắc chuẩn hóa |
| :--- | :--- | :--- |
| `location_count` | Integer | **Số lượng chi nhánh làm việc**: Tính bằng `size(location)`, đếm tổng số địa điểm chi nhánh/văn phòng làm việc đăng ký trên tin tuyển dụng. |
| `location_detail` | Array\<Struct\<city: String, address: String\>\> | **Chi tiết địa điểm chuẩn hóa**: <br>- Được xây dựng dưới dạng một mảng các cấu trúc chứa hai thuộc tính con là `city` (Tỉnh/Thành phố lớn) và `address` (Địa chỉ chi tiết).<br>- *Quy tắc cắt chuỗi*: Phân tách chuỗi địa điểm gốc của Bronze tại ký tự hai chấm (`:`) đầu tiên. Phần trước dấu hai chấm đại diện cho `city`, phần sau đại diện cho `address`. Nếu không tìm thấy dấu `:`, gán `city` bằng toàn bộ chuỗi gốc và `address` bằng `null`. <br>- Cấu trúc này hỗ trợ lập chỉ mục kiểu Nested/Keyword trên Elasticsearch, giúp người dùng thực hiện lọc tin chính xác theo Tỉnh/Thành phố hoặc tìm kiếm chi nhánh lân cận. |
| `has_remote` | Boolean | **Cờ hỗ trợ làm việc từ xa**: Giá trị boolean, tự động nhận giá trị `true` khi trường `job_location_type` của JSON-LD có giá trị bằng `"TELECOMMUTE"`. |

### D. Experience (Chuẩn hóa Kinh nghiệm)

| Tên trường | Kiểu | Mô tả chi tiết & Quy tắc chuẩn hóa |
| :--- | :--- | :--- |
| `experience_required` | Boolean | **Cờ yêu cầu kinh nghiệm**: Nhận giá trị `false` nếu chuỗi dữ liệu kinh nghiệm gốc `monthOfExperience` chứa các cụm từ biểu thị không yêu cầu (như `"Không yêu cầu"`, `"Chưa có kinh nghiệm"`, `"Thỏa thuận"`), ngược lại mặc định được đánh dấu là `true`. Giúp lọc nhanh các cơ hội việc làm dành cho sinh viên mới ra trường hoặc người chưa có kinh nghiệm thực tế. |

---

## Dedup Policy tại Silver

```
Input: Bronze (nhiều record per job_id, nhiều record_version)
Output: Silver (1 record per job_id, record_version cao nhất)
```

Khác với Bronze dedup theo `(job_id, hash_content)`, Silver dedup theo **`job_id`** — chỉ giữ snapshot mới nhất.

---

## Usage Policy

- `salary_min/max` — raw số từ JSON-LD, đơn vị theo `salary_unit`. Dùng `salary_min/max_vnd` cho analytics.
- `experience_required` — `false` nếu không yêu cầu kinh nghiệm, ngược lại `true`.
- `json_ld` — giữ nguyên raw string để audit/re-parse khi cần.
- `location` Bronze — giữ nguyên, không bị xóa. Dùng `location_detail` cho phân tích vị trí.
