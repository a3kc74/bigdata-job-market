# Tài liệu Đặc tả Cấu trúc Dữ liệu Gold (Gold Schema v1.0)

**Nguồn vào:** Silver Parquet (`/silver/jobs/ingest_date=YYYY-MM-DD/`)
**Định dạng lưu trữ:** Parquet (HDFS) và Elasticsearch Index
**HDFS path:** `/gold/jobs/job_market_index/ingest_date=YYYY-MM-DD/`
**Elasticsearch Index:** `job_market_index`
**Partition key (HDFS):** `ingest_date`

---

## Nguyên tắc thiết kế

- **Denormalization:** Tầng Gold không chia bảng Fact/Dim rườm rà. Toàn bộ thông tin Job và Company được gộp (flatten) vào chung một bảng `job_market_index` để push thẳng lên Elasticsearch.
- **Tối ưu UI/UX:** Phân loại rõ ràng các trường thành Text (phục vụ search), Keyword (phục vụ Dropdown, Button Filter), và Numeric (phục vụ tính Metrics).
- **Lược bỏ dữ liệu không cần thiết:** Lược bỏ các trường rác hoặc metadata nội bộ ETL (như `json_ld`, `pageText`, `hash_content`, `record_version`).

---

## Cấu trúc Bảng `job_market_index`

### A. Meta & Dates (Tracking & URL)
| Tên trường | Kiểu dữ liệu | Kiểu ES | Mô tả |
| :--- | :--- | :--- | :--- |
| `job_id` | String | Keyword | Primary Key |
| `company_id` | String | Keyword | ID định danh công ty (rename từ `job_id_platform`) |
| `source_url` | String | Keyword | URL gốc của job trên TopCV để user click nhảy trang |
| `date_posted` | Timestamp | Date | Ngày đăng job gốc (chuyển từ event_ts) |
| `deadline` | Timestamp | Date | Hạn nộp hồ sơ |
| `ingest_date` | String | Keyword | Ngày dữ liệu được crawl (Partition key) |
| `is_active` | Boolean | Boolean | Trạng thái tin tuyển dụng (hiện tại mặc định là `true` hoặc tính toán nếu quá deadline) |

### B. Full-Text Search (Phục vụ thanh tìm kiếm)
| Tên trường | Kiểu dữ liệu | Kiểu ES | Mô tả |
| :--- | :--- | :--- | :--- |
| `title` | String | Text | Tiêu đề công việc |
| `company_name` | String | Text | Tên công ty |
| `description` | String | Text | Mô tả công việc |
| `requirements` | String | Text | Yêu cầu ứng viên |
| `benefits` | String | Text | Quyền lợi |
| `company_scale` | String | Text | Quy mô công ty (Text search) |

### C. Categorical & Filters (Phục vụ Nút bấm, Dropdown)
| Tên trường | Kiểu dữ liệu | Kiểu ES | Mô tả |
| :--- | :--- | :--- | :--- |
| `company_field` | String | Keyword | Lĩnh vực hoạt động của công ty |
| `work_country` | String | Keyword | Quốc gia làm việc (VD: "VN") |
| `occupationalCategory` | String | Keyword | Cấp bậc (Nhân viên, Quản lý...) |
| `employmentType` | String | Keyword | Loại hình (FULL_TIME, PART_TIME...) |
| `education` | String | Keyword | Bằng cấp yêu cầu (Đại học, Cao đẳng...) |
| `salary_currency` | String | Keyword | Tiền tệ (VND, USD) |
| `salary_unit` | String | Keyword | Đơn vị tính lương (MONTH, YEAR) |
| `skills` | Array[String] | Keyword | Danh sách kỹ năng (Mảng) |
| `specialty` | Array[String] | Keyword | Chuyên môn (Mảng) |
| `location` | Array[String] | Text/Keyword | Chuỗi địa điểm gốc nguyên bản (VD: "Hà Nội: abc") |
| `location_detail` | Array[Struct] | Nested/Keyword | Struct chứa city, address |

### D. Booleans (Phục vụ Checkbox Filter)
| Tên trường | Kiểu dữ liệu | Kiểu ES | Mô tả |
| :--- | :--- | :--- | :--- |
| `has_remote` | Boolean | Boolean | Có hỗ trợ remote không |
| `experience_required` | Boolean | Boolean | Có yêu cầu kinh nghiệm không |
| `salary_is_negotiable` | Boolean | Boolean | Lương thỏa thuận không |
| `is_weekend_free` | Boolean | Boolean | Có làm cuối tuần (T7, CN) không? `True` = Nghỉ cuối tuần. |

### E. Lịch làm việc (Schedule)
| Tên trường | Kiểu dữ liệu | Kiểu ES | Mô tả |
| :--- | :--- | :--- | :--- |
| `schedule_type` | String | Keyword | Dùng làm nút bấm: "T2-T6", "T2-T7", "T2-CN", "Flexible", "Other" |
| `schedule` | String | Text | Nguyên bản text thời gian làm việc để hiển thị ra UI. |

### F. Numerical Metrics (Phục vụ biểu đồ tính toán)
| Tên trường | Kiểu dữ liệu | Kiểu ES | Mô tả |
| :--- | :--- | :--- | :--- |
| `salary_min_vnd` | Long | Long | Lương tối thiểu (VND/tháng) |
| `salary_max_vnd` | Long | Long | Lương tối đa (VND/tháng) |
| `monthOfExperience` | Integer | Integer | Số tháng kinh nghiệm tối thiểu yêu cầu |
| `openings` | Integer | Integer | Số lượng cần tuyển |
| `benefits_count` | Integer | Integer | Số lượng quyền lợi |
| `requirements_count` | Integer | Integer | Số lượng yêu cầu |
| `location_count` | Integer | Integer | Số lượng chi nhánh/nơi làm việc |
| `skills_count` | Integer | Integer | Số lượng kỹ năng yêu cầu |
| `specialty_count` | Integer | Integer | Số lượng chuyên môn yêu cầu |

### G. Raw Display (Hiển thị UI - Không dùng để search/filter)
| Tên trường | Kiểu dữ liệu | Kiểu ES | Mô tả |
| :--- | :--- | :--- | :--- |
| `salary` | String | Không Index | Chuỗi lương gốc để hiển thị (VD: "10 - 15 Triệu") |
| `company_logo` | String | Không Index | URL logo |
| `company_address` | String | Không Index | Địa chỉ chi tiết trụ sở |
| `company_url` | String | Không Index | Website của công ty |

---

## Chuyển đổi tại `silver_to_gold.py`

- Lấy dữ liệu từ Silver.
- Xóa các cột không cần thiết: `description_count`, `income_count`, `json_ld`, `pageText`, `hash_content`, `record_version`, `quality_flags`, v.v.
- Parse `schedule` để sinh ra `schedule_type` và `is_weekend_free`.
- Đổi tên `job_id_platform` thành `company_id`.
- Chuyển `monthOfExperience` thành Integer (thay "Thỏa thuận" thành `0` hoặc `null`).
- Lưu trữ ở HDFS Parquet tầng Gold.
