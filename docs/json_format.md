# Tài liệu Đặc tả Cấu trúc Dữ liệu Crawler (Raw Schema v1.0)

**Nguồn dữ liệu:** TopCV
**Định dạng lưu trữ:** JSONL (Mỗi dòng là một đối tượng JSON độc lập)
**Cập nhật lần cuối:** (Điền ngày hôm nay)
**Owner:** Thành viên 2 (Data Sources)

---

## 1. Cấu trúc Cấp ngoài cùng (Root Level)
*Tuân thủ nghiêm ngặt Hợp đồng Dữ liệu (Data Contract) với các trường cốt lõi. Không được tự ý thay đổi hoặc thêm bớt ở cấp độ này.*

| Tên trường | Kiểu dữ liệu | Bắt buộc | Mô tả |
| :--- | :--- | :---: | :--- |
| `source` | String | Có | Tên nguồn cào dữ liệu (Mặc định: `"topcv"`). |
| `source_url` | String | Có | URL gốc của bài đăng tuyển dụng. |
| `normalized_source_url` | String | Có | URL đã được chuẩn hóa (bỏ tham số tracking `?ta_source=...`). |
| `crawl_version` | Integer | Có | Phiên bản của Crawler (Hiện tại: `1`). |
| `ingest_ts` | Long (Unix ms) | Có | Thời gian lúc crawler bóc tách dữ liệu thành công. |
| `event_ts` | Long (Unix ms) | Không | Thời gian đăng bài lấy từ JSON-LD. Có thể `null`. |
| `job_id` | String | Có | Mã định danh duy nhất (SHA256 của `source` + `normalized_url`). |
| `hash_content` | String | Có | Mã băm (SHA256) của nội dung lõi, dùng phát hiện cập nhật bài đăng. |
| `payload` | Object | Có | Khối chứa toàn bộ dữ liệu nghiệp vụ đã được làm phẳng (Xem Mục 2). |
| `quality_flags` | Object | Có | Khối chứa các cờ boolean đánh giá chất lượng (Xem Mục 3). |

---

## 2. Cấu trúc Khối Payload (Flattened Business Data)
*Khu vực chứa toàn bộ dữ liệu thô đã được bóc tách. Các trường đã được dàn phẳng tối đa.*

| Tên trường | Kiểu dữ liệu | Nguồn bóc tách | Mô tả chi tiết |
| :--- | :--- | :--- | :--- |
| **Thông tin cơ bản** | | | |
| `title` | String | JSON-LD / HTML | Tiêu đề công việc. |
| `company_name` | String | JSON-LD / HTML | Tên công ty tuyển dụng. |
| `company_scale` | String | HTML | Quy mô nhân sự (VD: `"100-499 nhân viên"`). |
| `company_field` | String | HTML | Lĩnh vực hoạt động của công ty. |
| `company_address`| String | HTML | Địa chỉ chi tiết của công ty. |
| `salary` | String | HTML (Regex) | Mức lương hiển thị (VD: `"10 - 15 Triệu"`, `"Thỏa thuận"`). |
| `location` | Array[String] | HTML | Danh sách địa điểm làm việc (Thường là mảng Tỉnh/Thành phố). |
| `monthOfExperience`| String/Integer| JSON-LD / HTML | Yêu cầu số tháng/năm kinh nghiệm. |
| `deadline` | Long (Unix ms)| HTML (Regex) | Hạn nộp hồ sơ (Timestamp 23:59:59 của ngày hết hạn). |
| `occupationalCategory`| String | JSON-LD / HTML | Cấp bậc công việc (VD: `"Nhân viên"`, `"Trưởng phòng"`). |
| `education` | String | HTML | Yêu cầu học vấn (VD: `"Đại Học trở lên"`). |
| `employmentType` | String | JSON-LD | Hình thức làm việc (VD: `"Toàn thời gian"`). |
| `openings` | Integer/String| JSON-LD | Số lượng tuyển dụng (Nếu có). |
| **Thông tin nội dung (Văn bản dài)** | | | |
| `description` | Array[String] | HTML (Heuristic) | Mảng các chuỗi mô tả công việc. Mỗi phần tử là 1 gạch đầu dòng. |
| `requirements` | Array[String] | HTML (Heuristic) | Mảng các chuỗi yêu cầu ứng viên. |
| `benefits` | Array[String] | HTML (Heuristic) | Mảng các chuỗi quyền lợi ứng viên. |
| `income` | Array[String] | HTML | Mảng các chuỗi phụ cấp/thu nhập thêm (nếu tách riêng khỏi salary). |
| `schedule` | String | HTML (Heuristic) | Thời gian làm việc (VD: `"Thứ 2 - Thứ 6 (từ 08:00 đến 17:00)"`). |
Lưu ý: các trường này sẽ có thể có nhiều định dạng khác nhau, tùy theo việc có thể crawl nó bằng cách nào.
| **Kỹ năng & Chuyên môn** | | | |
| `skillsNeeded` | Array[String] | HTML | Kỹ năng BẮT BUỘC (Must-have skills). |
| `skillsShouldHave`| Array[String] | HTML | Kỹ năng NÊN CÓ (Nice-to-have skills). |
| `specialty` | Array[String] | HTML / JSON-LD | Chuyên môn công việc (VD: `["Software Engineer", "Backend"]`). |
| **Siêu dữ liệu thô (Raw Meta)** | | | |
| `extra_inf` | Object | HTML | Các trường thông tin tùy chỉnh khác (Custom forms). |
| `meta_tags` | Object | HTML | Toàn bộ meta tags của trang web (phục vụ debug/SEO tracking). |
| `json_ld` | Object | JSON-LD | Toàn bộ khối JSON-LD gốc của TopCV (Để backup nếu cần bóc lại). |
| `pageText` | String | HTML | Toàn bộ văn bản thô của trang (Vét máng Regex cuối cùng). |

---

## 3. Cấu trúc Khối Quality Flags
*Dùng để TV3 (Batch Layer) và TV4 (Speed Layer) dễ dàng lọc dữ liệu.*

| Tên trường | Kiểu dữ liệu | Ý nghĩa khi bằng `true` |
| :--- | :--- | :--- |
| `has_json_ld` | Boolean | Trang web có chứa thẻ schema chuẩn JSON-LD. |
| `has_page_text` | Boolean | Có text nội dung, không bị lỗi load trắng trang. |
| `has_structured_company_name_conflict` | Boolean | Tên công ty ở JSON-LD khác với tên ở HTML. |
| `has_valid_posting_date` | Boolean | Trường `event_ts` parse thành công, không bị `null`. |
| `has_valid_deadline` | Boolean | Trường `deadline` parse thành công, không bị `null`. |
| `has_salary_info` | Boolean | Bóc tách thành công thông tin Lương. |
| `has_location_info` | Boolean | Bóc tách thành công Địa điểm. |
| `has_experience_info` | Boolean | Bóc tách thành công Kinh nghiệm. |
| `has_requirements` | Boolean | Mảng `requirements` có dữ liệu. |
| `has_description` | Boolean | Mảng `description` có dữ liệu. |
| `has_benefits` | Boolean | Mảng `benefits` có dữ liệu. |
| `has_skills_info` | Boolean | Có thông tin về kỹ năng (Needed hoặc Should Have). |
| `has_education_info`| Boolean | Có thông tin yêu cầu học vấn. |
| `has_specialty` | Boolean | Có thông tin chuyên môn. |
| `has_schedule` | Boolean | Có thông tin thời gian làm việc. |
| `has_employment_type`| Boolean | Có thông tin hình thức làm việc. |
| `has_income` | Boolean | Có thông tin thu nhập phụ. |
| `has_extra_info` | Boolean | Có thông tin form tùy chỉnh (extra_inf). |

---

## 4. Ví dụ (JSON Sample)

```json
file data1.json
Lưu ý: file json chỉ để xem định dạng, khi làm bài tập lớn sẽ lưu ở file jsonl.