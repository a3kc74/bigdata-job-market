# Field Extraction Audit — TopCV

Đánh giá khả năng trích xuất từng field từ trang TopCV, theo từng layer.

**Ký hiệu:**
- 🟢 Lấy được dễ dàng (structured / visible rõ)
- 🟡 Lấy được nhưng không ổn định / cần xử lý
- 🔴 Rất khó hoặc không thể lấy

---

## RAW Layer

Phần lớn raw fields do crawler tự sinh hoặc copy trực tiếp từ trang — ít rủi ro nhất.

| Field | Độ khó | Nguồn | Ghi chú |
|-------|:------:|-------|---------|
| `source` | 🟢 | Hardcode | Luôn là `"topcv"` |
| `source_url` | 🟢 | URL page | Luôn có |
| `normalized_source_url` | 🟢 | Tính từ `source_url` | Bỏ query params tracking |
| `ingest_ts` | 🟢 | Tự sinh lúc crawl | `datetime.now()` |
| `event_ts` | 🟢 | Tự sinh lúc crawl | Dùng `ingest_ts` nếu không có posting date |
| `crawl_version` | 🟢 | Hardcode / config | Số version crawler |
| `crawl_domain` | 🟢 | Parse từ URL | `www.topcv.vn` |
| `fetch_method` | 🟢 | Hardcode | `playwright` |
| `title` (payload) | 🟢 | `<h1>` hoặc `<title>` tag | Luôn có |
| `company_name` (payload) | 🟡 | Visible block + JSON-LD `hiringOrganization.name` | Có thể conflict giữa 2 nguồn |
| `salary` (payload) | 🟢 | Visible text | Luôn có, dạng "15 - 30 triệu" |
| `location` (payload) | 🟢 | Visible | Chỉ có city-level |
| `experience` (payload) | 🟢 | Visible | Dạng "1 năm", "2-3 năm" |
| `deadline` (payload) | 🟡 | Visible "Còn X ngày" hoặc JSON-LD `validThrough` | Không phải lúc nào cũng có ngày cụ thể |
| `job_type` (payload) | 🟡 | Visible tag | Trường UI/payment của TopCV, **không phải employment_type** — nhiễu |
| `description` (payload) | 🟢 | HTML section | Luôn có |
| `requirements` (payload) | 🟢 | HTML section | Luôn có |
| `benefits` (payload) | 🟢 | HTML section | Thường có |
| `skills` (payload) | 🟡 | Tag chips cuối page | **Nhiều token nhiễu**: breadcrumb, city name, UI label |
| `categories` (payload) | 🟡 | Breadcrumb / tag | Không nhất quán giữa các job |
| `meta_tags` (payload) | 🟢 | `<meta>` tags | Luôn có |
| `json_ld` (payload) | 🟡 | `<script type="application/ld+json">` | Không phải job nào cũng có đầy đủ |
| `sections_by_heading` (payload) | 🟢 | Parse `<h3>` headings | Có thể khác nhau giữa các job |
| `page_text` (payload) | 🟢 | `innerText` toàn page | Luôn có |

---

## BRONZE Layer

Bronze lấy trực tiếp từ raw payload, thêm metadata kỹ thuật. Độ khó gần giống raw.

| Field | Độ khó | Nguồn | Ghi chú |
|-------|:------:|-------|---------|
| `job_id` | 🟢 | Tính: `sha256(source + "\|" + normalized_url)` | Ổn định theo URL |
| `hash_content` | 🟢 | Tính: `sha256(title + company + location + salary + requirements)` | Ổn định theo nội dung |
| `record_version` | 🟢 | Logic dedup | Tăng khi `hash_content` thay đổi |
| `is_deleted` | 🟢 | Mặc định `false` | Update khi detect job hết hạn |
| `source` | 🟢 | Raw | — |
| `source_url` | 🟢 | Raw | — |
| `normalized_source_url` | 🟢 | Raw | — |
| `ingest_ts` | 🟢 | Raw | — |
| `event_ts` | 🟢 | Raw | — |
| `crawl_version` | 🟢 | Raw | — |
| `crawl_domain` | 🟢 | Raw | — |
| `fetch_method` | 🟢 | Raw | — |
| `job_title_raw` | 🟢 | Raw payload `title` | — |
| `company_name_raw` | 🟢 | Raw payload `company_name` | Giữ nguyên để audit |
| `salary_raw` | 🟢 | Raw payload `salary` | — |
| `location_raw` | 🟢 | Raw payload `location` | — |
| `level_raw` | 🟢 | Raw payload visible UI | — |
| `experience_raw` | 🟢 | Raw payload `experience` | — |
| `deadline_raw` | 🟡 | Raw payload `deadline` | Text "Còn X ngày" — cần parse nếu muốn date |
| `job_type_raw` | 🟡 | Raw payload `job_type` | **Không dùng** cho employment classification |
| `quantity_raw` | 🟡 | Raw payload / JSON-LD `totalJobOpenings` | **Thường thiếu** |
| `description_raw` | 🟢 | Raw payload | — |
| `requirements_raw` | 🟢 | Raw payload | — |
| `benefits_raw` | 🟢 | Raw payload | — |
| `description_items_raw_count` | 🟢 | Đếm items | — |
| `requirement_items_raw_count` | 🟢 | Đếm items | — |
| `benefit_items_raw_count` | 🟢 | Đếm items | — |
| `skills_raw_count` | 🟢 | Đếm tags | — |
| `categories_raw_count` | 🟢 | Đếm tags | — |
| `json_ld_present` | 🟢 | Boolean flag | — |
| `meta_tags_present` | 🟢 | Boolean flag | — |
| `sections_by_heading_present` | 🟢 | Boolean flag | — |
| `page_text_present` | 🟢 | Boolean flag | — |
| `quality_issue_flags` | 🟢 | Tính từ raw | Map flag: `has_json_ld`, `has_noisy_skill_list`, ... |

---

## SILVER Layer

Silver yêu cầu canonicalize và parse — nơi có nhiều field khó nhất.

### Identity & Keys

| Field | Độ khó | Nguồn | Ghi chú |
|-------|:------:|-------|---------|
| `job_id` | 🟢 | Bronze | — |
| `hash_content` | 🟢 | Bronze | — |
| `record_version` | 🟢 | Bronze | — |
| `source` | 🟢 | Bronze | — |
| `source_url` | 🟢 | Bronze | — |
| `normalized_source_url` | 🟢 | Bronze | — |
| `ingest_ts` | 🟢 | Bronze | — |
| `event_ts` | 🟢 | Bronze | — |

### Job & Company

| Field | Độ khó | Nguồn | Ghi chú |
|-------|:------:|-------|---------|
| `job_title` | 🟢 | Bronze `job_title_raw` — bỏ salary phrase trong ngoặc | Bỏ "Lương từ X triệu" trong tiêu đề |
| `job_title_display` | 🟢 | Giữ nguyên `job_title_raw` | — |
| `company_name` | 🟡 | Ưu tiên JSON-LD `hiringOrganization.name` > page block | Conflict resolution phải log |
| `company_name_display` | 🟢 | Label raw trên page | — |
| `company_aliases` | 🔴 | Không có trên trang | Cần external source / bỏ |

### Location

| Field | Độ khó | Nguồn | Ghi chú |
|-------|:------:|-------|---------|
| `city` | 🟢 | `location_raw`, normalize tên tỉnh/thành | TopCV luôn có city |
| `location_display` | 🟢 | `location_raw` | — |
| `country_code` | 🟢 | Mặc định `VN` | — |
| `district` | 🔴 | Không có trên TopCV | Chỉ expose city-level — cần geocoding ngoài |
| `ward` | 🔴 | Không có trên TopCV | Như trên |
| `postal_code` | 🔴 | Không có trên TopCV | Như trên |

### Job Attributes

| Field | Độ khó | Nguồn | Ghi chú |
|-------|:------:|-------|---------|
| `level` | 🟢 | Visible UI "Nhân viên / Chuyên viên / Quản lý" | Structured |
| `level_normalized` | 🟢 | Map từ `level` theo enum | — |
| `employment_type` | 🟢 | Visible "Toàn thời gian" — map sang `FULL_TIME` | Structured |
| `employment_type_vi` | 🟢 | Map từ `employment_type` | — |
| `experience_text` | 🟢 | `experience_raw` | Giữ nguyên text |
| `experience_months_min` | 🟡 | Regex parse từ `experience_raw` | "2 năm" → 24, "1-3 năm" → 12 |
| `experience_months_max` | 🟡 | Regex parse từ `experience_raw` | Có thể null nếu chỉ có min |
| `education_min` | 🟡 | Visible nếu có ("Cao đẳng"), else extract free text | **Không phải lúc nào cũng structured** |
| `education_display` | 🟡 | Từ `education_min` hoặc free text | — |
| `openings` | 🟡 | JSON-LD `totalJobOpenings` hoặc parse text | **Thường thiếu** — nhiều job không điền |
| `status` | 🟡 | Infer từ `valid_through_ts` so với `ingest_ts` | Không explicit trên page |

### Salary

| Field | Độ khó | Nguồn | Ghi chú |
|-------|:------:|-------|---------|
| `salary_min` | 🟢 | JSON-LD `baseSalary.minValue` hoặc regex parse `salary_raw` | Cover ~80% job có salary text |
| `salary_max` | 🟢 | JSON-LD `baseSalary.maxValue` hoặc regex parse | Như trên |
| `salary_display` | 🟢 | `salary_raw` | Giữ nguyên text gốc |
| `salary_is_negotiable` | 🟡 | Detect "Thỏa thuận" trong `salary_raw` | Boolean parse |
| `currency` | 🟡 | Infer từ `salary_raw` ("triệu" → VND, "USD") | Không explicit structured |
| `salary_period` | 🟡 | Infer "tháng" từ context | Không explicit, mặc định `month` |

### Dates

| Field | Độ khó | Nguồn | Ghi chú |
|-------|:------:|-------|---------|
| `valid_through_ts` | 🟢 | JSON-LD `validThrough` hoặc tính từ "Còn X ngày" | Cover tốt |
| `posting_date` | 🟡 | JSON-LD `datePosted` | **Không phải job nào cũng có**, chỉ "cập nhật X ngày trước" |

### Categories & Industry

| Field | Độ khó | Nguồn | Ghi chú |
|-------|:------:|-------|---------|
| `category_level_1` | 🟡 | Breadcrumb / tag | Breadcrumb không nhất quán |
| `category_level_2` | 🟡 | Breadcrumb | Như trên |
| `category_level_3` | 🟡 | Breadcrumb | Như trên |
| `industry_primary` | 🟡 | Map từ category | Phụ thuộc category parse |
| `industry_secondary` | 🟡 | Nhiều category tags | Không nhất quán |

### Skills & Language

| Field | Độ khó | Nguồn | Ghi chú |
|-------|:------:|-------|---------|
| `skills` | 🟡 | Tag chips + filter taxonomy | Raw tags nhiễu — cần loại UI tokens, breadcrumb tokens |
| `hard_skills` | 🔴 | NLP classify từ `skills` | Cần taxonomy — không reliable |
| `soft_skills` | 🔴 | NLP classify từ `skills` | Như trên |
| `language_requirements` | 🔴 | Extract free text requirements | Không structured |
| `other_language_preference` | 🔴 | Extract free text | Như trên |

### Work Schedule

| Field | Độ khó | Nguồn | Ghi chú |
|-------|:------:|-------|---------|
| `work_schedule_text` | 🟡 | Section heading "Thời gian làm việc" nếu có | Không phải lúc nào cũng có |
| `work_schedule_type` | 🔴 | NLP inference từ free text | Không reliable |
| `requires_weekend_rotation` | 🔴 | NLP inference từ benefits/description | Không reliable |

### Text & Quality

| Field | Độ khó | Nguồn | Ghi chú |
|-------|:------:|-------|---------|
| `description_text` | 🟢 | `description_raw` cleaned | — |
| `requirements_text` | 🟢 | `requirements_raw` cleaned | — |
| `benefits_text` | 🟢 | `benefits_raw` cleaned | — |
| `benefits_tags` | 🟡 | Parse từ benefit_items list | Có thể thiếu |
| `data_quality_score` | 🟡 | Tự tính theo công thức — **chưa định nghĩa** | Cần TV1 + TV3 chốt công thức |
| `quality_notes` | 🟢 | Sinh từ quality flags | — |

---

## GOLD Layer

Gold là aggregates từ silver — độ khó chủ yếu ở **coverage của upstream** và **logic tính toán**, không phải extraction.

### `gold_job_facts_daily`

| Field | Độ khó | Ghi chú |
|-------|:------:|---------|
| `date_key` | 🟢 | Group by `posting_date` |
| `source` | 🟢 | Group by |
| `city` | 🟢 | Group by |
| `category_level_1/2/3` | 🟡 | Phụ thuộc silver category quality |
| `level_normalized` | 🟢 | Group by |
| `employment_type` | 🟢 | Group by |
| `job_count` | 🟢 | COUNT |
| `distinct_company_count` | 🟢 | COUNT DISTINCT |
| `avg_salary_min/max` | 🟡 | Chỉ tính được trên jobs có salary — **coverage ~60-80%** |
| `median_salary_min/max` | 🟡 | Cần `percentile_approx` — coverage phụ thuộc silver |
| `english_required_ratio` | 🔴 | Phụ thuộc `language_requirements` — field 🔴 ở silver |
| `benefit_coverage_ratio` | 🟡 | Phụ thuộc `benefits_tags` — field 🟡 ở silver |

### `gold_skill_counts_daily`

| Field | Độ khó | Ghi chú |
|-------|:------:|---------|
| `date_key`, `source`, `city`, `category_level_3` | 🟢 | Group by keys |
| `skill` | 🟡 | Phụ thuộc silver `skills` quality sau taxonomy filter |
| `job_count` | 🟢 | COUNT |
| `distinct_company_count` | 🟢 | COUNT DISTINCT |
| `avg_salary_min/max` | 🟡 | Coverage issue như trên |
| `skill_rank_in_day` | 🟢 | `RANK()` window function |
| `skill_rank_in_city` | 🟢 | `RANK()` window function |

### `gold_salary_stats_by_skill_month`

| Field | Độ khó | Ghi chú |
|-------|:------:|---------|
| `month_key`, `skill`, `city`, `category_level_3` | 🟡 | Phụ thuộc silver quality |
| `job_count` | 🟢 | COUNT |
| `avg/median salary` | 🟡 | Coverage issue |
| `p25_salary_min`, `p75_salary_max` | 🔴 | Cần đủ mẫu — **sparse data theo skill + tháng** |

### `gold_company_hiring_by_month`

| Field | Độ khó | Ghi chú |
|-------|:------:|---------|
| `month_key`, `company_name`, `city` | 🟢 | Group by |
| `job_count` | 🟢 | COUNT |
| `distinct_job_title_count` | 🟢 | COUNT DISTINCT |
| `avg_salary_min/max` | 🟡 | Coverage issue |
| `hiring_rank_in_city` | 🟢 | `RANK()` window function |

### `gold_skill_cooccurrence_weekly`

| Field | Độ khó | Ghi chú |
|-------|:------:|---------|
| `week_key`, `skill_a`, `skill_b` | 🟡 | Phụ thuộc silver `skills` quality |
| `cooccurrence_count` | 🟢 | Self-join + COUNT |
| `lift_score` | 🔴 | Cần tính P(A∩B)/P(A)P(B) — **nặng, dễ sai nếu skill list nhiễu** |
| `jaccard_score` | 🔴 | Cần tính \|A∩B\|/\|A∪B\| — **như trên** |

---

## Tóm tắt

| Layer | 🟢 | 🟡 | 🔴 |
|-------|:--:|:--:|:--:|
| Raw | 20 | 4 | 0 |
| Bronze | 28 | 3 | 0 |
| Silver | 26 | 18 | 9 |
| Gold | 18 | 12 | 4 |

**Fields 🔴 ưu tiên xử lý:**
- `district`, `ward`, `postal_code` → **Bỏ khỏi silver core**, đưa vào enrichment optional
- `soft_skills`, `hard_skills` → Đánh dấu **NLP-derived, optional**
- `work_schedule_type`, `requires_weekend_rotation` → Đánh dấu **best-effort**
- `language_requirements` → Đánh dấu **best-effort**
- `lift_score`, `jaccard_score` (gold) → Đánh dấu **optional gold table**
