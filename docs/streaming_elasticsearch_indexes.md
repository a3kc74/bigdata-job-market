# Giải thích các Elasticsearch index của speed layer

Tài liệu này giải thích theo cách dễ hiểu:

- Mỗi index của speed layer dùng để làm gì
- Mỗi document trong index đó đại diện cho cái gì
- Vì sao có index dành cho **job thật**
- Và vì sao có index dành cho **thống kê theo thời gian**
- Khi làm dashboard thì nên tách phần nào với phần nào

---

## 1. Ý chính cần hiểu trước

Speed layer hiện ghi ra **2 loại dữ liệu khác nhau** trong Elasticsearch:

### Loại A: Dữ liệu job thật

Mỗi document là **1 job cụ thể**.

Ví dụ:

- Backend Engineer ở HCM
- công ty ABC
- salary 20-30 triệu

Loại này dùng để:

- Search job
- Filter job
- Xem chi tiết job
- Sort theo salary, ngày đăng, city, skill

### Loại B: Dữ liệu thống kê theo khoảng thời gian

Mỗi document không còn là 1 job cụ thể nữa, mà là **kết quả tổng hợp của nhiều job trong một khung thời gian**.

Ví dụ:

- Từ `10:00` đến `10:10` có bao nhiêu job mới
- Trong `1 giờ` vừa qua skill nào xuất hiện nhiều nhất
- Trong `1 giờ` vừa qua salary bin nào xuất hiện nhiều

Loại này dùng để:

- Vẽ chart realtime
- Theo dõi trend
- Làm dashboard kiểu monitoring

---

## 2. `Aggregate theo time window` là gì?

Đây là khái niệm quan trọng nhất.

Giả sử speed layer nhận được 3 job:

- Job A lúc `10:02`
- Job B lúc `10:05`
- Job C lúc `10:12`

Nếu ta aggregate theo window 10 phút, thì:

- Window `10:00 - 10:10` có 2 job
- Window `10:10 - 10:20` có 1 job

Kết quả lưu vào Elasticsearch sẽ không phải là 3 document job riêng lẻ nữa, mà
là các document kiểu:

```json
{
  "window_start": "2026-05-27T10:00:00Z",
  "window_end": "2026-05-27T10:10:00Z",
  "job_count": 2
}
```

Tức là:

- **job-level** = 1 document là 1 job
- **aggregate/time-window** = 1 document là 1 kết quả thống kê của một khoảng thời gian

---

## 3. Speed layer hiện có những index nào?

| Index | Loại dữ liệu | Dùng để làm gì |
| --- | --- | --- |
| `realtime_jobs_v1` | Job-level | Xem và filter job realtime |
| `realtime_job_counts_10m_v1` | Aggregate | Đếm số job theo cửa sổ 10 phút |
| `realtime_skill_counts_hourly_v1` | Aggregate | Đếm số job theo skill trong từng giờ |
| `realtime_top_skills_hourly_v1` | Aggregate | Lấy top skill theo từng giờ |
| `realtime_salary_bins_hourly_v1` | Aggregate | Thống kê phân bố salary theo từng giờ |

---

## 4. `realtime_jobs_v1` là gì?

Đây là index chứa **job realtime đã clean**.

### 1 document trong index này đại diện cho gì?

- **1 job cụ thể**

Ví dụ dễ hình dung:

```json
{
  "job_id": "abc123",
  "title": "Backend Engineer",
  "company_name": "ABC Corp",
  "primary_city": "Ho Chi Minh",
  "employmentType": "FULL_TIME",
  "salary_min_vnd": 20000000,
  "salary_max_vnd": 30000000
}
```

### Index này dùng khi nào?

Khi bạn muốn:

- hiện danh sách job
- search theo title
- filter theo city, skill, loại hình, salary
- xem job realtime vừa crawl được

### `realtime_jobs_v1` được tạo ở đâu?

Nó được ghi tại:

- [apps/stream_etl/sinks/elasticsearch_sink.py](d:/workspace/Repo/bigdata-job-market/apps/stream_etl/sinks/elasticsearch_sink.py:12)

Tên index mặc định được set ở:

- `ES_INDEX_JOBS = "realtime_jobs_v1"` trong [elasticsearch_sink.py](d:/workspace/Repo/bigdata-job-market/apps/stream_etl/sinks/elasticsearch_sink.py:12)

Việc gọi writer này diễn ra trong:

- [apps/stream_etl/stream_main.py](d:/workspace/Repo/bigdata-job-market/apps/stream_etl/stream_main.py:147)

Và manifest chạy speed stream truyền biến môi trường:

- `spark.kubernetes.driverEnv.ES_INDEX_JOBS=realtime_jobs_v1`
- trong [infra/spark/speed-stream-es-job.yaml](d:/workspace/Repo/bigdata-job-market/infra/spark/speed-stream-es-job.yaml:120)

Nói ngắn gọn:

- `realtime_jobs_v1` là index job-level của speed
- nó có thật trong code, không phải chỉ có trong docs

### Các field chính của `realtime_jobs_v1`

Không cần nhớ hết. Chỉ cần chia nhóm:

**Nhóm định danh / thời gian**

- `job_id`
- `source`
- `source_url`
- `date_posted`
- `deadline`
- `ingest_date`
- `event_time`
- `ingest_time`
- `stream_ingest_time`

**Nhóm mô tả job**

- `title`
- `company_name`
- `description`
- `requirements`
- `benefits`

**Nhóm filter**

- `primary_city`
- `company_field`
- `occupationalCategory`
- `employmentType`
- `education`
- `skills`
- `specialty`
- `has_remote`
- `experience_required`
- `salary_is_negotiable`
- `schedule_type`
- `is_weekend_free`

**Nhóm salary**

- `salary`
- `salary_min_vnd`
- `salary_max_vnd`
- `salary_display_avg_vnd`
- `salary_predicted_avg_vnd`
- `salary_source`

**Lưu ý về contract hiện tại**

`realtime_jobs_v1` không còn cố giữ mọi field của Gold batch. Các field chỉ có
ý nghĩa khi batch parse được từ `json_ld` như `company_id`, `company_logo`,
`company_url`, `work_country` đã được bỏ khỏi speed schema vì nguồn speed hiện
không có dữ liệu đủ tin cậy cho chúng.

---

## 5. `realtime_job_counts_10m_v1` là gì?

Đây là index aggregate theo **cửa sổ 10 phút**.

### 1 document trong index này đại diện cho gì?

- số lượng job trong **một khung 10 phút**
- theo một số chiều như:
  - `source`
  - `primary_city`
  - `company_field`

Ví dụ:

```json
{
  "window_start": "2026-05-27T10:00:00Z",
  "window_end": "2026-05-27T10:10:00Z",
  "source": "topcv",
  "primary_city": "Ho Chi Minh",
  "company_field": "Software",
  "job_count": 18
}
```

### Dùng khi nào?

Khi bạn muốn vẽ:

- biểu đồ số job theo thời gian
- trend job tăng giảm theo 10 phút

### Không dùng khi nào?

Không dùng index này để:

- hiện danh sách job
- click vào từng job
- xem lương của từng job

Vì nó không còn dữ liệu từng job nữa.

---

## 6. `realtime_skill_counts_hourly_v1` và `realtime_top_skills_hourly_v1` là gì?

Hai index này đều liên quan đến skill theo từng giờ.

### `realtime_skill_counts_hourly_v1`

Lưu số lượng job theo từng skill trong từng giờ.

Ví dụ:

```json
{
  "window_start": "2026-05-27T10:00:00Z",
  "window_end": "2026-05-27T11:00:00Z",
  "skill": "Python",
  "job_count": 27
}
```

### `realtime_top_skills_hourly_v1`

Lưu danh sách top skill đã xếp hạng sẵn cho từng giờ.

Ví dụ:

```json
{
  "window_start": "2026-05-27T10:00:00Z",
  "rank": 1,
  "skill": "Python",
  "job_count": 27
}
```

### Dùng khi nào?

Khi bạn muốn vẽ:

- top skill theo giờ
- skill hot trong giờ gần nhất

---

## 7. `realtime_salary_bins_hourly_v1` là gì?

Đây là index aggregate salary theo **cửa sổ 1 giờ**.

### 1 document trong index này đại diện cho gì?

- thống kê salary của nhiều job trong 1 giờ
- theo:
  - `primary_city`
  - `occupationalCategory`
  - `salary_bin`

Ví dụ:

```json
{
  "window_start": "2026-05-27T10:00:00Z",
  "window_end": "2026-05-27T11:00:00Z",
  "primary_city": "Ha Noi",
  "occupationalCategory": "Nhan vien",
  "salary_bin": "20_30m",
  "job_count": 14
}
```

### Dùng khi nào?

Khi bạn muốn vẽ:

- phân bố mức lương theo giờ
- giờ này nhóm salary nào đang xuất hiện nhiều

---

## 8. Vì sao dashboard nên tách 2 phần?

Vì speed layer đang tạo ra **2 loại dữ liệu có ý nghĩa khác nhau**.

## Phần A: dashboard / màn hình xem job

Dùng các index job-level:

- `realtime_jobs_v1`
- hoặc batch index như `gold-jobs-flat`

Phần này để:

- search
- filter
- xem danh sách job
- xem chi tiết job

## Phần B: dashboard / màn hình xem thống kê realtime

Dùng các index aggregate:

- `realtime_job_counts_10m_v1`
- `realtime_skill_counts_hourly_v1`
- `realtime_top_skills_hourly_v1`
- `realtime_salary_bins_hourly_v1`

Phần này để:

- xem trend theo thời gian
- xem chart
- xem monitoring gần realtime

### Vì sao không nên trộn tất cả vào 1 loại dashboard?

Vì câu hỏi nghiệp vụ khác nhau:

**Dashboard kiểu job-level trả lời:**

- có job nào ở HCM không?
- công ty nào đang tuyển?
- job Python lương 20-30 triệu ở đâu?

**Dashboard kiểu aggregate trả lời:**

- 10 phút vừa rồi có bao nhiêu job mới?
- giờ này skill nào nổi lên?
- salary bin nào xuất hiện nhiều nhất trong 1 giờ gần đây?

Nói ngắn gọn:

- **job-level dashboard** = xem từng job
- **aggregate dashboard** = xem xu hướng và thống kê

Không nhất thiết phải tách thành 2 hệ thống Kibana hoàn toàn khác nhau, nhưng
về mặt data view và visualization thì nên coi là **2 phần khác nhau**.

---

## 9. Batch và speed khác nhau ở chỗ nào?

### Batch

Batch hiện đẩy dữ liệu kiểu:

- **1 document = 1 job**

Ví dụ:

- `gold-jobs-flat`

Batch phù hợp để:

- search/filter trên snapshot dữ liệu đã xử lý đầy đủ
- làm dashboard phân tích trên dữ liệu canonical hơn

### Speed

Speed hiện có cả:

- **job-level index**: `realtime_jobs_v1`
- **aggregate indexes**: các index theo time window

Vì vậy nếu ai nói:

- “speed chỉ có aggregate”

thì là **không đúng hoàn toàn**.

Đúng phải là:

- speed vừa có job-level, vừa có aggregate

---

## 10. Nếu chỉ cần nhớ 3 câu

1. `realtime_jobs_v1` là nơi lưu **từng job realtime**
2. các index còn lại như `realtime_job_counts_10m_v1`, `realtime_salary_bins_hourly_v1` là **thống kê theo time window**
3. dashboard nên tách thành:
   - phần xem **job**
   - phần xem **trend/thống kê**

---

## 11. Các writer tương ứng trong code

| Index | Writer |
| --- | --- |
| `realtime_jobs_v1` | `apps/stream_etl/sinks/elasticsearch_sink.py` |
| `realtime_job_counts_10m_v1` | `apps/stream_etl/sinks/jobs_per_10m_sink.py` |
| `realtime_skill_counts_hourly_v1` | `apps/stream_etl/sinks/top_skills_hourly_sink.py` |
| `realtime_top_skills_hourly_v1` | `apps/stream_etl/sinks/top_skills_hourly_sink.py` |
| `realtime_salary_bins_hourly_v1` | `apps/stream_etl/sinks/salary_bins_realtime_sink.py` |

---

## 12. Dead-letter

Pipeline hiện tại còn ghi record lỗi vào Kafka topic:

- `jobs_dead_letter`

Nó chưa phải là index Elasticsearch mặc định của speed layer, nhưng nếu sau này
mirror sang Elasticsearch thì nên coi nó là luồng riêng, không trộn với
`realtime_jobs_v1`.
