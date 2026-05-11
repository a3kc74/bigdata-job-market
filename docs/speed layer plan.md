Dựa trên sơ đồ `architecture.png`, Speed Layer nên được build thành luồng:

**Fake Crawler Streaming → Kafka `jobs_raw` → Spark Structured Streaming → realtime aggregations → Elasticsearch/Kibana + Cassandra/API**

Tài liệu planning trước đó cũng đã xác định phần thiếu chính của repo là Kafka, Structured Streaming, watermark, stateful aggregation, checkpoint và sink Cassandra/Elasticsearch cho realtime layer.

---

## 1. Mục tiêu của Speed Layer

Speed Layer không thay thế Batch Layer, mà xử lý dữ liệu job mới “gần thời gian thực”.

Batch Layer xử lý:

```text
Historical JSONL file
→ HDFS Raw Zone
→ Spark Batch ETL
→ HDFS Bronze/Silver/Gold
→ Elasticsearch/Kibana
```

Speed Layer xử lý:

```text
raw_jobs_batch.jsonl giả lập crawler
→ Kafka jobs_raw
→ Spark Structured Streaming
→ jobs_clean / jobs_dead_letter
→ realtime aggregations
→ Elasticsearch + Cassandra
→ Kibana / API / Grafana
```

Trong demo, `raw_jobs_batch.jsonl` đóng vai trò dữ liệu lịch sử được replay thành streaming event.

---

## 2. Raw format cần giữ từ `raw_jobs_batch.jsonl`

File raw đang có dạng JSON Lines, mỗi dòng là một job posting với envelope như sau:

```json
{
  "source": "topcv",
  "source_url": "...",
  "normalized_source_url": "...",
  "crawl_version": 1,
  "ingest_ts": 1777811661653,
  "event_ts": 1776877200000,
  "job_id": "...",
  "hash_content": "...",
  "payload": {
    "title": "...",
    "company_name": "...",
    "salary": "14 - 15 triệu",
    "location": ["- Hồ Chí Minh: ..."],
    "monthOfExperience": 3,
    "deadline": 1779555599000,
    "occupationalCategory": "Nhân viên",
    "education": "Đại Học trở lên",
    "employmentType": "FULL_TIME",
    "openings": 1,
    "description": "...",
    "requirements": "...",
    "benefits": "...",
    "skillsNeeded": [...],
    "skillsShouldHave": [...],
    "specialty": [...],
    "pageText": "..."
  },
  "quality_flags": {
    "has_salary_info": true,
    "has_location_info": true,
    "has_skills_info": true
  }
}
```

Speed Layer nên giữ nguyên raw event ở Kafka `jobs_raw`, sau đó Spark mới parse và normalize.

---

## 3. Cấu trúc thư mục nên triển khai cho Speed Layer

```text
bigdata-job-market/
├── apps/
│   ├── producer/
│   │   ├── fake_crawler_producer.py
│   │   ├── kafka_admin.py
│   │   └── requirements.txt
│   │
│   ├── stream_etl/
│   │   ├── stream_main.py
│   │   ├── schemas/
│   │   │   └── raw_job_schema.py
│   │   ├── stateful_jobs/
│   │   │   ├── jobs_per_10m.py
│   │   │   ├── top_skills_hourly.py
│   │   │   └── salary_bins_realtime.py
│   │   ├── sinks/
│   │   │   ├── kafka_sink.py
│   │   │   ├── elasticsearch_sink.py
│   │   │   └── cassandra_sink.py
│   │   └── tests/
│   │       ├── test_salary_parser.py
│   │       ├── test_skill_extractor.py
│   │       └── test_stream_schema.py
│
├── shared/
│   ├── utils/
│   │   ├── time_utils.py
│   │   ├── city_normalizer.py
│   │   └── json_utils.py
│   ├── udfs/
│   │   ├── salary_parser.py
│   │   ├── skill_extractor.py
│   │   └── category_normalizer.py
│   ├── quality/
│   │   └── streaming_quality_rules.py
│   └── observability/
│       └── logger.py
│
├── configs/
│   └── speed_layer.dev.yaml
│
├── infra/
│   ├── docker-compose/
│   │   └── docker-compose.speed.yml
│   └── kubernetes/
│       ├── kafka/
│       ├── spark/
│       ├── cassandra/
│       ├── elasticsearch/
│       └── monitoring/
│
├── dashboards/
│   ├── kibana/
│   │   ├── realtime_job_counts.ndjson
│   │   ├── realtime_top_skills.ndjson
│   │   └── realtime_salary_bins.ndjson
│   └── grafana/
│
└── scripts/
    ├── create_kafka_topics.sh
    ├── init_cassandra.cql
    ├── run_fake_crawler.sh
    ├── run_stream_speed_layer.sh
    └── smoke_test_speed_layer.sh
```

---

## 4. Kafka topics

Nên tạo 3 topic chính:

|Topic|Vai trò|Producer|Consumer|
|---|---|---|---|
|`jobs_raw`|Nhận raw event y hệt dữ liệu crawler|`fake_crawler_producer.py`|Spark Structured Streaming|
|`jobs_clean`|Lưu event đã parse, chuẩn hóa, hợp lệ|Spark Streaming|Debug, audit, downstream job|
|`jobs_dead_letter`|Lưu record lỗi JSON/schema/quality|Spark Streaming|Monitoring, replay lỗi|

Cấu hình gợi ý:

```bash
jobs_raw:          partitions=3, retention=7d
jobs_clean:        partitions=3, retention=7d
jobs_dead_letter:  partitions=1, retention=14d
```

Key Kafka nên là:

```text
job_id
```

Vì `job_id` giúp deduplicate và giúp cùng một job đi về cùng partition tương đối ổn định.

---

## 5. Fake crawler streaming

File:

```text
apps/producer/fake_crawler_producer.py
```

Nhiệm vụ:

```text
Đọc raw_jobs_batch.jsonl từng dòng
→ validate JSON
→ gắn thêm stream metadata
→ gửi vào Kafka topic jobs_raw
→ sleep 0.5s / 1s / 2s giữa các record để giả lập crawler realtime
```

Nên có 2 chế độ replay:

### Chế độ 1 — Giữ nguyên `event_ts`

Dùng khi muốn test late data, watermark, dữ liệu cũ.

```bash
python apps/producer/fake_crawler_producer.py \
  --input data/raw/raw_jobs_batch.jsonl \
  --topic jobs_raw \
  --bootstrap-servers localhost:9092 \
  --sleep-ms 1000 \
  --event-time-mode original
```

### Chế độ 2 — Rewrite `event_ts` thành thời gian hiện tại

Dùng cho demo realtime trên Kibana, vì nếu giữ timestamp cũ thì window realtime có thể không hiện như mong muốn.

```bash
python apps/producer/fake_crawler_producer.py \
  --input data/raw/raw_jobs_batch.jsonl \
  --topic jobs_raw \
  --bootstrap-servers localhost:9092 \
  --sleep-ms 1000 \
  --event-time-mode now
```

Khi rewrite, nên giữ lại timestamp gốc:

```json
{
  "event_ts": 1777812000000,
  "original_event_ts": 1776877200000,
  "stream_ingest_ts": 1777812000123
}
```

---

## 6. Spark Structured Streaming main flow

File chính:

```text
apps/stream_etl/stream_main.py
```

Pipeline logic:

```text
1. Read Kafka jobs_raw
2. Parse JSON theo raw_job_schema
3. Tách valid records và invalid records
4. Normalize field:
   - event_time
   - city
   - skills
   - salary_min
   - salary_max
   - salary_bin
   - level
   - category
5. Deduplicate theo job_id/hash_content
6. Ghi clean event vào:
   - Kafka jobs_clean
   - Elasticsearch jobs_realtime_v1
7. Chạy 3 stateful aggregations:
   - jobs per 10 minutes
   - top skills hourly
   - realtime salary bins
8. Ghi aggregate vào:
   - Cassandra
   - Elasticsearch
9. Ghi invalid record vào jobs_dead_letter
10. Checkpoint vào HDFS hoặc local volume
```

---

## 7. Clean schema sau khi Spark parse

Từ raw format, Speed Layer nên normalize thành schema này:

```text
job_id: string
hash_content: string
source: string
source_url: string
event_time: timestamp
ingest_time: timestamp
stream_ingest_time: timestamp

title: string
company_name: string
city: string
location_raw: array<string>
category: string
level: string
employment_type: string
experience_months: int

salary_raw: string
salary_min_million: double
salary_max_million: double
salary_avg_million: double
salary_bin: string
currency: string

skills: array<string>
description: string
requirements: string
benefits: string

quality_flags: struct
```

Ví dụ `salary_bin`:

```text
unknown
negotiable
under_10m
10_20m
20_30m
30_50m
over_50m
```

---

# 8. Làm rõ 3 stateful aggregations

## 8.1. Jobs per 10 minutes

### Ý nghĩa

Metric này trả lời câu hỏi:

```text
Trong mỗi cửa sổ 10 phút, có bao nhiêu job mới được đăng?
```

Dùng cho dashboard realtime như:

```text
Jobs posted in last 10 minutes
Jobs by city in last 10 minutes
Jobs by category in last 10 minutes
```

### Input

Dùng clean stream:

```text
job_id
event_time
source
city
category
company_name
```

### Logic Spark

```python
jobs_per_10m = (
    clean_df
    .withWatermark("event_time", "60 minutes")
    .dropDuplicates(["job_id"])
    .groupBy(
        window("event_time", "10 minutes"),
        col("source"),
        col("city"),
        col("category")
    )
    .agg(
        count("*").alias("job_count"),
        approx_count_distinct("company_name").alias("distinct_company_count")
    )
)
```

### Stateful ở đâu?

Spark giữ state theo key:

```text
window_start + window_end + source + city + category
```

Ví dụ:

```text
2026-05-11 10:00:00 → 2026-05-11 10:10:00
source=topcv
city=Hồ Chí Minh
category=IT - Phần mềm
job_count=12
```

State được giữ đến khi watermark vượt quá window.

### Sink

Cassandra table:

```sql
CREATE TABLE realtime_job_counts_10m (
    bucket_date date,
    window_start timestamp,
    window_end timestamp,
    source text,
    city text,
    category text,
    job_count bigint,
    distinct_company_count bigint,
    updated_at timestamp,
    PRIMARY KEY ((bucket_date), window_start, source, city, category)
);
```

Elasticsearch index:

```text
realtime_job_counts_10m_v1
```

Kibana visualization:

```text
Line chart: job_count by window_start
Bar chart: job_count by city
Metric: jobs in latest 10m window
```

---

## 8.2. Top skills theo giờ

### Ý nghĩa

Metric này trả lời câu hỏi:

```text
Trong mỗi giờ, kỹ năng nào đang được tuyển nhiều nhất?
```

Ví dụ:

```text
Top skills 10:00–11:00
1. Python: 32 jobs
2. SQL: 25 jobs
3. Java: 19 jobs
```

### Input

Dùng field:

```text
skillsNeeded
skillsShouldHave
description
requirements
```

Nên tạo field chuẩn:

```text
skills: array<string>
```

Nguồn skill:

```text
skills = union(skillsNeeded, skillsShouldHave, extracted_skills_from_text)
```

Sau đó normalize:

```text
"python" → "Python"
"py" → "Python"
"javascript" → "JavaScript"
"js" → "JavaScript"
```

### Logic Spark

```python
skill_events = (
    clean_df
    .withWatermark("event_time", "2 hours")
    .dropDuplicates(["job_id"])
    .select(
        window("event_time", "1 hour").alias("time_window"),
        explode("skills").alias("skill"),
        "job_id",
        "city",
        "category"
    )
)

skill_counts_hourly = (
    skill_events
    .groupBy(
        col("time_window"),
        col("skill")
    )
    .agg(
        countDistinct("job_id").alias("job_count")
    )
)
```

### Vì sao đây là stateful aggregation?

Spark phải giữ state theo:

```text
window_start + window_end + skill
```

Mỗi khi job mới vào, Spark cập nhật count cho skill tương ứng.

### Tính Top N

Không nên cố rank trực tiếp trong streaming query chính. Nên dùng `foreachBatch`:

```python
def write_top_skills(batch_df, batch_id):
    ranked = (
        batch_df
        .withColumn("window_start", col("time_window.start"))
        .withColumn("window_end", col("time_window.end"))
        .withColumn(
            "rank",
            row_number().over(
                Window.partitionBy("window_start")
                .orderBy(desc("job_count"))
            )
        )
        .filter(col("rank") <= 10)
    )

    write_to_cassandra(ranked, "realtime_top_skills_hourly")
    write_to_elasticsearch(ranked, "realtime_top_skills_hourly_v1")
```

### Sink

Cassandra full count table:

```sql
CREATE TABLE realtime_skill_counts_hourly (
    bucket_date date,
    window_start timestamp,
    window_end timestamp,
    skill text,
    job_count bigint,
    updated_at timestamp,
    PRIMARY KEY ((bucket_date, window_start), skill)
);
```

Cassandra top N table:

```sql
CREATE TABLE realtime_top_skills_hourly (
    bucket_date date,
    window_start timestamp,
    window_end timestamp,
    rank int,
    skill text,
    job_count bigint,
    updated_at timestamp,
    PRIMARY KEY ((bucket_date, window_start), rank)
);
```

Elasticsearch index:

```text
realtime_skill_counts_hourly_v1
realtime_top_skills_hourly_v1
```

Kibana visualization:

```text
Horizontal bar chart: Top 10 skills in latest hour
Heatmap: skill x hour
Line chart: Python/Java/SQL trend by hour
```

---

## 8.3. Realtime salary bins

### Ý nghĩa

Metric này trả lời câu hỏi:

```text
Realtime các job đang rơi vào khoảng lương nào?
```

Ví dụ:

```text
10–20 triệu: 30 jobs
20–30 triệu: 18 jobs
30–50 triệu: 9 jobs
Thoả thuận: 20 jobs
```

### Input

Từ raw:

```text
payload.salary
payload.location
payload.occupationalCategory
payload.monthOfExperience
```

Ví dụ salary raw:

```text
"14 - 15 triệu"
"30 - 40 triệu"
"Thoả thuận"
```

### Normalize salary

Tạo UDF hoặc function:

```text
"14 - 15 triệu"
→ salary_min_million = 14
→ salary_max_million = 15
→ salary_avg_million = 14.5
→ salary_bin = "10_20m"

"30 - 40 triệu"
→ salary_bin = "30_50m"

"Thoả thuận"
→ salary_bin = "negotiable"
```

### Logic Spark

```python
salary_bins = (
    clean_df
    .withWatermark("event_time", "2 hours")
    .dropDuplicates(["job_id"])
    .groupBy(
        window("event_time", "1 hour"),
        col("city"),
        col("level"),
        col("salary_bin")
    )
    .agg(
        count("*").alias("job_count"),
        avg("salary_min_million").alias("avg_salary_min_million"),
        avg("salary_max_million").alias("avg_salary_max_million"),
        expr("percentile_approx(salary_avg_million, 0.5)").alias("median_salary_million")
    )
)
```

### Stateful ở đâu?

Spark giữ state theo:

```text
window_start + window_end + city + level + salary_bin
```

Ví dụ:

```text
window=10:00–11:00
city=Hà Nội
level=Nhân viên
salary_bin=20_30m
job_count=15
```

### Sink

Cassandra table:

```sql
CREATE TABLE realtime_salary_bins_hourly (
    bucket_date date,
    window_start timestamp,
    window_end timestamp,
    city text,
    level text,
    salary_bin text,
    job_count bigint,
    avg_salary_min_million double,
    avg_salary_max_million double,
    median_salary_million double,
    updated_at timestamp,
    PRIMARY KEY ((bucket_date, window_start), city, level, salary_bin)
);
```

Elasticsearch index:

```text
realtime_salary_bins_hourly_v1
```

Kibana visualization:

```text
Stacked bar chart: salary_bin by hour
Pie chart: latest salary distribution
Filter: city, level, category
```

---

## 9. Watermark, dedup, checkpoint

Speed Layer nên có 3 cơ chế bắt buộc:

### Watermark

Dùng để xử lý late data.

```python
.withWatermark("event_time", "60 minutes")
```

Ý nghĩa:

```text
Spark chấp nhận record trễ tối đa 60 phút so với event_time.
Record đến quá trễ sẽ bị drop khỏi aggregation.
```

### Dedup

Dùng để tránh một job bị tính nhiều lần.

```python
.dropDuplicates(["job_id"])
```

Hoặc chặt hơn:

```python
.dropDuplicates(["job_id", "hash_content"])
```

Gợi ý:

```text
job_id: chống trùng cùng một job
hash_content: phát hiện cùng job nhưng nội dung thay đổi
```

### Checkpoint

Mỗi streaming query cần checkpoint riêng:

```text
/checkpoints/speed/jobs_clean
/checkpoints/speed/jobs_per_10m
/checkpoints/speed/top_skills_hourly
/checkpoints/speed/salary_bins_hourly
```

Không dùng chung checkpoint cho nhiều query.

---

## 10. Elasticsearch index nên có

Trong sơ đồ, Serving Layer đang là Elasticsearch + Kibana. Speed Layer nên ghi các index sau:

```text
jobs_realtime_v1
realtime_job_counts_10m_v1
realtime_skill_counts_hourly_v1
realtime_top_skills_hourly_v1
realtime_salary_bins_hourly_v1
stream_dead_letter_v1
```

`jobs_realtime_v1` dùng cho search/filter job mới.

Các index aggregate dùng cho dashboard.

---

## 11. Cassandra có nên dùng không?

Trong ảnh hiện tại chưa vẽ Cassandra ở Serving Layer, nhưng trong cấu trúc repo có `infra/kubernetes/cassandra/`, nên nên thêm Cassandra song song với Elasticsearch.

Vai trò nên tách như sau:

```text
Elasticsearch:
- search full-text
- filter job
- Kibana dashboard
- exploratory analytics

Cassandra:
- serving query nhanh theo pattern cố định
- API realtime
- lưu aggregate đã tính sẵn
```

Ví dụ API đọc Cassandra:

```text
GET /api/realtime/jobs-per-10m?city=Hà Nội
GET /api/realtime/top-skills?window=latest
GET /api/realtime/salary-bins?city=Hồ Chí Minh
```

---

## 12. Kế hoạch triển khai theo từng bước

### Giai đoạn 1 — Infra local cho Speed Layer

Tạo:

```text
infra/docker-compose/docker-compose.speed.yml
```

Services cần có:

```text
Kafka
Kafka UI
Spark master/worker
Elasticsearch
Kibana
Cassandra
Prometheus
Grafana
```

Scripts:

```text
scripts/create_kafka_topics.sh
scripts/init_cassandra.cql
scripts/smoke_test_speed_layer.sh
```

Kết quả cần đạt:

```text
docker compose up chạy được Kafka + Spark + ES + Kibana + Cassandra
Kafka có đủ 3 topic: jobs_raw, jobs_clean, jobs_dead_letter
Cassandra có đủ bảng realtime
```

---

### Giai đoạn 2 — Fake crawler producer

Tạo:

```text
apps/producer/fake_crawler_producer.py
```

Chức năng:

```text
Đọc raw_jobs_batch.jsonl
Gửi từng dòng vào Kafka jobs_raw
Có sleep interval
Có option loop/replay
Có option rewrite event_ts thành now
Có logging số record đã gửi
```

Command:

```bash
bash scripts/run_fake_crawler.sh
```

Kết quả cần đạt:

```text
Kafka UI thấy message vào topic jobs_raw liên tục
Message key = job_id
Message value = raw JSON
```

---

### Giai đoạn 3 — Spark parse + clean stream

Tạo:

```text
apps/stream_etl/stream_main.py
apps/stream_etl/schemas/raw_job_schema.py
shared/quality/streaming_quality_rules.py
```

Chức năng:

```text
Read jobs_raw
Parse JSON
Validate required fields
Convert event_ts → event_time
Normalize city, skills, salary
Tách valid/invalid
Ghi valid vào jobs_clean
Ghi invalid vào jobs_dead_letter
```

Kết quả cần đạt:

```text
jobs_clean có record đã chuẩn hóa
jobs_dead_letter có record lỗi nếu cố tình bơm JSON sai
Elasticsearch jobs_realtime_v1 có job mới
```

---

### Giai đoạn 4 — Jobs per 10 minutes

Tạo:

```text
apps/stream_etl/stateful_jobs/jobs_per_10m.py
```

Chức năng:

```text
Window 10 phút
Watermark 60 phút
Dedup job_id
Group by source, city, category
Count job_count
Count distinct company
Sink Cassandra + Elasticsearch
```

Dashboard:

```text
Realtime jobs per 10 minutes
Jobs by city
Jobs by category
```

---

### Giai đoạn 5 — Top skills hourly

Tạo:

```text
apps/stream_etl/stateful_jobs/top_skills_hourly.py
```

Chức năng:

```text
Extract skills
Explode skills
Window 1 giờ
Count distinct job_id by skill
Rank top 10 trong foreachBatch
Sink Cassandra + Elasticsearch
```

Dashboard:

```text
Top 10 skills latest hour
Skill trend by hour
Skill heatmap
```

---

### Giai đoạn 6 — Salary bins realtime

Tạo:

```text
apps/stream_etl/stateful_jobs/salary_bins_realtime.py
shared/udfs/salary_parser.py
```

Chức năng:

```text
Parse salary raw
Normalize salary_min, salary_max, salary_avg
Assign salary_bin
Window 1 giờ
Group by city, level, salary_bin
Sink Cassandra + Elasticsearch
```

Dashboard:

```text
Salary distribution latest hour
Salary bins by city
Salary bins by level
```

---

### Giai đoạn 7 — API + dashboard

Tạo:

```text
apps/api/routers/realtime.py
```

Endpoints:

```text
GET /realtime/jobs-per-10m
GET /realtime/top-skills-hourly
GET /realtime/salary-bins
GET /realtime/health
```

Kibana dashboard:

```text
dashboards/kibana/realtime_speed_layer.ndjson
```

Grafana dashboard:

```text
Kafka lag
Spark records/sec
Dead-letter count
Streaming query latency
```

---

## 13. Thứ tự chạy demo end-to-end

```bash
# 1. Start infra
docker compose -f infra/docker-compose/docker-compose.speed.yml up -d

# 2. Create Kafka topics
bash scripts/create_kafka_topics.sh

# 3. Init Cassandra
bash scripts/init_cassandra.sh

# 4. Start Spark streaming job
bash scripts/run_stream_speed_layer.sh

# 5. Start fake crawler
bash scripts/run_fake_crawler.sh

# 6. Open dashboard
# Kafka UI: check jobs_raw / jobs_clean / jobs_dead_letter
# Kibana: check realtime dashboards
# Grafana: check infra metrics
```

---

## 14. Deliverables cuối cùng của Speed Layer

Nhóm nên chốt các deliverables sau:

|Deliverable|File/thư mục|
|---|---|
|Fake crawler producer|`apps/producer/fake_crawler_producer.py`|
|Kafka topic scripts|`scripts/create_kafka_topics.sh`|
|Spark streaming main|`apps/stream_etl/stream_main.py`|
|Raw schema|`apps/stream_etl/schemas/raw_job_schema.py`|
|Jobs per 10m aggregation|`apps/stream_etl/stateful_jobs/jobs_per_10m.py`|
|Top skills hourly aggregation|`apps/stream_etl/stateful_jobs/top_skills_hourly.py`|
|Salary bins realtime aggregation|`apps/stream_etl/stateful_jobs/salary_bins_realtime.py`|
|Cassandra schema|`scripts/init_cassandra.cql`|
|ES/Kibana dashboard|`dashboards/kibana/`|
|Config|`configs/speed_layer.dev.yaml`|
|Run scripts|`scripts/run_fake_crawler.sh`, `scripts/run_stream_speed_layer.sh`|
|Test|`apps/stream_etl/tests/`|

---

## 15. Tóm tắt logic 3 metric chính

```text
jobs per 10 minutes
= count distinct job_id
  grouped by 10-minute event_time window
  optionally grouped by city/category/source

top skills hourly
= explode skills
  count distinct job_id per skill
  grouped by 1-hour event_time window
  rank top 10 in foreachBatch

realtime salary bins
= parse salary_raw
  map salary_avg to salary_bin
  count jobs per salary_bin
  grouped by 1-hour event_time window, city, level
```

Ba metric này đủ để chứng minh Speed Layer có:

```text
Kafka ingestion
Structured Streaming
event-time processing
watermark
deduplication
stateful aggregation
checkpoint
serving sink
realtime dashboard
```