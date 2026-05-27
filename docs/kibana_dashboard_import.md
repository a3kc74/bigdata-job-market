# Hướng dẫn import Kibana Dashboard từ file NDJSON

Tài liệu này hướng dẫn cách import dashboard Kibana từ các file `.ndjson` đã có sẵn trong repo.

## 1. Các file dashboard có sẵn

Các file Saved Objects được lưu tại:

```text
infra/kibana/saved_objects/
```

Các file hiện dùng:

### `job_market_batch_dashboards.ndjson`

Bộ dashboard batch chính, gồm các dashboard:

- Job Market - 01 Overview
- Job Market - 02 Salary Analysis
- Job Market - 03 Skills & Specialties
- Job Market - 04 Geography & Companies
- Job Market - 05 Tables

### `job_market_batch_filter_dashboard.ndjson`

Dashboard filter riêng cho batch:

- Job Market - Batch Filter View

### `job_market_realtime_ml_dashboard.ndjson`

Dashboard realtime cho speed layer và ML salary prediction:

- Job Market - Realtime ML Dashboard

Dashboard này đọc dữ liệu từ các realtime index:

- `realtime_jobs_v1`
- `realtime_job_counts_10m_v1`
- `realtime_salary_bins_hourly_v1`
- `realtime_top_skills_hourly_v1`
- `realtime_skill_counts_hourly_v1`

Dashboard realtime dùng để demo luồng:

```text
TopCV crawler -> Kafka jobs_raw -> Spark Structured Streaming -> Elasticsearch -> Kibana
```

## 2. Điều kiện trước khi import

Kibana và Elasticsearch phải đang chạy.

Mở Elasticsearch để kiểm tra index:

```powershell
kubectl port-forward -n search svc/elasticsearch 9200:9200
```

Mở terminal khác và kiểm tra batch index:

```powershell
curl.exe "http://localhost:9200/_cat/indices/gold-jobs-flat?v"
curl.exe "http://localhost:9200/gold-jobs-flat/_count?pretty"
```

Nếu chưa có index `gold-jobs-flat` thì cần chạy batch pipeline trước. Dashboard batch vẫn import được nhưng sẽ không có dữ liệu.

Với realtime dashboard, kiểm tra các realtime index:

```powershell
curl.exe "http://localhost:9200/_cat/indices/realtime_*?v"
curl.exe "http://localhost:9200/realtime_jobs_v1/_count?pretty"
curl.exe "http://localhost:9200/realtime_job_counts_10m_v1/_count?pretty"
curl.exe "http://localhost:9200/realtime_salary_bins_hourly_v1/_count?pretty"
curl.exe "http://localhost:9200/realtime_top_skills_hourly_v1/_count?pretty"
curl.exe "http://localhost:9200/realtime_skill_counts_hourly_v1/_count?pretty"
```

Nếu realtime index chưa có dữ liệu, cần đảm bảo Spark Streaming đang chạy:

```powershell
kubectl get pods -n spark -l spark-app-name=speed-stream-es -o wide
```

Nếu Spark Streaming chưa chạy, trigger DAG:

```text
job_market_speed_layer_bootstrap
```

Khuyến nghị config khi chỉ cần bật lại stream:

```json
{
  "reset_checkpoint": false,
  "run_real_crawler": false
}
```

Sau khi Spark Streaming `Running`, chạy crawler speed:

```text
job_market_speed_real_crawler
```

DAG này chạy định kỳ để crawl TopCV và đẩy dữ liệu vào Kafka. Spark Streaming sẽ tự consume Kafka và ghi vào Elasticsearch.

## 3. Mở Kibana

Port-forward Kibana:

```powershell
kubectl port-forward -n search svc/kibana 5601:5601
```

Mở trình duyệt:

```text
http://localhost:5601
```

Nếu service Kibana có tên khác, kiểm tra bằng:

```powershell
kubectl get svc -n search
```

rồi port-forward đúng service.

## 4. Import dashboard bằng giao diện Kibana

Vào:

```text
Stack Management -> Saved Objects -> Import
```

Chọn file cần import, ví dụ batch:

```text
infra/kibana/saved_objects/job_market_batch_dashboards.ndjson
```

Hoặc realtime:

```text
infra/kibana/saved_objects/job_market_realtime_ml_dashboard.ndjson
```

Khi Kibana hỏi conflict, chọn:

```text
Overwrite
```

Sau khi import xong, vào:

```text
Dashboards
```

và mở dashboard cần dùng.

## 5. Import dashboard filter

Nếu muốn import dashboard filter riêng:

```text
Stack Management -> Saved Objects -> Import
```

Chọn file:

```text
infra/kibana/saved_objects/job_market_batch_filter_dashboard.ndjson
```

Nếu có conflict, chọn:

```text
Overwrite
```

Dashboard sau khi import:

- Job Market - Batch Filter View

## 6. Import realtime dashboard

Import file:

```text
infra/kibana/saved_objects/job_market_realtime_ml_dashboard.ndjson
```

Dashboard sau khi import:

- Job Market - Realtime ML Dashboard

Dashboard realtime nên chỉnh time range theo mục đích kiểm tra/demo:

```text
Last 10 minutes / Last 20 minutes
```

Khi muốn kiểm tra đúng dữ liệu vừa được crawler speed đẩy vào Kafka và Spark Streaming vừa index vào Elasticsearch.

```text
Last 1 hour / Last 12 hours
```

Dùng khi muốn xem đầy đủ hơn các panel hourly, salary bucket, top skills và ML prediction, vì các aggregate này có thể ít dữ liệu nếu crawler mới chỉ lấy được vài job.

Khuyến nghị khi demo live:

- Time range: `Last 10 minutes` hoặc `Last 20 minutes`
- Refresh: `10 seconds`

Nếu các panel hourly/ML bị trống do quá ít job mới, tăng time range lên:

- `Last 1 hour`
- `Last 12 hours`
