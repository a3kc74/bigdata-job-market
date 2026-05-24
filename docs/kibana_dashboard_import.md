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

Nếu sau này speed dashboard đã xử lý lại field thời gian ổn định, có thể thêm:

- `job_market_speed_dashboard.ndjson`

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

Nếu chưa có index `gold-jobs-flat` thì cần chạy batch pipeline trước. Dashboard vẫn import được nhưng sẽ không có dữ liệu.

## 3. Mở Kibana

Port-forward Kibana:

```powershell
kubectl port-forward -n search svc/kibana 5601:5601
```

Mở trình duyệt:

```text
http://localhost:5601
```

## 4. Import dashboard bằng giao diện Kibana

Vào:

```text
Stack Management -> Saved Objects -> Import
```

Chọn file cần import, ví dụ:

```text
infra/kibana/saved_objects/job_market_batch_dashboards.ndjson
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

## 6. Import bằng command line

Nếu Kibana đang mở ở `http://localhost:5601`, có thể import bằng API.

Import bộ dashboard batch chính:

```powershell
curl.exe -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" `
  -H "kbn-xsrf: true" `
  -F "file=@infra\kibana\saved_objects\job_market_batch_dashboards.ndjson"
```

Import dashboard filter:

```powershell
curl.exe -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" `
  -H "kbn-xsrf: true" `
  -F "file=@infra\kibana\saved_objects\job_market_batch_filter_dashboard.ndjson"
```

## 7. Kiểm tra sau khi import

Vào:

```text
Dashboards
```

Kiểm tra có các dashboard:

- Job Market - 01 Overview
- Job Market - 02 Salary Analysis
- Job Market - 03 Skills & Specialties
- Job Market - 04 Geography & Companies
- Job Market - 05 Tables
- Job Market - Batch Filter View

Nếu dashboard mở ra nhưng không có dữ liệu, kiểm tra:

```powershell
curl.exe "http://localhost:9200/_cat/indices/gold-jobs-flat?v"
```

và kiểm tra time range/filter trong Kibana.

## 8. Lỗi thường gặp

### Dashboard không có dữ liệu

Nguyên nhân thường gặp:

- Elasticsearch chưa có index `gold-jobs-flat`
- Data view chưa trỏ đúng index
- Đang bật filter hoặc time range không phù hợp

Cách kiểm tra:

```powershell
curl.exe "http://localhost:9200/gold-jobs-flat/_count?pretty"
```

### Import báo conflict

Chọn:

```text
Overwrite
```

để dùng bản dashboard trong repo.

### Import báo thiếu data view

File NDJSON có thể thiếu data view. Với dashboard batch, cần có data view:

- `gold-jobs-flat`

Nếu thiếu, tạo thủ công trong Kibana:

```text
Stack Management -> Data Views -> Create data view
```

Thông tin:

- Name: `gold-jobs-flat`
- Index pattern: `gold-jobs-flat`

Nếu Kibana hỏi time field mà không chắc, có thể chọn:

```text
I do not want to use the time filter
```

### Import báo version/migration lỗi

Có thể file NDJSON được export từ Kibana version mới hơn. Cần dùng file NDJSON được export từ đúng Kibana version của project.
