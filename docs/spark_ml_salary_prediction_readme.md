# Spark ML Salary Prediction

Tài liệu này mô tả riêng nghiệp vụ Spark ML dự đoán lương trong project Big
Data Job Market. Mục tiêu là xử lý nhóm job không có lương công khai rõ ràng,
đồng thời vẫn giữ nguyên mọi thông tin lương thật crawl được.

## 1. Mục Tiêu Nghiệp Vụ

Một số job trên TopCV ghi lương là `Thỏa thuận`, hoặc ghi lương một phía như
`Lớn hơn 40 triệu`, `Tới 40 triệu`. Nếu chỉ nhìn chuỗi `salary` gốc thì
dashboard/API rất khó sort, filter, thống kê theo lương.

Bài toán Spark ML:

```text
X = title, skills, experience, location, company, remote
y = salary thật của những job có lương công khai
f(x) = model dự đoán lương cho job thật sự chưa có min/max lương
```

Điểm quan trọng: hệ thống không thay thế `salary` gốc. Hệ thống tạo thêm các
cột phục vụ search/dashboard:

- `salary_min_vnd`, `salary_max_vnd`: lương thật parse được từ raw/json_ld.
- `salary_predicted_*`: lương do model dự đoán, chỉ có khi cần prediction.
- `salary_display_*`: lương cuối cùng nên dùng cho Kibana/API.
- `salary_source`: nguồn tạo ra lương hiển thị.
- `salary_prediction_applied`: job có dùng model hay không.

## 2. Vị Trí Trong Lambda Architecture

```text
Batch layer:

Raw JSONL
-> Bronze
-> Silver
-> train Spark ML salary model từ toàn bộ Silver
-> Silver to Gold, apply model
-> Elasticsearch index gold-jobs-flat

Speed layer:

Kafka jobs_raw
-> Spark Structured Streaming
-> load model đã train từ batch
-> apply model vào realtime clean jobs
-> Elasticsearch index realtime_jobs_v1
```

Batch layer chịu trách nhiệm train model. Speed layer không train model, chỉ
load model ở path `latest` và score realtime records.

## 3. Nguyên Tắc Production

Mỗi lần batch có dữ liệu mới:

```text
Dữ liệu mới vào Silver
-> train lại model bằng toàn bộ Silver cũ + mới
-> overwrite model cũ tại /models/salary_prediction/latest
-> Silver to Gold load model mới
-> Gold to Elasticsearch ghi lại index
```

Job train model không dùng `--date`. Lý do:

- Train theo một ngày làm data quá ít và lệch phân phối.
- Salary prediction cần học pattern từ lịch sử job cũ + job mới.
- Production chỉ cần một model mới nhất ở path cố định `latest`.

Path model:

```text
hdfs://hdfs-namenode.hdfs.svc:9000/models/salary_prediction/latest
```

Path metrics:

```text
hdfs://hdfs-namenode.hdfs.svc:9000/models/salary_prediction/metrics/latest
```

## 4. Cây Thư Mục Liên Quan

```text
bigdata-job-market/
|-- apps/
|   |-- ml/
|   |   |-- __init__.py
|   |   `-- salary_prediction.py
|   |
|   |-- batch/
|   |   `-- jobs/
|   |       |-- bronze_to_silver.py
|   |       |-- train_salary_model.py
|   |       |-- silver_to_gold.py
|   |       `-- gold_to_elasticsearch.py
|   |
|   `-- stream_etl/
|       |-- normalizers.py
|       |-- transform.py
|       `-- stream_main.py
|
|-- configs/
|   `-- settings.py
|
|-- infra/
|   |-- airflow/dags/job_market_batch_pipeline.py
|   `-- spark/
|       |-- Dockerfile
|       |-- salary-model-train-cronjob.yaml
|       `-- speed-stream-es-job.yaml
|
|-- data/gold/gold_data_format.md
`-- docs/
    |-- salary_prediction_runbook.md
    `-- spark_ml_salary_prediction_readme.md
```

## 5. Ý Nghĩa Từng File

| File | Mục đích |
|---|---|
| `apps/ml/salary_prediction.py` | File lõi: tạo feature, tạo label, build GBTRegressor pipeline, load model, score prediction, sinh `salary_display_*` và `salary_source`. |
| `apps/batch/jobs/bronze_to_silver.py` | Parse JSON-LD và salary text để tạo `salary_min_vnd`, `salary_max_vnd`, `salary_is_negotiable`. Xử lý cả lương một cận. |
| `apps/batch/jobs/train_salary_model.py` | Train model bằng toàn bộ Silver, ghi model vào path `latest`, ghi metrics vào `metrics/latest`. |
| `apps/batch/jobs/silver_to_gold.py` | Load model, apply prediction, chọn cột Gold phục vụ Elasticsearch/Kibana. |
| `apps/batch/jobs/gold_to_elasticsearch.py` | Ghi Gold Parquet vào index `gold-jobs-flat`. |
| `apps/stream_etl/normalizers.py` | Parse salary text trong speed layer, gồm range/min-only/max-only. |
| `apps/stream_etl/transform.py` | Tạo clean stream schema, thêm salary VND fields và `has_remote`. |
| `apps/stream_etl/stream_main.py` | Load model một lần khi stream start, apply model cho realtime jobs. |
| `configs/settings.py` | Chứa `SALARY_MODEL_PATH`, `SALARY_MODEL_METRICS_PATH`. |
| `infra/spark/Dockerfile` | Build Spark image có `apps/ml`, batch jobs, stream jobs và dependency như `numpy`. |
| `infra/spark/salary-model-train-cronjob.yaml` | Kubernetes CronJob template để train model. |
| `infra/spark/speed-stream-es-job.yaml` | Kubernetes Job chạy speed layer và trỏ tới model `latest`. |
| `infra/airflow/dags/job_market_batch_pipeline.py` | DAG batch, có task `train_salary_model` giữa `bronze_to_silver` và `silver_to_gold`. |
| `data/gold/gold_data_format.md` | Tài liệu schema Gold, gồm các cột Spark ML. |
| `docs/salary_prediction_runbook.md` | Runbook chạy batch/speed từ đầu đến cuối. |

## 6. Input Feature Của Model

| Feature | Nguồn batch | Nguồn speed | Ý nghĩa |
|---|---|---|---|
| `title` | Silver `title` | Clean stream `title` | Tên job. |
| `skills` | Silver `skills` | Clean stream `skills` | Kỹ năng yêu cầu. |
| `experience` | Silver `monthOfExperience` | Clean stream `experience_months` | Số tháng kinh nghiệm. |
| `location` | Silver `location` | Clean stream `location_raw` | Địa điểm làm việc. |
| `company` | Silver `company_name` | Clean stream `company_name` | Tên công ty. |
| `remote` | Silver `has_remote` | Clean stream `has_remote` | Job remote hay không. |

Feature engineering nằm trong `apps/ml/salary_prediction.py` để batch và speed
dùng chung một chuẩn.

## 7. Label Training

Label `y` là salary thật theo VND/tháng.

Model dùng các job có ít nhất một cận lương thật:

```text
Nếu có cả salary_min_vnd và salary_max_vnd:
    y = (salary_min_vnd + salary_max_vnd) / 2

Nếu chỉ có salary_min_vnd:
    y = salary_min_vnd

Nếu chỉ có salary_max_vnd:
    y = salary_max_vnd

Nếu cả salary_min_vnd và salary_max_vnd đều null:
    không dùng để train
```

Điểm rất quan trọng: nếu `salary = "Thỏa thuận"` nhưng JSON-LD vẫn có
`minValue/maxValue`, job đó vẫn có label thật và vẫn được dùng để train.
`salary_is_negotiable` không được dùng để loại job khỏi training khi min/max đã
có số.

Model train trên `log1p(y)` để giảm ảnh hưởng outlier salary quá lớn. Khi score,
hệ thống dùng `expm1(prediction)` để đổi về VND.

## 8. Model Đang Dùng

Model production hiện tại là Spark ML `GBTRegressor`.

Pipeline:

```text
title
-> RegexTokenizer
-> CountVectorizer

skills
-> CountVectorizer

location, company
-> StringIndexer
-> OneHotEncoder

experience, remote
-> numeric features

all features
-> VectorAssembler
-> GBTRegressor
```

Cấu hình chính:

```text
maxIter = 60
maxDepth = 5
maxBins = 64
minInstancesPerNode = 5
stepSize = 0.05
subsamplingRate = 0.8
seed = 42
```

Lý do dùng GBTRegressor thay Linear Regression: salary không tăng tuyến tính đơn
giản theo từng feature. Ví dụ cùng `Java`, nhưng `Senior + Remote + công ty lớn`
có thể khác hẳn `Fresher + Onsite + công ty nhỏ`. GBT học được các tương tác
phi tuyến tốt hơn baseline tuyến tính.

Model version hiện tại:

```text
spark_ml_salary_gbt_v1
```

## 9. Quy Tắc Scoring

Luật scoring cuối cùng là:

```text
Chỉ predict khi:
salary_min_vnd IS NULL
AND salary_max_vnd IS NULL
AND model load thành công
```

Hệ thống không predict nếu còn bất kỳ cận lương thật nào.

| Trường hợp | Có predict không | Lý do |
|---|---|---|
| `salary = "10 - 60 triệu"`, min=10M, max=60M | Không | Có range thật. |
| `salary = "Tới 40 triệu"`, min=null, max=40M | Không | Có cận trên thật. |
| `salary = "Lớn hơn 40 triệu"`, min=40M, max=null | Không | Có cận dưới thật. |
| `salary = "Thỏa thuận"`, min=30M, max=40M | Không | JSON-LD có lương thật. |
| `salary = "Thỏa thuận"`, min=30M, max=null | Không | Có một cận lương thật. |
| `salary = "Thỏa thuận"`, min=null, max=null | Có | Không có lương thật để dùng. |

`salary_is_negotiable` chỉ là cờ mô tả text nguồn. Nó không được phép ép model
ghi đè khi `salary_min_vnd` hoặc `salary_max_vnd` đã có giá trị.

## 10. Output Columns

| Column | Ý nghĩa |
|---|---|
| `salary_min_vnd` | Cận dưới lương thật parse được từ raw/json_ld. |
| `salary_max_vnd` | Cận trên lương thật parse được từ raw/json_ld. |
| `salary_predicted_min_vnd` | Cận dưới do model dự đoán, bằng 90% predicted average. |
| `salary_predicted_avg_vnd` | Lương trung bình do model dự đoán. |
| `salary_predicted_max_vnd` | Cận trên do model dự đoán, bằng 110% predicted average. |
| `salary_display_min_vnd` | Lương min cuối cùng cho UI/API. |
| `salary_display_avg_vnd` | Lương average cuối cùng cho sort/filter/chart. |
| `salary_display_max_vnd` | Lương max cuối cùng cho UI/API. |
| `salary_prediction_applied` | `true` nếu model thật sự được dùng. |
| `salary_source` | Nguồn tạo ra `salary_display_*`. |
| `salary_prediction_model_version` | Version model nếu có prediction. |

Phân biệt quan trọng:

```text
salary_predicted_* = riêng kết quả model
salary_display_*   = giá trị cuối cùng nên dùng cho Kibana/API
salary_source      = nguồn tạo ra salary_display_*
```

## 11. `salary_source`

`salary_source` giúp Kibana/Elasticsearch biểu diễn đẹp hơn, vì chỉ cần nhìn
một field là biết salary đang đến từ đâu.

| salary_source | Điều kiện |
|---|---|
| `parsed_range` | Có cả `salary_min_vnd` và `salary_max_vnd`. |
| `parsed_min_only` | Chỉ có `salary_min_vnd`. |
| `parsed_max_only` | Chỉ có `salary_max_vnd`. |
| `predicted` | Không có min/max thật và model đã dự đoán. |
| `unknown` | Không có min/max thật và chưa có prediction, thường do model chưa load được. |

Ví dụ:

| salary | salary_min_vnd | salary_max_vnd | salary_display_avg_vnd | salary_prediction_applied | salary_source |
|---|---:|---:|---:|---|---|
| `10 - 60 triệu` | 10000000 | 60000000 | 35000000 | false | `parsed_range` |
| `Tới 40 triệu` | null | 40000000 | 40000000 | false | `parsed_max_only` |
| `Lớn hơn 40 triệu` | 40000000 | null | 40000000 | false | `parsed_min_only` |
| `Thỏa thuận` | null | null | 21000000 | true | `predicted` |
| `Thỏa thuận` | 30000000 | 40000000 | 35000000 | false | `parsed_range` |
| `Thỏa thuận` | 30000000 | null | 30000000 | false | `parsed_min_only` |

## 12. Batch Flow

Thứ tự production:

```text
crawl_jobs
-> raw_to_bronze
-> bronze_to_silver
-> train_salary_model
-> silver_to_gold
-> gold_to_elasticsearch
```

Ý nghĩa:

| Step | Mục đích |
|---|---|
| `raw_to_bronze` | Đọc raw JSONL, chuẩn hóa dạng Bronze Parquet. |
| `bronze_to_silver` | Parse JSON-LD, salary, location, experience; tạo Silver. |
| `train_salary_model` | Train GBTRegressor bằng toàn bộ Silver. |
| `silver_to_gold` | Load model, apply prediction, sinh `salary_display_*`, `salary_source`. |
| `gold_to_elasticsearch` | Ghi Gold vào `gold-jobs-flat` cho Kibana/API. |

Nếu chỉ sửa logic scoring hoặc `salary_source`, không nhất thiết phải crawl lại.
Chỉ cần rebuild Spark image rồi chạy lại:

```powershell
Run-BatchJob "batch-etl-silver-to-gold" "manual-silver-to-gold" "7200s"
Run-BatchJob "batch-etl-gold-to-elasticsearch" "manual-gold-to-elasticsearch" "7200s"
```

Nếu vừa đổi model hoặc muốn train lại GBT:

```powershell
Run-BatchJob "batch-etl-train-salary-model" "manual-train-salary-model" "7200s"
Run-BatchJob "batch-etl-silver-to-gold" "manual-silver-to-gold" "7200s"
Run-BatchJob "batch-etl-gold-to-elasticsearch" "manual-gold-to-elasticsearch" "7200s"
```

## 13. Speed Flow

Speed layer load model một lần khi job start:

```text
Kafka jobs_raw
-> build clean jobs
-> load /models/salary_prediction/latest
-> score salary prediction
-> write realtime_jobs_v1
```

Nếu batch train model mới, cần restart speed job để load model mới:

```powershell
kubectl delete job -n spark speed-stream-es-submit --ignore-not-found=true
kubectl apply -f infra\spark\speed-stream-es-job.yaml
```

Nếu model chưa tồn tại hoặc load lỗi, speed vẫn chạy và tạo các cột prediction
null, `salary_prediction_applied=false`, `salary_source` sẽ là `parsed_*` nếu có
min/max hoặc `unknown` nếu không có min/max.

## 14. Kibana/Elasticsearch

Index batch:

```text
gold-jobs-flat
```

Index realtime:

```text
realtime_jobs_v1
```

Các field nên add trong Kibana Discover:

```text
title
company_name
salary
salary_min_vnd
salary_max_vnd
salary_display_avg_vnd
salary_prediction_applied
salary_source
salary_prediction_model_version
```

KQL hữu ích:

```text
salary_source: "predicted"
salary_source: "parsed_range"
salary_source: "parsed_min_only"
salary_source: "parsed_max_only"
salary_source: "unknown"
salary: "Thỏa thuận" and salary_source: "parsed_range"
salary: "Thỏa thuận" and salary_prediction_applied: true
```

Nếu vừa thêm field mới mà Kibana chưa thấy, vào:

```text
Stack Management -> Data Views -> gold-jobs-flat -> Refresh field list
```

## 15. Lưu Ý Vận Hành

- Dashboard/API nên dùng `salary_display_*`.
- Không dùng trực tiếp `salary_predicted_*` cho mọi job, vì field này chỉ có
  nghĩa khi `salary_prediction_applied=true`.
- `salary_source` nên được dùng để giải thích cho người xem dashboard vì sao
  một job có lương hiển thị.
- Model cũ tại `latest` bị overwrite khi train lại.
- Sau khi rebuild image, các Kubernetes job mới mới dùng code mới.
- Sau khi `gold_to_elasticsearch` chạy xong, refresh Kibana field list nếu có
  field mới như `salary_source`.
