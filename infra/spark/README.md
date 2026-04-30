# Spark on Kubernetes — Hướng dẫn tích hợp Batch ETL

## Kiến trúc

```
Kubernetes (Minikube)
│
├── namespace: spark
│   ├── ServiceAccount: spark          (RBAC — infra/spark/10-rbac.yaml)
│   ├── CronJob: batch-etl-raw-bronze  (infra/kubernetes/batch-etl-cronjob.yaml)
│   │     ↓ tạo
│   ├── Job → Pod: spark-driver
│   │               ↓ tạo
│   └── Pods: spark-executor-xxx (x2)
│
├── namespace: kafka
├── namespace: streaming
└── namespace: k8ssandra-operator
```

## Luồng chạy

```
02:00 AM hàng ngày
    → CronJob tạo Job
    → Job tạo Pod "spark-submit"
    → Pod chạy spark-submit --master k8s://...
    → Spark Driver Pod khởi động trong namespace spark
    → Driver tạo 2 Executor Pods
    → ETL đọc HDFS raw → xử lý → ghi HDFS bronze
    → Executor Pods tự xóa sau khi xong
    → Driver Pod ở trạng thái Completed (log vẫn xem được)
```

## Cấu trúc file liên quan

```
infra/
├── spark/
│   ├── Dockerfile          ← Image Spark + batch ETL code
│   ├── 10-rbac.yaml        ← Namespace, ServiceAccount, RoleBinding
│   └── README.md
│
├── kubernetes/
│   └── batch-etl-cronjob.yaml  ← CronJob chạy raw→bronze hàng ngày
│
apps/
└── batch/
    └── jobs/
        ├── job_raw_to_bronze.py
        ├── job_bronze_to_silver.py   (chưa viết)
        └── job_silver_to_gold.py     (chưa viết)
```

## Build và deploy (Minikube)

```bash
# 1. Dùng Docker daemon của Minikube (không cần push registry)
eval $(minikube docker-env)

# 2. Build image từ root repo (để COPY paths đúng)
docker build -f infra/spark/Dockerfile -t bigdata-job-market/spark-etl:latest .

# 3. Apply RBAC
kubectl apply -f infra/spark/10-rbac.yaml

# 4. Apply CronJob
kubectl apply -f infra/kubernetes/batch-etl-cronjob.yaml

# 5. Kiểm tra
kubectl get cronjob -n spark
```

## Chạy thủ công (test ngay, không cần đợi schedule)

```bash
kubectl create job --from=cronjob/batch-etl-raw-to-bronze \
    manual-bronze-$(date +%Y%m%d) -n spark
```

## Xem log

```bash
# Xem driver pod
kubectl get pods -n spark

# Xem log
kubectl logs <driver-pod-name> -n spark

# Xem executor logs
kubectl logs <executor-pod-name> -n spark
```

## Lưu ý HDFS + Kubernetes

Spark trên K8s cần kết nối được HDFS NameNode. Có 2 cách:

| Cách | Mô tả |
|---|---|
| **HDFS trong K8s** | Deploy HDFS bằng Helm chart, expose NameNode qua Service. Spark dùng `hdfs://hdfs-namenode.hdfs.svc:9000` |
| **HDFS ngoài K8s** | HDFS chạy trên máy host/VM, Spark dùng IP trực tiếp. Cần cấu hình `core-site.xml` trong Spark image |

Với Minikube (local dev), thường dùng **HDFS trong K8s** hoặc mount volume local thay thế khi test.
