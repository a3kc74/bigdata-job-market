# Hướng dẫn mở Airflow UI trên Minikube

Tài liệu này hướng dẫn cách khởi động Minikube, deploy Airflow và mở giao diện Airflow Web UI trên máy local.

## 1. Khởi động Minikube

Chạy Minikube với đủ CPU, RAM và disk cho Airflow, Spark, Kafka, Elasticsearch và HDFS:

```powershell
minikube start -p job-market --driver=docker --cpus=8 --memory=10000 --disk-size=40g
```

Kiểm tra trạng thái cluster:

```powershell
minikube status
kubectl get nodes
```

Kỳ vọng:

```text
host: Running
kubelet: Running
apiserver: Running
```

## 2. Bật các addon cần thiết

```powershell
minikube addons enable storage-provisioner
minikube addons enable default-storageclass
```

Kiểm tra storage class:

```powershell
kubectl get storageclass
```

Kỳ vọng có storage class `standard`.

## 3. Build image local dùng cho Airflow và Spark

Nếu manifest Kubernetes dùng image local, cần build image vào Minikube.

Build Airflow image:

```powershell
minikube image build -f infra\airflow\Dockerfile -t job-market-airflow:2.9.3 .
```

Build Spark image:

```powershell
minikube image build -f infra\spark\Dockerfile -t spark-job-market:latest .
```

Kiểm tra image đã có trong Minikube:

```powershell
minikube image ls | Select-String "job-market-airflow|spark-job-market"
```

Nếu tag image trong YAML khác, kiểm tra bằng:

```powershell
Select-String -Path infra\airflow\airflow.yaml -Pattern "image:"
Select-String -Path infra\spark\*.yaml -Pattern "image:"
```

## 4. Apply namespace

Chạy từ thư mục root của project:

```powershell
kubectl apply -f infra\namespaces\all.yaml
```

Kiểm tra namespace:

```powershell
kubectl get ns
```

Kỳ vọng có các namespace như:

- `airflow`
- `spark`
- `kafka`
- `search`
- `hdfs`

## 5. Apply Airflow manifests

```powershell
kubectl apply -f infra\airflow\airflow-rbac.yaml
kubectl apply -f infra\airflow\airflow-postgres.yaml
kubectl apply -f infra\airflow\airflow.yaml
```

## 6. Chờ Airflow chạy

Kiểm tra pod:

```powershell
kubectl get pods -n airflow
```

Kỳ vọng:

- `airflow-postgres-...` — `1/1 Running`
- `airflow-scheduler-...` — `1/1 Running`
- `airflow-webserver-...` — `1/1 Running`

Kiểm tra rollout:

```powershell
kubectl rollout status deployment -n airflow airflow-scheduler
kubectl rollout status deployment -n airflow airflow-webserver
```

## 7. Mở Airflow Web UI

Port-forward Airflow webserver:

```powershell
kubectl port-forward -n airflow svc/airflow-webserver 8082:8080
```

Sau đó mở trình duyệt:

```text
http://localhost:8082
```

Giữ terminal port-forward mở trong lúc dùng Airflow.

## 8. Nếu service name khác

Kiểm tra service trong namespace `airflow`:

```powershell
kubectl get svc -n airflow
```

Nếu service webserver không tên là `airflow-webserver`, thay tên service trong lệnh port-forward:

```powershell
kubectl port-forward -n airflow svc/<ten-service-webserver> 8082:8080
```

Ví dụ:

```powershell
kubectl port-forward -n airflow svc/airflow-webserver 8082:8080
```
