# Hướng dẫn Cài đặt và Sử dụng Minikube cho Dự án Big Data Job Market

Dự án này sử dụng Kubernetes (thông qua **Minikube**) làm môi trường triển khai (Deployment Environment) cho các tiến trình Spark Batch ETL, Kafka, Cassandra và Elasticsearch. Dưới đây là hướng dẫn từ số không để bạn thiết lập thành công.

---

## 1. Yêu cầu hệ thống

- Hệ điều hành: Windows 10/11 (chạy qua WSL2) hoặc Linux (Ubuntu).
- RAM: Tối thiểu 8GB (khuyến nghị 12GB+ vì chạy cả Spark, Kafka, Cassandra).
- CPU: Tối thiểu 4 cores.
- Đã cài đặt Docker Desktop (trên Windows) hoặc Docker Engine (trên Linux).

---

## 2. Cài đặt Minikube

### Đối với Windows (Sử dụng WSL2)
Nên cài Minikube bên trong Ubuntu (WSL) để đạt hiệu năng tốt nhất, thay vì cài trên Windows native.

1. Mở terminal Ubuntu (WSL2).
2. Tải và cài đặt Minikube:
   ```bash
   curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
   sudo install minikube-linux-amd64 /usr/local/bin/minikube
   ```
3. Cài đặt `kubectl` (Công cụ giao tiếp với K8s):
   ```bash
   curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
   sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
   ```

### Đối với macOS / Linux Native
Sử dụng Homebrew (macOS) hoặc apt (Ubuntu):
```bash
brew install minikube
brew install kubectl
```

---

## 3. Khởi động Cluster Minikube

Để có đủ tài nguyên chạy toàn bộ Stack Big Data, hãy khởi động Minikube với cấu hình tài nguyên lớn:

```bash
minikube start --driver=docker --memory=8192 --cpus=4
```

> **Lưu ý quan trọng**: File cấu hình của Minikube sẽ nằm ở `~/.kube/config` (chứa credentials để kubectl kết nối) và `~/.minikube/config/config.json`. Mọi thao tác K8s của bạn giờ sẽ được tự động trỏ vào cụm Minikube này.

---

## 4. Xây dựng Docker Image trực tiếp trong Minikube

Minikube sử dụng môi trường Docker "của riêng nó", hoàn toàn tách biệt với Docker Desktop bên ngoài. Vì vậy, để Minikube thấy được Spark Image của bạn, bạn phải build image đó **bên trong Minikube**.

Gõ lệnh sau để "trỏ" terminal hiện tại vào môi trường Docker của Minikube:
```bash
eval $(minikube docker-env)
```

Kiểm tra xem đã thành công chưa bằng cách gõ `docker ps`. Bạn sẽ thấy các container hệ thống của K8s đang chạy (chứ không phải container của host).

Bây giờ, tiến hành build Spark ETL Image cho dự án (đảm bảo bạn đang đứng ở thư mục gốc của project):
```bash
docker build -f infra/spark/Dockerfile -t bigdata-job-market/spark-etl:latest .
```
*(Lưu ý: Nếu bạn restart lại Minikube, bạn không cần build lại trừ khi bạn xoá sạch minikube, nhưng bạn luôn phải chạy lại lệnh `eval $(minikube docker-env)` mỗi khi mở tab terminal mới nếu muốn build image).*

---

## 5. Cấu hình Namespaces và RBAC cho Spark

Spark khi chạy trên Kubernetes cần quyền (RBAC) để tự động tạo (spin-up) các Pod (Executor/Driver). 

Tạo Namespace và cấp quyền:
```bash
kubectl create namespace spark
kubectl apply -f infra/spark/10-rbac.yaml
```

---

## 6. Triển khai các CronJob ETL

Sau khi mọi thứ đã sẵn sàng, hãy apply các CronJob để hệ thống tự động chạy quy trình ETL (Raw -> Bronze -> Silver -> Gold):

```bash
kubectl apply -f infra/kubernetes/batch-etl-cronjob.yaml
```

**Một số lệnh kubectl hữu ích để theo dõi:**
- Xem danh sách CronJobs: `kubectl get cronjob -n spark`
- Kích hoạt chạy thủ công (Manual Trigger) một Job thay vì chờ đến giờ:
  ```bash
  kubectl create job --from=cronjob/batch-etl-raw-to-bronze manual-run-1 -n spark
  ```
- Xem các Pod (Driver/Executor) đang chạy: `kubectl get pods -n spark`
- Đọc log của một Job: `kubectl logs -f job.batch/manual-run-1 -n spark`

---

## 7. Đóng Minikube

Khi kết thúc công việc, hãy tắt Minikube để giải phóng RAM:
```bash
minikube stop
```
Để xoá toàn bộ cụm (Lưu ý: Mất sạch dữ liệu Database):
```bash
minikube delete
```
