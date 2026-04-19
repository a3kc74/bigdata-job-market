# Spark on Kubernetes

Tài liệu này mô tả cách chuẩn bị Spark trên Kubernetes để submit job từ máy local lên cluster Minikube.

## 1. Thành phần sử dụng

- Namespace: `spark`
- ServiceAccount: `spark`
- RoleBinding: `spark-edit`

Các resource này được khai báo trong:

- `infra/spark/10-rbac.yaml`

## 2. Apply cấu hình Spark namespace và RBAC

```powershell
kubectl apply -f infra/spark/10-rbac.yaml
```

## 3. Kiểm tra quyền của Spark service account

```powershell
kubectl auth can-i create pods --as=system:serviceaccount:spark:spark -n spark
kubectl auth can-i create services --as=system:serviceaccount:spark:spark -n spark
kubectl auth can-i create configmaps --as=system:serviceaccount:spark:spark -n spark
```

Kỳ vọng: cả 3 lệnh đều trả về `yes`.

## 4. Build Spark image cho app Python

```powershell
docker build --no-cache -t spark-kafka-es:demo -f infra/spark/Dockerfile .
docker run --rm spark-kafka-es:demo sh -lc "python --version && python3 --version"
minikube image load spark-kafka-es:demo
```

## 5. Kiểm tra API server của Kubernetes

```powershell
kubectl cluster-info
```

Ví dụ:

```text
Kubernetes control plane is running at https://127.0.0.1:<port>
```

Giá trị này sẽ được dùng trong lệnh `spark-submit`.

## 6. Submit Spark job lên Kubernetes

Ví dụ submit job streaming:

```cmd
set PYSPARK_PYTHON=python3
set PYSPARK_DRIVER_PYTHON=python3
set SPARK_SUBMIT_OPTS=-Djava.security.manager=allow

spark-submit ^
  --master k8s://https://127.0.0.1:<port> ^
  --deploy-mode cluster ^
  --name kafka-to-es ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 ^
  --conf spark.executor.instances=1 ^
  --conf spark.kubernetes.namespace=spark ^
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark ^
  --conf spark.kubernetes.container.image=spark-kafka-es:demo ^
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent ^
  local:///opt/spark/work-dir/kafka_to_es.py
```

## 7. Theo dõi pod Spark

```powershell
kubectl get pods -n spark -w
```

## 8. Ghi chú

- Nếu Minikube restart, cổng API server có thể thay đổi.
- Khi đó cần chạy lại `kubectl cluster-info` và cập nhật giá trị trong `--master`.
- Nếu build lại image, cần chạy lại `minikube image load spark-kafka-es:demo`.
