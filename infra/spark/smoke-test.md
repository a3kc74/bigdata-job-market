# Smoke test - Spark on Kubernetes

## 1. Kiểm tra namespace và RBAC

```powershell
kubectl get ns spark
kubectl get sa spark -n spark
kubectl get rolebinding spark-edit -n spark
```

Kỳ vọng:
- namespace `spark` tồn tại
- serviceaccount `spark` tồn tại
- rolebinding `spark-edit` tồn tại

## 2. Kiểm tra quyền của service account

```powershell
kubectl auth can-i create pods --as=system:serviceaccount:spark:spark -n spark
kubectl auth can-i create services --as=system:serviceaccount:spark:spark -n spark
kubectl auth can-i create configmaps --as=system:serviceaccount:spark:spark -n spark
```

Kỳ vọng:
- cả 3 lệnh đều trả về `yes`

## 3. Kiểm tra Spark image local

```powershell
docker run --rm spark-kafka-es:demo sh -lc "python --version && python3 --version"
```

Kỳ vọng:
- image chạy được
- có cả `python` và `python3`

## 4. Kiểm tra image đã load vào Minikube

```powershell
minikube image ls | findstr spark-kafka-es
```

Kỳ vọng:
- có image `spark-kafka-es:demo`

## 5. Submit thử job Spark lên Kubernetes

```cmd
set PYSPARK_PYTHON=python3
set PYSPARK_DRIVER_PYTHON=python3
set SPARK_SUBMIT_OPTS=-Djava.security.manager=allow
```

```powershell
kubectl cluster-info
```

```cmd
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

## 6. Theo dõi pod Spark

```powershell
kubectl get pods -n spark -w
```

Kỳ vọng:
- 1 driver pod được tạo
- 1 executor pod được tạo

## 7. Kiểm tra log driver

```powershell
kubectl logs <driver-pod-name> -n spark --container spark-kubernetes-driver
```

Kỳ vọng:
- không lỗi `python not found`
- không lỗi version mismatch với Kafka package
- nếu chạy streaming pipeline thì có log `batch_id=...`

## 8. Kết luận pass

Smoke test pass nếu:
- RBAC đúng
- image Spark dùng được
- pod driver được tạo thành công
- pod executor được tạo thành công
- log driver không fail ở giai đoạn bootstrap
