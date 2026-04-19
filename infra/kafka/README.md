# Kafka on Kubernetes (TV5)

Thư mục này chứa tài liệu chạy Kafka trên Kubernetes bằng Strimzi Operator.

## 1. Yêu cầu trước khi chạy

- Docker Desktop đang chạy
- `minikube` đã cài
- `kubectl` đã cài
- `helm` đã cài
- Cluster Minikube đã lên

Kiểm tra:

```powershell
kubectl get nodes
kubectl cluster-info
```

## 2. Start Minikube

Ví dụ:

```powershell
minikube start --driver=docker --cpus=2 --memory=9500
```

Kiểm tra:

```powershell
kubectl get nodes
kubectl cluster-info
```

## 3. Tạo namespace Kafka

```powershell
kubectl create namespace kafka
```

## 4. Cài Strimzi operator

```powershell
helm install strimzi-cluster-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator -n kafka
```

Kiểm tra:

```powershell
kubectl get deployments -n kafka
kubectl get pods -n kafka
```

Đợi pod `strimzi-cluster-operator` thành `1/1 Running`.

## 5. Deploy Kafka single-node

```powershell
kubectl apply -n kafka -f https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/main/examples/kafka/kafka-single-node.yaml
```

Theo dõi:

```powershell
kubectl get pods -n kafka -w
```

Khi thành công, sẽ thấy các pod như:

- `my-cluster-dual-role-0`
- `my-cluster-entity-operator-...`

đều ở trạng thái `Running`.

## 6. Kiểm tra resource Kafka

```powershell
kubectl get kafka -n kafka
kubectl get kafkanodepool -n kafka
kubectl get svc -n kafka
kubectl get pods -n kafka
```

## 7. Test producer / consumer

Mở 2 terminal khác nhau.

### Consumer

```powershell
kubectl run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.51.0-kafka-4.2.0 --rm=true --restart=Never -n kafka -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic --from-beginning
```

### Producer

```powershell
kubectl run kafka-producer -ti --image=quay.io/strimzi/kafka:0.51.0-kafka-4.2.0 --rm=true --restart=Never -n kafka -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic
```

Ví dụ message test:

```text
hello kafka
job_1|Data Engineer|Ha Noi
job_2|Backend Developer|HCM
```

Nếu consumer nhận lại được các dòng này, Kafka trên Kubernetes đã hoạt động đúng.

## 8. Dữ liệu trong giai đoạn này đi đâu?

Trong giai đoạn Kafka-only:
- Producer gửi message vào Kafka topic
- Kafka lưu message trong topic
- Consumer đọc message từ topic

Chưa cần Cassandra hay Elasticsearch ở bước chứng minh Kafka cơ bản.

## 9. Commit phần Kafka

```powershell
git add infra/kafka
git commit -m "add kafka k8s docs"
```
