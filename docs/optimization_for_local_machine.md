# Optimization Guide - Local Development Setup

Repo này được tối ưu cho máy local với tài nguyên hạn chế. Dưới đây là 8 tối ưu được áp dụng:

## Tối ưu 1: Elasticsearch Resources

**Hiện tại:**
```yaml
requests:
  memory: 768Mi
  cpu: 300m
limits:
  memory: 768Mi
  cpu: 700m
```

Nếu máy quá yếu, có thể thử 512Mi nhưng ES dễ không ổn định:
```yaml
requests:
  memory: 512Mi
  cpu: 200m
limits:
  memory: 512Mi
  cpu: 500m
```

**File:** `infra/elastic/10-elasticsearch.yaml`

## Tối ưu 2: Kibana Resources

**Hiện tại:**
```yaml
requests:
  memory: 384Mi
  cpu: 150m
limits:
  memory: 512Mi
  cpu: 300m
```

Nhẹ hơn so với Elasticsearch do chỉ là UI.

**File:** `infra/elastic/20-kibana.yaml`

## Tối ưu 3: MongoDB/HDFS Single Replica

Trong môi trường local Minikube, hệ thống được cấu hình ở mức tối thiểu để tiết kiệm tài nguyên.

Các thành phần lưu trữ chính:

- MongoDB: 1 replica, dùng để lưu dữ liệu serving cho backend/API.
- HDFS NameNode: 1 replica, quản lý metadata cho Raw/Bronze/Silver/Gold.
- HDFS DataNode: 1 replica trong môi trường local, dùng để lưu dữ liệu thực tế.

HDFS được sử dụng làm storage chính cho pipeline batch ETL. Các tầng dữ liệu được tổ chức theo dạng:

```text
hdfs://hdfs-namenode.hdfs.svc:9000/raw/jobs
hdfs://hdfs-namenode.hdfs.svc:9000/bronze/jobs
hdfs://hdfs-namenode.hdfs.svc:9000/silver/jobs
hdfs://hdfs-namenode.hdfs.svc:9000/gold
```

Cấu hình này phù hợp cho môi trường đồ án/local vì vẫn mô phỏng được distributed storage nhưng giảm số lượng replica để tránh quá tải tài nguyên.

## Tối ưu 4: Elasticsearch Storage

**Giảm từ 3Gi xuống 2Gi** cho demo local:

```yaml
storage: 2Gi
```

**File:** `infra/elastic/10-elasticsearch.yaml`

## Tối ưu 5: Single Instance Everywhere

Tất cả services chỉ chạy 1 instance:
- MongoDB: 1 replica
- HDFS NameNode: 1 instance
- HDFS DataNode: 1 instance
- Elasticsearch: 1 node
- Kibana: 1 pod
- Spark executors: 1 instance/job

## Tối ưu 6: Non-Overlapping Batch Jobs

Spark Operator jobs được schedule để **không chạy chồng nhau**:

```
silver-job:        0 * * * *  (giờ thứ 0)
gold-job:         10 * * * *  (giờ thứ 10)
gold-sync-mongo:  20 * * * *  (giờ thứ 20)
gold-sync-es:     30 * * * *  (giờ thứ 30)
```

**Files:** 
- `infra/spark-operator/silver-job.yaml`
- `infra/spark-operator/gold-job.yaml`
- `infra/spark-operator/gold-sync-mongo-job.yaml`
- `infra/spark-operator/gold-sync-es-job.yaml`

## Tối ưu 7: Reduced Spark Resources

### speed-job (real-time, không schedule)
```yaml
driver:
  memory: "768m"
executor:
  instances: 1
  memory: "768m"
```

### silver-job, gold-job (batch)
```yaml
driver:
  memory: "1g"
executor:
  instances: 1
  memory: "1g"
```

### gold-sync-mongo-job, gold-sync-es-job (lightweight sync)
```yaml
driver:
  memory: "512m"
executor:
  instances: 1
  memory: "512m"
```

Cả 4 job đều giảm `spark.sql.shuffle.partitions` từ 4 xuống 2.

**Files:**
- `infra/spark-operator/speed-job.yaml`
- `infra/spark-operator/silver-job.yaml`
- `infra/spark-operator/gold-job.yaml`
- `infra/spark-operator/gold-sync-mongo-job.yaml`
- `infra/spark-operator/gold-sync-es-job.yaml`

## Tối ưu 8: Optional Kibana Deployment

**Flow nhẹ hơn cho máy yếu:**

```bash
# Bước 1: Deploy basic infrastructure
make k8s-up

# Bước 2: Deploy Elasticsearch (không Kibana)
make elastic-up

# Bước 3: Deploy Spark Operator
make spark-operator-up

# Bước 4: Deploy Spark jobs
make build-spark-image
make apps-up

# Bước 5: Khi system ổn định, mới deploy Kibana
make kibana-up
```

**Makefile targets:**
- `elastic-up` - Elasticsearch only (nhẹ)
- `kibana-up` - Kibana only (thêm vào sau khi cần)

## Checklist Setup Local

1. **Start Minikube:**
   ```bash
   minikube start --driver=docker --cpus=2 --memory=9500
   ```

2. **Setup Elasticsearch ECK:**
   ```bash
   kubectl create ns elastic-stack
   kubectl apply -f https://download.elastic.co/downloads/eck/2.10.0/crds.yaml
   kubectl apply -f https://download.elastic.co/downloads/eck/2.10.0/operator.yaml
   ```

3. **Deploy infrastructure:**
   ```bash
   make k8s-up
   make kafka-up
   make elastic-up
   make spark-operator-up
   ```

4. **Build and deploy apps:**
   ```bash
   make build-spark-image
   make apps-up
   ```

5. **Monitor:**
   ```bash
   make status
   kubectl logs -n job-market pod-name -f
   ```

## Troubleshooting

**Elasticsearch crash loop:**
- Thử giảm memory xuống 512Mi
- Check logs: `kubectl logs -n elastic-stack elasticsearch-topcv-es-default-0`

**Spark jobs pending:**
- Check resources: `kubectl describe node`
- Giảm số instances hoặc memory
- Wait for pending pods: `kubectl get pods -n job-market -w`

**Out of disk:**
- Check storage: `df -h /mnt/docker.raw` (on minikube)
- Giảm Elasticsearch storage xuống 1Gi

## Resource Estimation

Tổng tài nguyên tối thiểu cho full setup:

- Kafka/Zookeeper: ~400Mi
- MongoDB: ~300Mi
- HDFS: tùy dung lượng dữ liệu Raw/Bronze/Silver/Gold, cấu hình tối thiểu cho local
- Elasticsearch: ~768Mi
- Kibana: ~384Mi (optional)
- Spark jobs: ~2Gi (giờ đó là peak)

**Total: ~4Gi RAM, ~2 CPUs**

Recommended: **7-8GB RAM, 4 CPUs** để chạy ổn định.

## Notes

- Tất cả cấu hình có thể điều chỉnh trong các YAML files
- Để scale up cho production, tăng replicas, memory, CPU
- Spark partitions (`spark.sql.shuffle.partitions`) nên = số cores
- Đừng deploy Kibana nếu máy < 6GB RAM
