# Smoke test checklist - Cassandra on Kubernetes (TV5)

## Kubernetes / Helm
- [ ] `kubectl get nodes` thấy node `Ready`
- [ ] `kubectl get storageclass` thấy có `standard (default)` hoặc equivalent
- [ ] `kubectl get pods -n cert-manager` thấy các pod `Running`
- [ ] `kubectl get pods -n k8ssandra-operator` thấy:
  - [ ] `k8ssandra-operator` là `Running`
  - [ ] `cass-operator` là `Running`

## Cassandra cluster
- [ ] `kubectl apply -f infra/cassandra/10-k8ssandra.yaml` chạy thành công
- [ ] `kubectl get k8cs -n k8ssandra-operator` thấy cluster `demo`
- [ ] `kubectl get pods -n k8ssandra-operator` thấy pod:
  - [ ] `demo-dc1-default-sts-0`
- [ ] Pod Cassandra đạt trạng thái `2/2 Running`
- [ ] `kubectl get pvc -n k8ssandra-operator` thấy PVC `Bound`

## Node health
- [ ] `kubectl exec -it demo-dc1-default-sts-0 -n k8ssandra-operator -c cassandra -- nodetool status`
- [ ] Output có trạng thái `UN`

## Authentication
- [ ] `kubectl get secret demo-superuser -n k8ssandra-operator` thấy secret tồn tại
- [ ] Lấy được `username`
- [ ] Lấy được `password`

## CQL test
- [ ] Tạo được keyspace `test`
- [ ] Tạo được table `test.users`
- [ ] Insert được record test
- [ ] `SELECT * FROM test.users;` trả về dữ liệu

## Dấu hiệu thành công
- [ ] Cassandra chạy trên Kubernetes
- [ ] Có thể dùng `cqlsh` với tài khoản superuser
- [ ] Có thể đọc/ghi dữ liệu thành công
