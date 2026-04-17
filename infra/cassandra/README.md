# Cassandra on Kubernetes (TV5)

Thư mục này chứa manifest và tài liệu chạy Cassandra trên Kubernetes bằng K8ssandra Operator.

## 1. Yêu cầu trước khi chạy

- Docker Desktop đang chạy
- `minikube` đã cài
- `kubectl` đã cài
- `helm` đã cài
- Cluster Minikube đã lên
- Có StorageClass mặc định, ví dụ `standard`

Kiểm tra:

```powershell
kubectl get nodes
kubectl get storageclass
```

## 2. Start Minikube

Ví dụ:

```powershell
minikube start --driver=docker --cpus=2 --memory=6144
```

Kiểm tra:

```powershell
kubectl cluster-info
kubectl get nodes
```

## 3. Cài cert-manager

```powershell
helm repo add jetstack https://charts.jetstack.io
helm repo update

helm install cert-manager jetstack/cert-manager `
  --namespace cert-manager `
  --create-namespace `
  --set installCRDs=true
```

Kiểm tra:

```powershell
kubectl get pods -n cert-manager
```

## 4. Cài K8ssandra Operator

```powershell
helm repo add k8ssandra https://helm.k8ssandra.io/stable
helm repo update

helm install k8ssandra-operator k8ssandra/k8ssandra-operator `
  -n k8ssandra-operator `
  --create-namespace
```

Kiểm tra:

```powershell
kubectl get pods -n k8ssandra-operator
```

## 5. Deploy Cassandra cluster

Apply manifest:

```powershell
kubectl apply -f infra/cassandra/10-k8ssandra.yaml
```

Kiểm tra:

```powershell
kubectl get k8cs -n k8ssandra-operator
kubectl get pods -n k8ssandra-operator -w
kubectl get pvc -n k8ssandra-operator
```

Khi thành công, pod Cassandra sẽ có dạng:

```text
demo-dc1-default-sts-0
```

và trạng thái:

```text
2/2 Running
```

## 6. Kiểm tra trạng thái node Cassandra

```powershell
kubectl exec -it demo-dc1-default-sts-0 -n k8ssandra-operator -c cassandra -- nodetool status
```

Nếu thành công sẽ thấy node có trạng thái `UN`.

## 7. Lấy username/password superuser

```powershell
$username_b64 = kubectl get secret demo-superuser -n k8ssandra-operator -o jsonpath="{.data.username}"
$password_b64 = kubectl get secret demo-superuser -n k8ssandra-operator -o jsonpath="{.data.password}"

$username = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($username_b64))
$password = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($password_b64))

$username
$password
```

## 8. Dùng cqlsh để tạo dữ liệu test

```powershell
kubectl exec -it demo-dc1-default-sts-0 -n k8ssandra-operator -c cassandra -- cqlsh -u $username -p $password -e "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

kubectl exec -it demo-dc1-default-sts-0 -n k8ssandra-operator -c cassandra -- cqlsh -u $username -p $password -e "CREATE TABLE test.users (email text PRIMARY KEY, name text);"

kubectl exec -it demo-dc1-default-sts-0 -n k8ssandra-operator -c cassandra -- cqlsh -u $username -p $password -e "INSERT INTO test.users (email, name) VALUES ('a@test.com', 'Alice');"

kubectl exec -it demo-dc1-default-sts-0 -n k8ssandra-operator -c cassandra -- cqlsh -u $username -p $password -e "SELECT * FROM test.users;"
```

Nếu lệnh cuối ra `Alice` thì Cassandra trên Kubernetes đã hoạt động.

## 9. File manifest chính

- `infra/cassandra/10-k8ssandra.yaml`

## 10. Commit phần Cassandra

```powershell
git add infra/cassandra
git commit -m "add cassandra k8s configs and docs"
```
