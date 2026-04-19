# Elastic on Kubernetes (TV5)

Thư mục này chứa manifest và tài liệu chạy Elasticsearch + Kibana bằng ECK trên Minikube.

## 1. Yêu cầu trước khi chạy

- Docker Desktop đang chạy ở Linux containers
- `minikube` đã cài
- `kubectl` đã cài
- Cluster Minikube đã lên
- Có StorageClass mặc định, ví dụ `standard`

Kiểm tra:

```powershell
kubectl get nodes
kubectl get storageclass
```

## 2. Start Minikube

Ví dụ cấu hình nhẹ:

```powershell
minikube start --driver=docker --cpus=2 --memory=9500
```

Kiểm tra:

```powershell
kubectl cluster-info
kubectl get nodes
```

## 3. Cài ECK operator

```powershell
kubectl create -f https://download.elastic.co/downloads/eck/3.3.2/crds.yaml
kubectl apply -f https://download.elastic.co/downloads/eck/3.3.2/operator.yaml
kubectl get pods -n elastic-system
```

Đợi `elastic-operator-0` thành `1/1 Running`.

## 4. Tạo namespace

```powershell
kubectl create namespace elastic-stack
```

## 5. Deploy Elasticsearch

```powershell
kubectl apply -f infra/elastic/10-elasticsearch.yaml
kubectl get elasticsearch -n elastic-stack
kubectl get pods -n elastic-stack
kubectl get pvc -n elastic-stack
```

Đợi pod `topcv-es-default-0` thành `1/1 Running`.

## 6. Deploy Kibana

```powershell
kubectl apply -f infra/elastic/20-kibana.yaml
kubectl get kibana -n elastic-stack
kubectl get pods -n elastic-stack
```

Đợi pod Kibana thành `1/1 Running`.

## 7. Lấy password của user elastic

```powershell
kubectl get secret topcv-es-elastic-user -n elastic-stack -o go-template='{{.data.elastic | base64decode}}'
```

## 8. Port-forward để truy cập local

Mở 2 cửa sổ PowerShell riêng:

### Elasticsearch
```powershell
kubectl port-forward -n elastic-stack service/topcv-es-http 9200
```

### Kibana
```powershell
kubectl port-forward -n elastic-stack service/topcv-kb-http 5601
```

## 9. Link truy cập

- Elasticsearch: https://localhost:9200
- Kibana: https://localhost:5601

## 10. Tạo index jobs_historical

Sau khi đã lấy password `elastic`, có thể tạo index qua PowerShell.

```powershell
$pair = "elastic:YOUR_PASSWORD"
$encoded = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes($pair))
$headers = @{
  Authorization = "Basic $encoded"
}

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
[System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }

$body = @"
{
  "mappings": {
    "properties": {
      "job_id":        { "type": "keyword" },
      "job_url":       { "type": "keyword" },
      "content_hash":  { "type": "keyword" },
      "job_title":     { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
      "company_name":  { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
      "city":          { "type": "keyword" },
      "event_date":    { "type": "date", "format": "yyyy-MM-dd" },
      "fetched_at":    { "type": "date" },
      "salary_text":   { "type": "keyword" },
      "salary_min":    { "type": "integer" },
      "salary_max":    { "type": "integer" },
      "skills":        { "type": "keyword" }
    }
  }
}
"@

Invoke-RestMethod -Method Put `
  -Uri https://localhost:9200/jobs_historical `
  -Headers $headers `
  -ContentType "application/json" `
  -Body $body
```

## 11. Tạo Data View trong Kibana

- Vào Kibana
- Stack Management
- Data Views
- Create data view
- Name: `jobs_historical`
- Index pattern: `jobs_historical`
- Time field: `fetched_at`

## 12. Commit phần hạ tầng

```powershell
git add infra/elastic
git commit -m "add eck elasticsearch and kibana manifests"
```
