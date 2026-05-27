# Spark Jobs on Minikube — General Guide

How to set up, deploy, and operate Spark Batch ETL jobs on a local Kubernetes cluster (Minikube).

---

## First-Time Setup

### 1. Start Minikube

```bash
minikube start --memory=4096 --cpus=4
```

### 2. Point Docker CLI to Minikube's Docker Daemon

Build images directly inside Minikube so Kubernetes can find them without a registry.

```bash
# Linux / macOS
eval $(minikube docker-env)

# Windows PowerShell
& minikube -p minikube docker-env --shell powershell | Invoke-Expression
```

### 3. Build the Spark Image

Run from the **repo root** so COPY paths in the Dockerfile resolve correctly.

```bash
docker build -f infra/spark/Dockerfile -t bigdata-job-market/spark-etl:latest .
```

### 4. Apply RBAC

Creates the `spark` namespace, a dedicated ServiceAccount, and a RoleBinding that allows the Spark driver to create/delete Executor pods.

```bash
kubectl apply -f infra/spark/10-rbac.yaml
```

### 5. Deploy the CronJob

The CronJob triggers `raw_to_bronze.py` daily at **02:00 AM** (local time, UTC+7 → 19:00 UTC).

```bash
kubectl apply -f infra/kubernetes/batch-etl-cronjob.yaml

# Verify
kubectl get cronjob -n spark
```

---

## Resuming After Machine Restart

Minikube **loses all Docker images** when the virtual machine restarts. You must rebuild the image each time.

```bash
# 1. Restart Minikube
minikube start

# 2. Re-point Docker CLI to Minikube daemon
eval $(minikube docker-env)                         # Linux/macOS
& minikube -p minikube docker-env --shell powershell | Invoke-Expression  # Windows

# 3. Rebuild the Spark image
docker build -f infra/spark/Dockerfile -t bigdata-job-market/spark-etl:latest .

# 4. CronJob manifests persist — no need to re-apply unless YAML changed
kubectl get cronjob -n spark
```

> **Tip:** If you changed `infra/spark/Dockerfile` or `infra/kubernetes/batch-etl-cronjob.yaml`, re-apply the corresponding manifest after rebuild.

---

## Monitoring & Logs

### Pods & Jobs

```bash
# List all pods in the spark namespace
kubectl get pods -n spark

# Watch pods in real time
kubectl get pods -n spark -w

# List all jobs (completed, running, failed)
kubectl get jobs -n spark
```

### Logs

```bash
# Stream logs from a driver pod (replace with actual pod name)
kubectl logs -f <driver-pod-name> -n spark

# View logs from a completed pod
kubectl logs <driver-pod-name> -n spark

# View logs from an executor pod
kubectl logs <executor-pod-name> -n spark
```

### Inspect a Pod

```bash
# Show events and status details (useful for debugging crash/OOM)
kubectl describe pod <pod-name> -n spark
```

### CronJob Status

```bash
# Check schedule and last run time
kubectl get cronjob batch-etl-raw-to-bronze -n spark

# Describe CronJob for full detail
kubectl describe cronjob batch-etl-raw-to-bronze -n spark
```

---

## Managing Jobs

```bash
# Delete a failed or stuck job
kubectl delete job <job-name> -n spark

# List and clean up all completed jobs
kubectl get jobs -n spark | grep Completed
kubectl delete job <job-name> -n spark
```

---

## Web Dashboard

```bash
minikube dashboard
```

Opens a browser-based UI showing all pods, jobs, namespaces, and resource usage.

---

## Updating the Spark Image

When you change Python code or add dependencies:

```bash
# 1. Re-point to Minikube daemon
eval $(minikube docker-env)

# 2. Rebuild (tag stays the same so K8s picks it up automatically)
docker build -f infra/spark/Dockerfile -t bigdata-job-market/spark-etl:latest .

# 3. Trigger a new job to test
kubectl create job --from=cronjob/batch-etl-raw-to-bronze \
    test-$(date +%Y%m%d%H%M) -n spark
```

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| Pod stuck in `ImagePullBackOff` | Image not found in Minikube | Rebuild image with `eval $(minikube docker-env)` first |
| Pod in `OOMKilled` | Executor needs more memory | Increase `spark.executor.memory` in CronJob YAML |
| HDFS connection refused | NameNode pod not running | `kubectl get pods -n hdfs` and check NameNode status |
| Job never starts | RBAC not applied | `kubectl apply -f infra/spark/10-rbac.yaml` |
| `CrashLoopBackOff` | Python error in ETL script | `kubectl logs <pod-name> -n spark` to see traceback |
