# HDFS Data Ingestion Guide

How to load raw crawler data (JSONL) into HDFS running on Minikube.

---

## Prerequisites

- Minikube is running (`minikube status`)
- HDFS is deployed in the `hdfs` namespace
- `kubectl` is configured to point to your Minikube cluster

---

## Step 1 — Verify HDFS is Running

```bash
kubectl get pods -n hdfs
```

Expected output:
```
NAME                  READY   STATUS    RESTARTS   AGE
hdfs-namenode-0       1/1     Running   0          5m
hdfs-datanode-0       1/1     Running   0          5m
```

---

## Step 2 — Create Directory Structure in HDFS

Exec into the NameNode pod and create the target partition folder:

```bash
# Open a shell inside the NameNode pod
kubectl exec -it hdfs-namenode-0 -n hdfs -- bash

# Inside the pod: create directories for today's partition
hdfs dfs -mkdir -p /raw/jobs/ingest_date=2026-04-30
hdfs dfs -mkdir -p /bronze/jobs
hdfs dfs -mkdir -p /silver/jobs
hdfs dfs -mkdir -p /gold

# Verify
hdfs dfs -ls /raw/jobs/

# Exit the pod
exit
```

---

## Step 3 — Copy JSONL Files from Local to HDFS

HDFS runs inside the cluster, so you must first copy the file into the NameNode pod, then move it into HDFS.

```bash
# 1. Copy local JSONL file into the NameNode pod's /tmp
kubectl cp ./data/raw/jobs/2026-04-30/jobs.jsonl \
    hdfs/hdfs-namenode-0:/tmp/jobs_2026-04-30.jsonl

# 2. Exec into pod and push from /tmp to HDFS
kubectl exec -it hdfs-namenode-0 -n hdfs -- \
    hdfs dfs -put /tmp/jobs_2026-04-30.jsonl \
    /raw/jobs/ingest_date=2026-04-30/

# 3. Verify the file is in HDFS
kubectl exec -it hdfs-namenode-0 -n hdfs -- \
    hdfs dfs -ls /raw/jobs/ingest_date=2026-04-30/
```

---

## Step 4 — Bulk Ingestion (Multiple Days)

For loading multiple partitions at once, use a loop:

```bash
# Example: load all JSONL files from data/raw/jobs/
for dir in data/raw/jobs/*/; do
    date=$(basename "$dir")   # e.g. 2026-04-30

    # Create HDFS partition
    kubectl exec -it hdfs-namenode-0 -n hdfs -- \
        hdfs dfs -mkdir -p /raw/jobs/ingest_date=${date}

    # Copy files
    for f in "$dir"*.jsonl; do
        filename=$(basename "$f")
        kubectl cp "$f" hdfs/hdfs-namenode-0:/tmp/${filename}
        kubectl exec -it hdfs-namenode-0 -n hdfs -- \
            hdfs dfs -put /tmp/${filename} /raw/jobs/ingest_date=${date}/
    done

    echo "Loaded: ingest_date=${date}"
done
```

---

## Useful HDFS Commands (inside NameNode pod)

```bash
# List directory
hdfs dfs -ls /raw/jobs/

# Check file size
hdfs dfs -du -h /raw/jobs/

# Delete a partition (use carefully)
hdfs dfs -rm -r /raw/jobs/ingest_date=2026-04-30/

# Move file
hdfs dfs -mv /raw/jobs/ingest_date=2026-04-29/old.jsonl /raw/jobs/ingest_date=2026-04-29/new.jsonl

# Check HDFS overall status
hdfs dfsadmin -report
```

---

## HDFS Connection Info

When connecting from Spark (inside Kubernetes):

```
hdfs://hdfs-namenode.hdfs.svc:9000
```

- `hdfs-namenode` — Kubernetes Service name of NameNode
- `hdfs` — Kubernetes namespace
- `9000` — default HDFS RPC port

> If your HDFS Helm chart uses a different Service name, update `RAW_BASE_PATH` in `raw_to_bronze.py` accordingly.
