"""Run TopCV crawler-branch batch strategy and publish one raw JSONL file to HDFS.

topcv_crawler.py writes records incrementally to a local temp JSONL file.
This wrapper publishes that completed local file to HDFS after the batch finishes:

  /raw/jobs/ingest_date=YYYY-MM-DD/raw_jobs_YYYYMMDDTHHMMSSZ.jsonl
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import SparkSession

from apps.ingestion import run_crawler
from apps.ingestion import topcv_crawler


def _extract_ingest_date(local_output_path: str) -> str:
    match = re.search(r"ingest_date=([0-9]{4}-[0-9]{2}-[0-9]{2})", local_output_path)
    if match:
        return match.group(1)

    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _count_local_lines(path: str) -> int:
    with open(path, "r", encoding="utf-8") as file:
        return sum(1 for _ in file)


def _make_hdfs_raw_filename() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"raw_jobs_{timestamp}.jsonl"


def copy_local_jsonl_to_hdfs(local_output_path: str, hdfs_raw_base_path: str) -> str:
    local_file = Path(local_output_path).resolve()

    if not local_file.exists():
        raise FileNotFoundError(f"Crawler output file was not created: {local_file}")

    line_count = _count_local_lines(str(local_file))
    if line_count <= 0:
        raise RuntimeError(f"[batch_crawler] no records crawled: {local_file}")

    ingest_date = _extract_ingest_date(str(local_file))
    hdfs_raw_base_path = hdfs_raw_base_path.rstrip("/")

    parsed = urlparse(hdfs_raw_base_path)
    if parsed.scheme != "hdfs":
        raise ValueError(f"HDFS_RAW_JOBS_PATH must be an hdfs:// URI, got: {hdfs_raw_base_path}")

    hdfs_dest_dir = f"{hdfs_raw_base_path}/ingest_date={ingest_date}"
    hdfs_dest_file = f"{hdfs_dest_dir}/{_make_hdfs_raw_filename()}"

    spark = (
        SparkSession.builder
        .appName("copy-topcv-crawler-output-to-hdfs")
        .getOrCreate()
    )

    try:
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            jvm.java.net.URI.create(hdfs_raw_base_path),
            conf,
        )

        fs.mkdirs(jvm.org.apache.hadoop.fs.Path(hdfs_dest_dir))
        fs.copyFromLocalFile(
            False,
            True,
            jvm.org.apache.hadoop.fs.Path(str(local_file)),
            jvm.org.apache.hadoop.fs.Path(hdfs_dest_file),
        )
    finally:
        spark.stop()

    print(f"[batch_crawler] wrote {line_count} raw records to {hdfs_dest_file}")
    print(f"[HDFS] copied local crawler output: {local_file} -> {hdfs_dest_file}")

    return hdfs_dest_file


def main() -> None:
    run_crawler.main()

    local_output_path = topcv_crawler.DATA_FILE
    hdfs_raw_base_path = os.getenv(
        "HDFS_RAW_JOBS_PATH",
        "hdfs://hdfs-namenode.hdfs.svc.cluster.local:9000/raw/jobs",
    )

    copy_local_jsonl_to_hdfs(local_output_path, hdfs_raw_base_path)


if __name__ == "__main__":
    main()
