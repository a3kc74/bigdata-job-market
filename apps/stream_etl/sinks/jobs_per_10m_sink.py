"""Cassandra and Elasticsearch sinks for jobs-per-10m aggregates."""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import date, datetime
from typing import Any

from cassandra.cluster import Cluster


CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "job_market_speed")
ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX_JOB_COUNTS_10M = os.getenv("ES_INDEX_JOB_COUNTS_10M", "realtime_job_counts_10m_v1")


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _connect_cassandra():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(CASSANDRA_KEYSPACE)
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS realtime_job_counts_10m (
            bucket_date date,
            window_start timestamp,
            window_end timestamp,
            source text,
            city text,
            category text,
            job_count bigint,
            distinct_company_count bigint,
            updated_at timestamp,
            PRIMARY KEY ((bucket_date), window_start, source, city, category)
        ) WITH CLUSTERING ORDER BY (window_start DESC)
        """
    )
    return cluster, session


def _write_cassandra(rows: list[dict]) -> None:
    if not rows:
        return

    cluster, session = _connect_cassandra()
    try:
        statement = session.prepare(
            """
            INSERT INTO realtime_job_counts_10m (
                bucket_date,
                window_start,
                window_end,
                source,
                city,
                category,
                job_count,
                distinct_company_count,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        for row in rows:
            session.execute(
                statement,
                (
                    row["bucket_date"],
                    row["window_start"],
                    row["window_end"],
                    row["source"],
                    row["city"],
                    row["category"],
                    int(row["job_count"]),
                    int(row["distinct_company_count"]),
                    row["updated_at"],
                ),
            )
    finally:
        session.shutdown()
        cluster.shutdown()


def _write_elasticsearch(rows: list[dict]) -> None:
    if not rows:
        return

    lines: list[str] = []
    for row in rows:
        doc_id = "|".join(
            [
                str(row["window_start"]),
                row["source"],
                row["city"],
                row["category"],
            ]
        )
        lines.append(json.dumps({"index": {"_index": ES_INDEX_JOB_COUNTS_10M, "_id": doc_id}}))
        lines.append(json.dumps(row, ensure_ascii=False, default=_json_default))

    request = urllib.request.Request(
        f"{ES_URL}/_bulk",
        data=("\n".join(lines) + "\n").encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.loads(response.read().decode("utf-8"))
        if result.get("errors"):
            raise RuntimeError(f"Elasticsearch bulk index reported errors for {ES_INDEX_JOB_COUNTS_10M}")


def write_jobs_per_10m(batch_df, batch_id: int) -> None:
    """Write one jobs-per-10m micro-batch to Cassandra and Elasticsearch."""

    if batch_df.isEmpty():
        print(f"[jobs_per_10m] empty batch {batch_id}")
        return

    rows = [row.asDict(recursive=True) for row in batch_df.collect()]
    try:
        _write_cassandra(rows)
        print(f"[jobs_per_10m] wrote {len(rows)} rows to Cassandra in batch {batch_id}")
    except Exception as exc:
        print(f"[jobs_per_10m] Cassandra write failed in batch {batch_id}: {exc}")

    try:
        _write_elasticsearch(rows)
        print(f"[jobs_per_10m] indexed {len(rows)} rows to Elasticsearch in batch {batch_id}")
    except Exception as exc:
        print(f"[jobs_per_10m] Elasticsearch write failed in batch {batch_id}: {exc}")
