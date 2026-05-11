"""Cassandra and Elasticsearch sinks for realtime salary-bin aggregates."""

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
ES_INDEX_SALARY_BINS_HOURLY = os.getenv(
    "ES_INDEX_SALARY_BINS_HOURLY",
    "realtime_salary_bins_hourly_v1",
)


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
        CREATE TABLE IF NOT EXISTS realtime_salary_bins_hourly (
            bucket_date date,
            window_start timestamp,
            window_end timestamp,
            city text,
            level text,
            salary_bin text,
            job_count bigint,
            avg_salary_min_million double,
            avg_salary_max_million double,
            median_salary_million double,
            updated_at timestamp,
            PRIMARY KEY ((bucket_date, window_start), city, level, salary_bin)
        )
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
            INSERT INTO realtime_salary_bins_hourly (
                bucket_date,
                window_start,
                window_end,
                city,
                level,
                salary_bin,
                job_count,
                avg_salary_min_million,
                avg_salary_max_million,
                median_salary_million,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        for row in rows:
            session.execute(
                statement,
                (
                    row["bucket_date"],
                    row["window_start"],
                    row["window_end"],
                    row["city"],
                    row["level"],
                    row["salary_bin"],
                    int(row["job_count"]),
                    row["avg_salary_min_million"],
                    row["avg_salary_max_million"],
                    row["median_salary_million"],
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
                row["city"],
                row["level"],
                row["salary_bin"],
            ]
        )
        lines.append(json.dumps({"index": {"_index": ES_INDEX_SALARY_BINS_HOURLY, "_id": doc_id}}))
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
            raise RuntimeError(f"Elasticsearch bulk index reported errors for {ES_INDEX_SALARY_BINS_HOURLY}")


def write_salary_bins_hourly(batch_df, batch_id: int) -> None:
    """Write hourly salary-bin aggregate rows to Cassandra and Elasticsearch."""

    if batch_df.isEmpty():
        print(f"[salary_bins_hourly] empty batch {batch_id}")
        return

    rows = [row.asDict(recursive=True) for row in batch_df.collect()]
    try:
        _write_cassandra(rows)
        print(f"[salary_bins_hourly] wrote {len(rows)} rows to Cassandra in batch {batch_id}")
    except Exception as exc:
        print(f"[salary_bins_hourly] Cassandra write failed in batch {batch_id}: {exc}")

    try:
        _write_elasticsearch(rows)
        print(f"[salary_bins_hourly] indexed {len(rows)} rows to Elasticsearch in batch {batch_id}")
    except Exception as exc:
        print(f"[salary_bins_hourly] Elasticsearch write failed in batch {batch_id}: {exc}")
