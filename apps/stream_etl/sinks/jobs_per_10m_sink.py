"""Elasticsearch sink for jobs-per-10m aggregates."""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import date, datetime
from typing import Any


ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX_JOB_COUNTS_10M = os.getenv(
    "ES_INDEX_JOB_COUNTS_10M",
    "realtime_job_counts_10m_v1",
)


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _bulk_index(index_name: str, rows: list[dict], id_fields: list[str]) -> None:
    if not rows:
        return

    lines: list[str] = []
    for row in rows:
        doc_id = "|".join(str(row[field]) for field in id_fields)
        lines.append(json.dumps({"index": {"_index": index_name, "_id": doc_id}}))
        lines.append(json.dumps(row, ensure_ascii=False, default=_json_default))

    request = urllib.request.Request(
        f"{ES_URL.rstrip('/')}/_bulk",
        data=("\n".join(lines) + "\n").encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.loads(response.read().decode("utf-8"))
        if result.get("errors"):
            raise RuntimeError(f"Elasticsearch bulk index reported errors for {index_name}")


def write_jobs_per_10m(batch_df, batch_id: int) -> None:
    """Write one jobs-per-10m micro-batch to Elasticsearch."""

    if batch_df.isEmpty():
        print(f"[jobs_per_10m] empty batch {batch_id}")
        return

    rows = [row.asDict(recursive=True) for row in batch_df.collect()]
    _bulk_index(
        ES_INDEX_JOB_COUNTS_10M,
        rows,
        ["window_start", "source", "city", "category"],
    )
    print(
        f"[jobs_per_10m] indexed {len(rows)} rows to "
        f"{ES_INDEX_JOB_COUNTS_10M} in batch {batch_id}"
    )
