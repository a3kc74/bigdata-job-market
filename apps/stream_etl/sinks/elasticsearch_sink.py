"""Elasticsearch sink used by phase 3 for `jobs_realtime_v1`."""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime, timezone


ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX_JOBS = os.getenv("ES_INDEX_JOBS", "realtime_jobs_v1")


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _bulk_index(index_name: str, docs: list[dict]) -> None:
    if not docs:
        return

    lines: list[str] = []
    for doc in docs:
        doc_id = doc.get("job_id")
        action = {"index": {"_index": index_name}}
        if doc_id:
            action["index"]["_id"] = str(doc_id)
        lines.append(json.dumps(action))
        lines.append(json.dumps(doc, ensure_ascii=False, default=_json_default))

    request = urllib.request.Request(
        f"{ES_URL}/_bulk",
        data=("\n".join(lines) + "\n").encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.loads(response.read().decode("utf-8"))
        if result.get("errors"):
            raise RuntimeError(f"Elasticsearch bulk index reported errors for {index_name}")


def write_jobs_realtime(batch_df, batch_id: int) -> None:
    """Write a micro-batch of clean jobs to Elasticsearch."""

    if batch_df.isEmpty():
        print(f"[jobs_realtime_v1] empty batch {batch_id}")
        return

    docs = []
    indexed_at = datetime.now(timezone.utc).isoformat()
    for row in batch_df.drop("raw_json").collect():
        doc = row.asDict(recursive=True)
        doc["indexed_at"] = indexed_at
        docs.append(doc)

    try:
        _bulk_index(ES_INDEX_JOBS, docs)
        print(f"[jobs_realtime_v1] indexed {len(docs)} docs in batch {batch_id}")
    except Exception as exc:
        print(f"[jobs_realtime_v1] Elasticsearch write failed in batch {batch_id}: {exc}")
