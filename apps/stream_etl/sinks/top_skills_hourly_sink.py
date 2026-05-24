"""Elasticsearch sink for hourly skill counts and top skills."""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import date, datetime
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql.window import Window


ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX_SKILL_COUNTS_HOURLY = os.getenv(
    "ES_INDEX_SKILL_COUNTS_HOURLY",
    "realtime_skill_counts_hourly_v1",
)
ES_INDEX_TOP_SKILLS_HOURLY = os.getenv(
    "ES_INDEX_TOP_SKILLS_HOURLY",
    "realtime_top_skills_hourly_v1",
)
TOP_N = int(os.getenv("TOP_SKILLS_HOURLY_N", "10"))


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


def write_top_skills_hourly(batch_df, batch_id: int) -> None:
    """Write hourly skill counts plus ranked top skills for each window."""

    if batch_df.isEmpty():
        print(f"[top_skills_hourly] empty batch {batch_id}")
        return

    skill_rows = [row.asDict(recursive=True) for row in batch_df.collect()]

    rank_window = Window.partitionBy("window_start").orderBy(
        F.desc("job_count"),
        F.asc("skill"),
    )
    ranked_df = (
        batch_df.withColumn("rank", F.row_number().over(rank_window))
        .filter(F.col("rank") <= TOP_N)
        .select(
            "bucket_date",
            "window_start",
            "window_end",
            "rank",
            "skill",
            "job_count",
            "updated_at",
        )
    )
    top_rows = [row.asDict(recursive=True) for row in ranked_df.collect()]

    _bulk_index(
        ES_INDEX_SKILL_COUNTS_HOURLY,
        skill_rows,
        ["window_start", "skill"],
    )
    _bulk_index(
        ES_INDEX_TOP_SKILLS_HOURLY,
        top_rows,
        ["window_start", "rank"],
    )
    print(
        f"[top_skills_hourly] indexed {len(skill_rows)} count rows and "
        f"{len(top_rows)} top rows to Elasticsearch in batch {batch_id}"
    )
