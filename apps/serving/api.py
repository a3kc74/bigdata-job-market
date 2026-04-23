"""
Speed Layer — Serving API.

Reads the latest speed views (gold aggregates) from Redis
and exposes them through REST endpoints.

Endpoints:
  GET /health             — health check
  GET /realtime/job-counts — latest 10-min job counts by city
  GET /realtime/top-skills — latest 30-min skill counts (sorted)
"""

import os
from typing import Any, Dict, List

import redis
from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

app = FastAPI(title="Speed Layer API — Big Data Job Market")


def get_redis_client():
    """Create a Redis client with decoded responses."""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )


def get_latest_window(prefix: str) -> Dict[str, Any]:
    """
    Fetch the most recent window from the sorted-set index,
    then return the full hash data for that window.
    """
    client = get_redis_client()
    keys = client.zrevrange(f"index:{prefix}", 0, 0)

    if not keys:
        return {
            "window_key": None,
            "values": {},
        }

    key = keys[0]
    data = client.hgetall(key)

    parts = key.split(":")
    return {
        "window_key": key,
        "window_start": parts[1] if len(parts) > 2 else None,
        "window_end": parts[2] if len(parts) > 2 else None,
        "values": data,
    }


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/realtime/job-counts")
def realtime_job_counts():
    """Return the latest 10-minute job counts grouped by city."""
    return get_latest_window("job_counts_10m")


@app.get("/realtime/top-skills")
def realtime_top_skills(limit: int = 10):
    """Return the top N skills from the latest 30-minute window."""
    payload = get_latest_window("skill_counts_30m")
    raw_values = payload.get("values", {})

    sorted_values: List[Dict[str, Any]] = sorted(
        [{"skill": k, "count": int(v)} for k, v in raw_values.items()],
        key=lambda x: x["count"],
        reverse=True,
    )[:limit]

    payload["values"] = sorted_values
    return payload
