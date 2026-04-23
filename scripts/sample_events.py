"""
Sample event generator for the speed layer demo.

Simulates the Data Generation block in the architecture diagram.
In production, this would be replaced by the real TopCV crawler
(apps/ingestion/crawler.py).
"""

import random
from datetime import datetime, timezone
from uuid import uuid4

TITLES = [
    "Data Engineer",
    "Backend Engineer",
    "Machine Learning Engineer",
    "Data Analyst",
    "Platform Engineer",
]

COMPANIES = [
    "ABC Tech",
    "Delta Solutions",
    "NextGen Data",
    "SkyLab",
    "Viet Analytics",
]

LOCATIONS = [
    "Hà Nội",
    "HN",
    "Ho Chi Minh",
    "HCM",
    "Da Nang",
]

SKILL_SETS = [
    ["Python", "Spark", "Kafka", "AWS"],
    ["Java", "Spring Boot", "Redis", "Docker"],
    ["Python", "Airflow", "PostgreSQL"],
    ["Scala", "Spark", "Hadoop"],
    ["Python", "FastAPI", "Redis"],
]

SALARY_TEXTS = [
    "20-35 triệu",
    "25-40 triệu",
    "15-25 triệu",
    "1000-2000 USD",
    "30-50 triệu",
]


def build_event() -> dict:
    """Build a single fake job-posting event."""
    skills = random.choice(SKILL_SETS)
    now = datetime.now(timezone.utc).isoformat()

    return {
        "job_id": str(uuid4()),
        "source": "topcv",
        "source_url": f"https://topcv.vn/jobs/{uuid4().hex[:8]}",
        "title": random.choice(TITLES),
        "company_name": random.choice(COMPANIES),
        "salary_text": random.choice(SALARY_TEXTS),
        "location_text": random.choice(LOCATIONS),
        "skills_text": ",".join(skills),
        "description_text": "Build data pipelines, streaming apps, and analytics services.",
        "event_ts": now,
        "ingest_ts": now,
    }
