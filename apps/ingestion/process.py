"""
Maps raw crawler output to the RAW_EVENT_SCHEMA defined in schemas.py.

Takes the dict produced by crawler.parse_job_posting() or
StreamingCrawler.stream() and normalizes it into the flat,
all-StringType schema that the Kafka → Spark pipeline expects.

Schema fields (from schemas.RAW_EVENT_SCHEMA):
    job_id, source, source_url, title, company_name,
    salary_text, location_text, skills_text, description_text,
    event_ts, ingest_ts
"""

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict


def process_crawled_data(raw_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Transform raw crawler output into the schema expected by Kafka / Spark.

    Args:
        raw_data: Dict from crawler.parse_job_posting() or
                  StreamingCrawler.stream(). Expected keys include:
                  source_url, domain, title, company_name, salary,
                  location, skills (list), description, requirements,
                  benefits, crawled_at.

    Returns:
        Dict with all keys from RAW_EVENT_SCHEMA, all values as str.
    """
    source_url = raw_data.get("source_url", "")

    # Generate a deterministic job_id by hashing the source URL
    job_id = hashlib.md5(source_url.encode("utf-8")).hexdigest() if source_url else ""

    # Extract source domain, strip "www."
    domain = raw_data.get("domain", "")
    source = domain.replace("www.", "") if domain else ""

    # Convert skills list → comma-separated string
    skills = raw_data.get("skills", [])
    skills_text = ", ".join(skills) if isinstance(skills, list) else str(skills)

    # Merge description + requirements + benefits into one description_text
    description = raw_data.get("description", "")
    requirements = raw_data.get("requirements", "")
    benefits = raw_data.get("benefits", "")

    full_description = description
    if requirements:
        full_description += f"\n\nYêu cầu công việc:\n{requirements}"
    if benefits:
        full_description += f"\n\nQuyền lợi:\n{benefits}"

    # Timestamps
    event_ts = raw_data.get("crawled_at", datetime.now(timezone.utc).isoformat())
    ingest_ts = datetime.now(timezone.utc).isoformat()

    processed_data = {
        "job_id": job_id,
        "source": source,
        "source_url": source_url,
        "title": raw_data.get("title", ""),
        "company_name": raw_data.get("company_name", ""),
        "salary_text": raw_data.get("salary", ""),
        "location_text": raw_data.get("location", ""),
        "skills_text": skills_text,
        "description_text": full_description.strip(),
        "event_ts": event_ts,
        "ingest_ts": ingest_ts,
    }

    # Schema requires all fields to be StringType — ensure no None values
    for key in processed_data:
        if processed_data[key] is None:
            processed_data[key] = ""
        else:
            processed_data[key] = str(processed_data[key])

    return processed_data


if __name__ == "__main__":
    import json
    import os
    import sys

    # Add project root to sys.path so we can import from apps.ingestion
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    try:
        from apps.ingestion.crawler import parse_job_posting
    except ImportError:
        print("Cannot import crawler. Run from the project root directory.")
        sys.exit(1)

    url = input("Enter a job posting URL to test: ").strip()
    if url:
        print("Crawling...")
        raw_data, _ = parse_job_posting(url)
        print("Done crawling! Processing data...")

        processed_data = process_crawled_data(raw_data)

        print("\nProcessed data (ready for Kafka):")
        print(json.dumps(processed_data, ensure_ascii=False, indent=2))
