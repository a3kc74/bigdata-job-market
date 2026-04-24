"""
Cassandra + Elasticsearch sinks for speed-layer results.

Replaces Redis with:
- Cassandra: durable storage for windowed aggregates and real-time job events
- Elasticsearch: full-text search and Kibana dashboards

Layer: GOLD -> Speed Views (Cassandra + Elasticsearch)
"""

import json
import os
import ssl
import urllib.request
from datetime import datetime, timezone

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()

# ── Cassandra config ────────────────────────────────────────────────────
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_USERNAME = os.getenv("CASSANDRA_USERNAME", "cassandra")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "job_market")
CASSANDRA_TTL = int(os.getenv("CASSANDRA_TTL_SECONDS", "7200"))

# ── Elasticsearch config ────────────────────────────────────────────────
ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX_JOBS = os.getenv("ES_INDEX_JOBS", "jobs_realtime")
ES_INDEX_JOB_COUNTS = os.getenv("ES_INDEX_JOB_COUNTS", "job_counts_10m")
ES_INDEX_SKILL_COUNTS = os.getenv("ES_INDEX_SKILL_COUNTS", "skill_counts_30m")


# ═══════════════════════════════════════════════════════════════════════
# Cassandra helpers
# ═══════════════════════════════════════════════════════════════════════

def _get_cassandra_session():
    """Create and return a Cassandra session for the configured keyspace."""
    auth_provider = PlainTextAuthProvider(
        username=CASSANDRA_USERNAME,
        password=CASSANDRA_PASSWORD,
    )
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        auth_provider=auth_provider,
    )
    session = cluster.connect()
    
    # Create keyspace if it does not exist
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(CASSANDRA_KEYSPACE)
    
    return cluster, session


def _ensure_cassandra_tables(session):
    """
    Create Cassandra tables if they do not exist.

    Tables:
    - job_counts_10m: windowed job count by city
    - skill_counts_30m: windowed skill count
    - jobs_realtime: individual normalized job events
    """
    session.execute("""
        CREATE TABLE IF NOT EXISTS job_counts_10m (
            window_start  TIMESTAMP,
            window_end    TIMESTAMP,
            location_city TEXT,
            count         INT,
            ingested_at   TIMESTAMP,
            PRIMARY KEY ((window_start, window_end), location_city)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS skill_counts_30m (
            window_start TIMESTAMP,
            window_end   TIMESTAMP,
            skill        TEXT,
            count        INT,
            ingested_at  TIMESTAMP,
            PRIMARY KEY ((window_start, window_end), skill)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS jobs_realtime (
            job_id           TEXT,
            source           TEXT,
            source_url       TEXT,
            job_title        TEXT,
            company_name     TEXT,
            salary_text      TEXT,
            salary_min       BIGINT,
            salary_max       BIGINT,
            location_text    TEXT,
            location_city    TEXT,
            skills_text      TEXT,
            description_text TEXT,
            event_ts         TIMESTAMP,
            ingest_ts        TIMESTAMP,
            ingested_at      TIMESTAMP,
            PRIMARY KEY (job_id)
        )
    """)


# ═══════════════════════════════════════════════════════════════════════
# Elasticsearch helpers
# ═══════════════════════════════════════════════════════════════════════

def _es_bulk_index(index_name: str, docs: list[dict]):
    """Send documents to Elasticsearch using the _bulk API."""
    if not docs:
        return

    lines = []
    for doc in docs:
        lines.append(json.dumps({"index": {"_index": index_name}}))
        lines.append(json.dumps(doc, ensure_ascii=False, default=str))

    payload = ("\n".join(lines) + "\n").encode("utf-8")
    req = urllib.request.Request(
        f"{ES_URL}/_bulk",
        data=payload,
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )

    context = None
    if ES_URL.startswith("https://"):
        context = ssl._create_unverified_context()

    try:
        with urllib.request.urlopen(req, context=context) as resp:
            body = resp.read().decode("utf-8")
            result = json.loads(body)
            if result.get("errors"):
                print(f"[ES] bulk errors for {index_name}: {body[:500]}")
            else:
                print(f"[ES] indexed {len(docs)} docs -> {index_name}")
    except Exception as e:
        print(f"[ES] ERROR indexing to {index_name}: {e}")


# ═══════════════════════════════════════════════════════════════════════
# foreachBatch sinks
# ═══════════════════════════════════════════════════════════════════════

def write_job_counts(batch_df, batch_id: int):
    """
    foreachBatch sink for job_counts_10m.

    Writes each (window, location_city) -> count to both
    Cassandra and Elasticsearch.
    """
    if batch_df.isEmpty():
        print(f"[job_counts] empty batch {batch_id}")
        return

    rows = batch_df.collect()
    now = datetime.now(timezone.utc)

    # ── Cassandra ───────────────────────────────────────────────────────
    cluster, session = _get_cassandra_session()
    _ensure_cassandra_tables(session)

    prepared = session.prepare(f"""
        INSERT INTO job_counts_10m
            (window_start, window_end, location_city, count, ingested_at)
        VALUES (?, ?, ?, ?, ?)
        USING TTL {CASSANDRA_TTL}
    """)

    for row in rows:
        session.execute(prepared, (
            row["window"]["start"],
            row["window"]["end"],
            row["location_city"] or "unknown",
            int(row["count"]),
            now,
        ))

    session.shutdown()
    cluster.shutdown()

    # ── Elasticsearch ───────────────────────────────────────────────────
    es_docs = []
    for row in rows:
        es_docs.append({
            "window_start": str(row["window"]["start"]),
            "window_end": str(row["window"]["end"]),
            "location_city": row["location_city"] or "unknown",
            "count": int(row["count"]),
            "ingested_at": now.isoformat(),
        })
    _es_bulk_index(ES_INDEX_JOB_COUNTS, es_docs)

    print(f"[job_counts] wrote batch {batch_id} -> Cassandra + ES ({len(rows)} rows)")


def write_skill_counts(batch_df, batch_id: int):
    """
    foreachBatch sink for skill_counts_30m.

    Writes each (window, skill) -> count to both
    Cassandra and Elasticsearch.
    """
    if batch_df.isEmpty():
        print(f"[skill_counts] empty batch {batch_id}")
        return

    rows = batch_df.collect()
    now = datetime.now(timezone.utc)

    # ── Cassandra ───────────────────────────────────────────────────────
    cluster, session = _get_cassandra_session()
    _ensure_cassandra_tables(session)

    prepared = session.prepare(f"""
        INSERT INTO skill_counts_30m
            (window_start, window_end, skill, count, ingested_at)
        VALUES (?, ?, ?, ?, ?)
        USING TTL {CASSANDRA_TTL}
    """)

    for row in rows:
        session.execute(prepared, (
            row["window"]["start"],
            row["window"]["end"],
            row["skill"],
            int(row["count"]),
            now,
        ))

    session.shutdown()
    cluster.shutdown()

    # ── Elasticsearch ───────────────────────────────────────────────────
    es_docs = []
    for row in rows:
        es_docs.append({
            "window_start": str(row["window"]["start"]),
            "window_end": str(row["window"]["end"]),
            "skill": row["skill"],
            "count": int(row["count"]),
            "ingested_at": now.isoformat(),
        })
    _es_bulk_index(ES_INDEX_SKILL_COUNTS, es_docs)

    print(f"[skill_counts] wrote batch {batch_id} -> Cassandra + ES ({len(rows)} rows)")


def write_jobs_realtime(batch_df, batch_id: int):
    """
    foreachBatch sink for the main normalized stream.

    Writes each individual job event to Cassandra (durable store)
    and Elasticsearch (search/visualization).
    """
    if batch_df.isEmpty():
        print(f"[jobs_realtime] empty batch {batch_id}")
        return

    rows = batch_df.collect()
    now = datetime.now(timezone.utc)

    # ── Cassandra ───────────────────────────────────────────────────────
    cluster, session = _get_cassandra_session()
    _ensure_cassandra_tables(session)

    prepared = session.prepare("""
        INSERT INTO jobs_realtime
            (job_id, source, source_url, job_title, company_name,
             salary_text, salary_min, salary_max,
             location_text, location_city, skills_text,
             description_text, event_ts, ingest_ts, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    for row in rows:
        # Avoid KeyError if salary missing or null
        sal_min = int(row["salary_min"]) if getattr(row, "salary_min", None) is not None else None
        sal_max = int(row["salary_max"]) if getattr(row, "salary_max", None) is not None else None
        
        session.execute(prepared, (
            row["job_id"],
            getattr(row, "source", None),
            getattr(row, "source_url", None),
            getattr(row, "job_title", None),
            getattr(row, "company_name", None),
            getattr(row, "salary_text", None),
            sal_min,
            sal_max,
            getattr(row, "location_text", None),
            getattr(row, "location_city", None),
            getattr(row, "skills_text", None),
            getattr(row, "description_text", None),
            getattr(row, "event_ts", None),
            getattr(row, "ingest_ts", None),
            now,
        ))

    session.shutdown()
    cluster.shutdown()

    # ── Elasticsearch ───────────────────────────────────────────────────
    es_docs = []
    for row in rows:
        doc = row.asDict()
        doc["ingested_at"] = now.isoformat()
        
        # Convert datetime to ISO string for Elasticsearch
        if doc.get("event_ts"):
            doc["event_ts"] = doc["event_ts"].isoformat()
        if doc.get("ingest_ts"):
            doc["ingest_ts"] = doc["ingest_ts"].isoformat()
            
        # Remove raw_json to save space
        doc.pop("raw_json", None)
        es_docs.append(doc)
        
    _es_bulk_index(ES_INDEX_JOBS, es_docs)

    print(f"[jobs_realtime] wrote batch {batch_id} -> Cassandra + ES ({len(rows)} rows)")
