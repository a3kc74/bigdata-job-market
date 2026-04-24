"""
Spark Structured Streaming — Kafka -> Cassandra

Standalone job that reads raw messages from Kafka and writes each job event
into a Cassandra table for durable, low-latency reads.

Usage:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    apps/spark/kafka_to_cassandra.py
"""

import json
import os
from datetime import datetime, timezone

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split


# ── Config ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "my-cluster-kafka-bootstrap.kafka.svc:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "my-topic")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "demo-dc1-service.k8ssandra-operator.svc")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_USERNAME = os.getenv("CASSANDRA_USERNAME", "cassandra")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "demo_jobs")


# ── Cassandra helpers ───────────────────────────────────────────────────

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


def _ensure_table(session):
    """Create the jobs_streaming table if it does not exist."""
    session.execute("""
        CREATE TABLE IF NOT EXISTS jobs_streaming (
            job_id     TEXT,
            ingested_at TIMESTAMP,
            job_title  TEXT,
            city       TEXT,
            raw_value  TEXT,
            PRIMARY KEY (job_id, ingested_at)
        ) WITH CLUSTERING ORDER BY (ingested_at DESC)
    """)


def write_to_cassandra(batch_df, batch_id):
    """
    foreachBatch sink: parse each micro-batch and insert into Cassandra.
    """
    rows = [json.loads(x) for x in batch_df.toJSON().collect()]
    print(f"[cassandra] batch_id={batch_id}, rows={len(rows)}")

    if not rows:
        return

    cluster, session = _get_cassandra_session()
    _ensure_table(session)

    prepared = session.prepare("""
        INSERT INTO jobs_streaming (job_id, ingested_at, job_title, city, raw_value)
        VALUES (?, ?, ?, ?, ?)
    """)

    now = datetime.now(timezone.utc)

    for row in rows:
        session.execute(prepared, (
            row.get("job_id"),
            now,
            row.get("job_title"),
            row.get("city"),
            row.get("raw_value"),
        ))

    session.shutdown()
    cluster.shutdown()
    print(f"[cassandra] wrote batch {batch_id} ({len(rows)} rows)")


# ── Spark pipeline ──────────────────────────────────────────────────────

spark = SparkSession.builder.appName("kafka-to-cassandra").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Demo message format:
# job_1|Data Engineer|Ha Noi
parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING) AS raw_value")
    .select(
        split(col("raw_value"), "\\|").getItem(0).alias("job_id"),
        split(col("raw_value"), "\\|").getItem(1).alias("job_title"),
        split(col("raw_value"), "\\|").getItem(2).alias("city"),
        col("raw_value")
    )
)

query = (
    parsed_df.writeStream
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/kafka_to_cassandra")
    .foreachBatch(write_to_cassandra)
    .start()
)

query.awaitTermination()
