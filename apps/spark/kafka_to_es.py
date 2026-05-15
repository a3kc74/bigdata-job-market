"""
Spark Structured Streaming — Kafka -> Elasticsearch

Standalone job that reads raw messages from Kafka and indexes each job event
into Elasticsearch for full-text search and Kibana visualization.

Usage:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    apps/spark/kafka_to_es.py
"""

import json
import os
import ssl
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split


# ── Config ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "my-cluster-kafka-bootstrap.kafka.svc:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "my-topic")
ES_URL = os.getenv("ES_URL", "http://elasticsearch.streaming.svc:9200")
ES_INDEX = os.getenv("ES_INDEX", "jobs_streaming")


# ── Elasticsearch helpers ───────────────────────────────────────────────

def bulk_index(docs):
    """Send documents to Elasticsearch using the _bulk API."""
    if not docs:
        return

    lines = []
    for doc in docs:
        lines.append(json.dumps({"index": {"_index": ES_INDEX}}))
        lines.append(json.dumps(doc, ensure_ascii=False))

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
                print(f"[ES] bulk errors: {body[:500]}")
            else:
                print(f"[ES] indexed {len(docs)} docs -> {ES_INDEX}")
    except Exception as e:
        print(f"[ES] ERROR: {e}")


def write_to_es(batch_df, batch_id):
    """
    foreachBatch sink: parse each micro-batch and index into Elasticsearch.
    """
    rows = [json.loads(x) for x in batch_df.toJSON().collect()]
    print(f"[ES] batch_id={batch_id}, rows={len(rows)}")
    bulk_index(rows)


# ── Spark pipeline ──────────────────────────────────────────────────────

spark = SparkSession.builder.appName("kafka-to-es").getOrCreate()
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
    .option("checkpointLocation", "/tmp/checkpoints/kafka_to_es")
    .foreachBatch(write_to_es)
    .start()
)

query.awaitTermination()