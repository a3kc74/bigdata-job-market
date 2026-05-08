import json
import os
from typing import List

from pymongo import MongoClient, ReplaceOne
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def build_schema() -> StructType:
    return StructType([
        StructField("source", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("normalized_source_url", StringType(), True),
        StructField("crawl_version", IntegerType(), True),
        StructField("ingest_ts", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("hash_content", StringType(), True),
        StructField("payload", StructType([
            StructField("domain", StringType(), True),
            StructField("fetch_method", StringType(), True),
            StructField("title", StringType(), True),
            StructField("company_name", StringType(), True),
            StructField("salary", StringType(), True),
            StructField("experience", StringType(), True),
            StructField("deadline", StringType(), True),
            StructField("description", StringType(), True),
            StructField("location", StringType(), True),
        ]), True),
    ])


def normalize_text(col_expr):
    return F.trim(F.lower(F.regexp_replace(F.coalesce(col_expr, F.lit("")), r"\s+", " ")))


def classify_work_mode(title_col, desc_col):
    txt = F.lower(F.concat_ws(" ", F.coalesce(title_col, F.lit("")), F.coalesce(desc_col, F.lit(""))))
    return (
        F.when(txt.rlike(r"\bremote\b|làm việc từ xa|work from home|wfh|online"), F.lit("remote"))
        .when(txt.rlike(r"\bhybrid\b"), F.lit("hybrid"))
        .otherwise(F.lit("onsite"))
    )


def extract_province(location_col):
    cleaned = F.regexp_replace(F.coalesce(location_col, F.lit("")), r'[\[\]\"]', '')
    return F.trim(F.regexp_extract(cleaned, r'-\s*([^:,\]]+)', 1))


def bulk_upsert(df: DataFrame, collection, id_field="_id", batch_size=500):
    ops: List[ReplaceOne] = []

    for row_json in df.toJSON().toLocalIterator():
        doc = json.loads(row_json)
        if id_field not in doc or not doc[id_field]:
            continue

        ops.append(
            ReplaceOne(
                {id_field: doc[id_field]},
                doc,
                upsert=True
            )
        )

        if len(ops) >= batch_size:
            collection.bulk_write(ops, ordered=False)
            ops = []

    if ops:
        collection.bulk_write(ops, ordered=False)


def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    bronze_path = os.getenv("BRONZE_PATH", "s3a://bronze/job_postings/")
    mongo_uri = os.getenv("MONGODB_URI", "mongodb://admin:admin123@mongodb:27017/admin")
    mongo_db_name = os.getenv("MONGODB_DATABASE", "job_market")

    batch_df.persist()

    # 1) Ghi Bronze
    bronze_out = batch_df.select(
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_json",
        "source",
        "source_url",
        "normalized_source_url",
        "crawl_version",
        "ingest_ts",
        "event_ts",
        "job_id",
        "hash_content",
        "payload_json",
        "job_title",
        "company_name",
        "salary_raw",
        "deadline_raw",
        "description_raw",
        "location_raw",
        "work_mode",
        "province",
        "bronze_ingest_date"
    )

    (
        bronze_out.write
        .mode("append")
        .partitionBy("bronze_ingest_date", "source")
        .parquet(bronze_path)
    )

    # 2) Chuẩn bị kết nối MongoDB
    client = MongoClient(mongo_uri)
    db = client[mongo_db_name]

    # 3) realtime_recent_jobs
    recent_jobs_df = (
        batch_df
        .withColumn("_id", F.concat_ws("_", F.col("source"), F.col("job_id")))
        .select(
            "_id",
            "job_id",
            "source",
            "source_url",
            "normalized_source_url",
            F.col("job_title").alias("title"),
            "company_name",
            "province",
            "work_mode",
            F.col("salary_raw").alias("salary_text_raw"),
            "deadline_raw",
            "ingest_ts",
            "event_ts",
            "kafka_timestamp",
            "hash_content"
        )
    )

    bulk_upsert(recent_jobs_df, db["realtime_recent_jobs"])

    # 4) realtime_job_stats_5m
    stats_df = (
        batch_df
        .withColumn("event_time_for_speed", F.coalesce(F.col("ingest_ts"), F.col("event_ts"), F.col("kafka_timestamp")))
        .groupBy(
            F.window("event_time_for_speed", "5 minutes").alias("w"),
            "source",
            "province",
            "work_mode"
        )
        .agg(
            F.countDistinct("job_id").alias("job_count")
        )
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_end", F.col("w.end"))
        .withColumn(
            "_id",
            F.concat_ws(
                "_",
                F.date_format(F.col("w.start"), "yyyy-MM-dd_HH-mm-ss"),
                F.coalesce(F.col("source"), F.lit("unknown")),
                F.coalesce(F.col("province"), F.lit("unknown")),
                F.coalesce(F.col("work_mode"), F.lit("unknown"))
            )
        )
        .select(
            "_id",
            "window_start",
            "window_end",
            "source",
            "province",
            "work_mode",
            "job_count"
        )
    )

    bulk_upsert(stats_df, db["realtime_job_stats_5m"])

    client.close()
    batch_df.unpersist()


def main():
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap.kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "job-postings")
    checkpoint_path = os.getenv("SPEED_CHECKPOINT_PATH", "s3a://bronze/checkpoints/speed_job/")

    spark = (
        SparkSession.builder
        .appName("speed-job")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = build_schema()

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    base_df = raw_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    parsed_df = base_df.withColumn("parsed", F.from_json(F.col("raw_json"), schema))

    enriched_df = (
        parsed_df
        .filter(F.col("parsed").isNotNull())
        .withColumn("source", F.col("parsed.source"))
        .withColumn("source_url", F.col("parsed.source_url"))
        .withColumn("normalized_source_url", F.col("parsed.normalized_source_url"))
        .withColumn("crawl_version", F.col("parsed.crawl_version"))
        .withColumn("ingest_ts", F.to_timestamp(F.col("parsed.ingest_ts")))
        .withColumn("event_ts", F.to_timestamp(F.col("parsed.event_ts")))
        .withColumn("job_id", F.coalesce(F.col("parsed.job_id"), F.col("kafka_key")))
        .withColumn("hash_content", F.col("parsed.hash_content"))
        .withColumn("payload_json", F.to_json(F.col("parsed.payload")))
        .withColumn("job_title", F.col("parsed.payload.title"))
        .withColumn("company_name", F.col("parsed.payload.company_name"))
        .withColumn("salary_raw", F.col("parsed.payload.salary"))
        .withColumn("deadline_raw", F.col("parsed.payload.deadline"))
        .withColumn("description_raw", F.col("parsed.payload.description"))
        .withColumn("location_raw", F.col("parsed.payload.location"))
        .withColumn("work_mode", classify_work_mode(F.col("parsed.payload.title"), F.col("parsed.payload.description")))
        .withColumn("province", extract_province(F.col("parsed.payload.location")))
        .withColumn("title_normalized", normalize_text(F.col("parsed.payload.title")))
        .withColumn("bronze_ingest_date", F.to_date(F.coalesce(F.col("ingest_ts"), F.col("event_ts"), F.col("kafka_timestamp"))))
        .drop("parsed")
    )

    query = (
        enriched_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("update")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()