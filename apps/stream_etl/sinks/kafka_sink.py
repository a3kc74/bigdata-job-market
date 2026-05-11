"""Kafka sink helpers for clean and dead-letter streams."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_jobs_to_kafka(clean_df: DataFrame) -> DataFrame:
    payload_cols = [column for column in clean_df.columns if column != "raw_json"]
    return clean_df.select(
        F.col("job_id").cast("string").alias("key"),
        F.to_json(F.struct(*[F.col(column) for column in payload_cols])).alias("value"),
    )


def dead_letter_to_kafka(dead_letter_df: DataFrame) -> DataFrame:
    return dead_letter_df.select(
        F.col("dead_letter_key").cast("string").alias("key"),
        F.to_json(
            F.struct(
                "raw_json",
                "error_reason",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "created_at",
            )
        ).alias("value"),
    )
