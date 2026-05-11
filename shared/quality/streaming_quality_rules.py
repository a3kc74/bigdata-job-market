"""Validation rules for speed-layer raw job events."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_quality_errors(parsed_df: DataFrame) -> DataFrame:
    """Attach a compact validation error reason to parsed Kafka rows."""

    return parsed_df.withColumn(
        "error_reason",
        F.when(~F.col("is_parse_ok"), F.lit("malformed_json"))
        .when(F.col("job_id").isNull() | (F.length(F.trim(F.col("job_id"))) == 0), F.lit("missing_job_id"))
        .when(F.col("payload").isNull(), F.lit("missing_payload"))
        .when(F.col("payload.title").isNull() | (F.length(F.trim(F.col("payload.title"))) == 0), F.lit("missing_title"))
        .when(F.col("event_ts").isNull() & F.col("kafka_timestamp").isNull(), F.lit("missing_event_time"))
        .otherwise(F.lit(None).cast("string")),
    )


def valid_records(df: DataFrame) -> DataFrame:
    return df.filter(F.col("error_reason").isNull())


def invalid_records(df: DataFrame) -> DataFrame:
    return df.filter(F.col("error_reason").isNotNull())
