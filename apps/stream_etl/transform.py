"""Parse, validate, and normalize raw speed-layer Kafka records."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from apps.stream_etl.normalizers import (
    epoch_ms_to_timestamp,
    normalize_city,
    normalize_skills,
    salary_bin,
    salary_max_million,
    salary_min_million,
)
from apps.stream_etl.schemas.raw_job_schema import RAW_JOB_SCHEMA
from shared.quality.streaming_quality_rules import with_quality_errors


def parse_raw_kafka(raw_kafka_df: DataFrame) -> DataFrame:
    """Parse Kafka key/value bytes into typed raw job rows plus metadata."""

    return (
        raw_kafka_df.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS raw_json",
            "topic AS kafka_topic",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
            "timestamp AS kafka_timestamp",
        )
        .withColumn("parsed", F.from_json(F.col("raw_json"), RAW_JOB_SCHEMA))
        .withColumn("is_parse_ok", F.col("parsed").isNotNull())
        .select(
            "kafka_key",
            "raw_json",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "is_parse_ok",
            "parsed.*",
        )
    )


def validate_raw_jobs(parsed_df: DataFrame) -> DataFrame:
    return with_quality_errors(parsed_df)


def build_clean_jobs(validated_df: DataFrame) -> DataFrame:
    """Flatten valid raw events into the phase-3 clean stream schema."""

    valid_df = validated_df.filter(F.col("error_reason").isNull())
    event_time = F.coalesce(epoch_ms_to_timestamp(F.col("event_ts")), F.col("kafka_timestamp"))
    ingest_time = F.coalesce(epoch_ms_to_timestamp(F.col("ingest_ts")), F.col("kafka_timestamp"))
    stream_ingest_time = F.coalesce(
        epoch_ms_to_timestamp(F.col("stream_ingest_ts")),
        F.current_timestamp(),
    )
    salary_raw = F.trim(F.coalesce(F.col("payload.salary"), F.lit("")))
    sal_min = salary_min_million(salary_raw)
    sal_max = salary_max_million(salary_raw)
    sal_avg = F.when(sal_min.isNotNull() & sal_max.isNotNull(), (sal_min + sal_max) / F.lit(2.0))

    with_skills_source = valid_df.withColumn(
        "_skills_needed",
        F.coalesce(F.col("payload.skillsNeeded"), F.array().cast("array<string>")),
    ).withColumn(
        "_skills_should_have",
        F.coalesce(F.col("payload.skillsShouldHave"), F.array().cast("array<string>")),
    )

    return (
        with_skills_source.withColumn("event_time", event_time)
        .withColumn("ingest_time", ingest_time)
        .withColumn("stream_ingest_time", stream_ingest_time)
        .withColumn("salary_raw", salary_raw)
        .withColumn("salary_min_million", sal_min)
        .withColumn("salary_max_million", sal_max)
        .withColumn("salary_avg_million", sal_avg)
        .withColumn("salary_bin", salary_bin(F.col("salary_avg_million"), F.col("salary_raw")))
        .withColumn("skills", normalize_skills(F.col("_skills_needed"), F.col("_skills_should_have")))
        .select(
            F.trim(F.col("job_id")).alias("job_id"),
            "hash_content",
            F.lower(F.coalesce(F.col("source"), F.lit("unknown"))).alias("source"),
            "source_url",
            "event_time",
            "ingest_time",
            "stream_ingest_time",
            F.trim(F.coalesce(F.col("payload.title"), F.lit(""))).alias("title"),
            F.trim(F.coalesce(F.col("payload.company_name"), F.lit(""))).alias("company_name"),
            normalize_city(F.col("payload.location")).alias("city"),
            F.coalesce(F.col("payload.location"), F.array().cast("array<string>")).alias("location_raw"),
            F.coalesce(F.col("payload.company_details.field"), F.element_at(F.col("payload.specialty"), 2)).alias("category"),
            F.coalesce(F.col("payload.occupationalCategory"), F.lit("unknown")).alias("level"),
            F.col("payload.employmentType").alias("employment_type"),
            F.col("payload.monthOfExperience").cast("int").alias("experience_months"),
            "salary_raw",
            "salary_min_million",
            "salary_max_million",
            "salary_avg_million",
            "salary_bin",
            F.when(F.lower(F.col("salary_raw")).contains("usd"), F.lit("USD")).otherwise(F.lit("VND")).alias("currency"),
            "skills",
            F.col("payload.description").alias("description"),
            F.col("payload.requirements").alias("requirements"),
            F.col("payload.benefits").alias("benefits"),
            "quality_flags",
            "raw_json",
        )
    )


def build_dead_letter(validated_df: DataFrame) -> DataFrame:
    """Build structured dead-letter rows from invalid raw records."""

    return (
        validated_df.filter(F.col("error_reason").isNotNull())
        .withColumn("created_at", F.current_timestamp())
        .select(
            F.coalesce(F.col("kafka_key"), F.col("job_id"), F.lit("unknown")).alias("dead_letter_key"),
            "raw_json",
            "error_reason",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "created_at",
        )
    )
