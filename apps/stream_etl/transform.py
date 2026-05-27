"""Parse, validate, and normalize raw speed-layer Kafka records."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from apps.batch.jobs.silver_to_gold import parse_schedule
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


def _count_lines(column: F.Column) -> F.Column:
    return F.size(F.split(F.coalesce(column, F.lit("")), "\n"))


def _location_detail_expr() -> F.Column:
    return F.transform(
        F.coalesce(F.col("payload.location"), F.array().cast("array<string>")),
        lambda x: F.struct(
            F.trim(F.split(x, ":", 2).getItem(0)).alias("city"),
            F.when(F.size(F.split(x, ":", 2)) > 1, F.trim(F.split(x, ":", 2).getItem(1)))
            .otherwise(F.lit(None).cast("string"))
            .alias("address"),
        ),
    )


def _salary_currency_expr(salary_raw: F.Column) -> F.Column:
    text = F.lower(F.trim(F.coalesce(salary_raw, F.lit(""))))
    return (
        F.when(text == "", F.lit(None).cast("string"))
        .when(text.contains("usd"), F.lit("USD"))
        .otherwise(F.lit("VND"))
    )


def _salary_unit_expr(salary_raw: F.Column) -> F.Column:
    text = F.lower(F.trim(F.coalesce(salary_raw, F.lit(""))))
    return (
        F.when(text == "", F.lit(None).cast("string"))
        .when(text.rlike(r"year|/n[aă]m|m[oỗ]i\s*n[aă]m|1\s*n[aă]m"), F.lit("YEAR"))
        .otherwise(F.lit("MONTH"))
    )


def _experience_required_expr() -> F.Column:
    has_exp_info = F.coalesce(F.col("quality_flags.has_experience_info").cast("boolean"), F.lit(False))
    experience_months = F.col("payload.monthOfExperience").cast("int")
    return (
        F.when(experience_months.isNotNull(), F.lit(True))
        .when(has_exp_info, F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def build_clean_jobs(validated_df: DataFrame) -> DataFrame:
    """Flatten valid raw events into a canonical realtime job schema.

    The output intentionally follows Gold-like canonical field names as closely
    as raw Kafka data allows, so batch serving and speed scoring use the same
    logical schema.
    """

    valid_df = validated_df.filter(F.col("error_reason").isNull())
    event_time = F.coalesce(epoch_ms_to_timestamp(F.col("event_ts")), F.col("kafka_timestamp"))
    ingest_time = F.coalesce(epoch_ms_to_timestamp(F.col("ingest_ts")), F.col("kafka_timestamp"))
    stream_ingest_time = F.coalesce(
        epoch_ms_to_timestamp(F.col("stream_ingest_ts")),
        F.current_timestamp(),
    )
    deadline = epoch_ms_to_timestamp(F.col("payload.deadline"))
    ingest_date = F.date_format(ingest_time, "yyyy-MM-dd")

    salary_raw = F.trim(F.coalesce(F.col("payload.salary"), F.lit("")))
    sal_min = salary_min_million(salary_raw)
    sal_max = salary_max_million(salary_raw)
    sal_avg = F.when(sal_min.isNotNull() & sal_max.isNotNull(), (sal_min + sal_max) / F.lit(2.0))
    salary_text = F.lower(salary_raw)
    looks_negotiable = salary_text.rlike(
        r"th\S*a\s*thu\S*n|tho\S*a\s*thu\S*n|negotiable|canh\s*tranh|c\S*nh\s*tranh"
    )
    salary_is_negotiable = sal_min.isNull() & sal_max.isNull() & looks_negotiable
    location_array = F.coalesce(F.col("payload.location"), F.array().cast("array<string>"))
    location_text = F.lower(F.concat_ws(" ", location_array))
    normalized_city = normalize_city(F.col("payload.location"))
    skills = normalize_skills(
        F.coalesce(F.col("payload.skillsNeeded"), F.array().cast("array<string>")),
        F.coalesce(F.col("payload.skillsShouldHave"), F.array().cast("array<string>")),
    )
    specialty = F.coalesce(F.col("payload.specialty"), F.array().cast("array<string>"))
    company_field = F.coalesce(F.col("payload.company_details.field"), F.lit(""))
    company_scale = F.coalesce(F.col("payload.company_details.scale"), F.lit(""))
    company_address = F.coalesce(F.col("payload.company_details.address"), F.lit(""))
    employment_type = F.col("payload.employmentType")
    education = F.col("payload.education")
    occupational_category = F.coalesce(F.col("payload.occupationalCategory"), F.lit("unknown"))
    experience_months = F.col("payload.monthOfExperience").cast("int")
    location_count = F.size(location_array)

    base_df = (
        valid_df.withColumn("event_time", event_time)
        .withColumn("date_posted", event_time)
        .withColumn("ingest_time", ingest_time)
        .withColumn("stream_ingest_time", stream_ingest_time)
        .withColumn("ingest_date", ingest_date)
        .withColumn("deadline", deadline)
        .withColumn("is_active", F.when(deadline.isNull(), F.lit(True)).otherwise(deadline >= F.current_timestamp()))
        .withColumn("salary", salary_raw)
        .withColumn("salary_min_million", sal_min)
        .withColumn("salary_max_million", sal_max)
        .withColumn("salary_avg_million", sal_avg)
        .withColumn("salary_min_vnd", (F.col("salary_min_million") * F.lit(1_000_000)).cast("long"))
        .withColumn("salary_max_vnd", (F.col("salary_max_million") * F.lit(1_000_000)).cast("long"))
        .withColumn("salary_is_negotiable", salary_is_negotiable)
        .withColumn("salary_bin", salary_bin(F.col("salary_avg_million"), F.col("salary")))
        .withColumn("salary_currency", _salary_currency_expr(F.col("salary")))
        .withColumn("salary_unit", _salary_unit_expr(F.col("salary")))
        .withColumn(
            "has_remote",
            location_text.rlike("remote|wfh|work\\s*from\\s*home|táº¡i\\s*nhÃ |lÃ m\\s*viá»‡c\\s*táº¡i\\s*nhÃ "),
        )
        .withColumn("skills", skills)
        .withColumn("specialty", specialty)
        .withColumn("location", location_array)
        .withColumn("location_detail", _location_detail_expr())
        .withColumn("location_count", location_count)
        .withColumn("primary_city", normalized_city)
        .withColumn("company_field", company_field)
        .withColumn("company_scale", company_scale)
        .withColumn("company_address", company_address)
        .withColumn("occupationalCategory", occupational_category)
        .withColumn("employmentType", employment_type)
        .withColumn("education", education)
        .withColumn("monthOfExperience", experience_months)
        .withColumn("experience_required", _experience_required_expr())
        .withColumn("openings", F.col("payload.openings").cast("int"))
        .withColumn("benefits_count", _count_lines(F.col("payload.benefits")))
        .withColumn("requirements_count", _count_lines(F.col("payload.requirements")))
        .withColumn("description_count", _count_lines(F.col("payload.description")))
        .withColumn("skills_count", F.size(F.col("skills")))
        .withColumn("specialty_count", F.size(F.col("specialty")))
        .withColumn("schedule", F.coalesce(F.col("payload.schedule"), F.lit("")))
        .withColumn("source", F.lower(F.coalesce(F.col("source"), F.lit("unknown"))))
    )
    base_df = parse_schedule(base_df)

    return base_df.select(
        F.trim(F.col("job_id")).alias("job_id"),
        "hash_content",
        "source",
        "source_url",
        "date_posted",
        "deadline",
        "ingest_date",
        "is_active",
        "event_time",
        "ingest_time",
        "stream_ingest_time",
        F.trim(F.coalesce(F.col("payload.title"), F.lit(""))).alias("title"),
        F.trim(F.coalesce(F.col("payload.company_name"), F.lit(""))).alias("company_name"),
        F.coalesce(F.col("payload.description"), F.lit("")).alias("description"),
        F.coalesce(F.col("payload.requirements"), F.lit("")).alias("requirements"),
        F.coalesce(F.col("payload.benefits"), F.lit("")).alias("benefits"),
        "company_field",
        "occupationalCategory",
        "employmentType",
        "education",
        "salary_currency",
        "salary_unit",
        "skills",
        "specialty",
        "location",
        "location_detail",
        "has_remote",
        "experience_required",
        "salary_is_negotiable",
        "is_weekend_free",
        "schedule_type",
        "schedule",
        "salary_min_vnd",
        "salary_max_vnd",
        "monthOfExperience",
        "company_scale",
        "openings",
        "benefits_count",
        "requirements_count",
        "location_count",
        "skills_count",
        "specialty_count",
        "salary",
        "company_address",
        "primary_city",
        "salary_min_million",
        "salary_max_million",
        "salary_avg_million",
        "salary_bin",
        "quality_flags",
        "raw_json",
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
