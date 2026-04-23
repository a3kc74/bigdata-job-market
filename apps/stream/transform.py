"""
Validation, cleaning, and normalization logic.

- validate_raw_events(...) = Kafka raw bytes -> parsed rows + Kafka metadata
- clean_events(...)        = filter invalid rows, parse timestamps, basic trimming
- normalize_events(...)    = normalize business fields for downstream streaming

Layers in intent:
RAW SOURCE -> VALIDATE -> CLEAN -> NORMALIZE
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_raw_events(raw_kafka_df: DataFrame, schema) -> DataFrame:
    """
    Validate and parse raw Kafka messages into structured rows.

    Keeps Kafka metadata and adds a parse flag so downstream stages
    can decide what to keep or reject.
    """
    return (
        raw_kafka_df.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS raw_json",
            "topic AS kafka_topic",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
            "timestamp AS kafka_ts",
        )
        .withColumn("payload", F.from_json("raw_json", schema))
        .withColumn("is_parse_ok", F.col("payload").isNotNull())
        .select(
            "kafka_key",
            "raw_json",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_ts",
            "is_parse_ok",
            "payload.*",
        )
        .withColumn("validated_ingest_ts", F.current_timestamp())
    )


def clean_events(validated_df: DataFrame) -> DataFrame:
    """
    Clean validated rows.

    Steps:
    - keep only successfully parsed events
    - require job_id
    - parse event_ts / ingest_ts
    - fall back to Kafka timestamp when event_ts is missing
    - trim common text fields
    """
    return (
        validated_df.filter(F.col("is_parse_ok") == True)
        .filter(F.col("job_id").isNotNull())
        .withColumn(
            "event_ts", F.coalesce(F.to_timestamp("event_ts"), F.col("kafka_ts"))
        )
        .withColumn(
            "ingest_ts",
            F.coalesce(F.to_timestamp("ingest_ts"), F.col("validated_ingest_ts")),
        )
        .withColumn("title", F.trim(F.coalesce(F.col("title"), F.lit(""))))
        .withColumn(
            "company_name", F.trim(F.coalesce(F.col("company_name"), F.lit("")))
        )
        .withColumn(
            "location_text",
            F.trim(F.coalesce(F.col("location_text"), F.lit("unknown"))),
        )
        .withColumn("salary_text", F.trim(F.coalesce(F.col("salary_text"), F.lit(""))))
        .withColumn("skills_text", F.trim(F.coalesce(F.col("skills_text"), F.lit(""))))
        .withColumn(
            "description_text", F.trim(F.coalesce(F.col("description_text"), F.lit("")))
        )
        .withColumn("source", F.trim(F.coalesce(F.col("source"), F.lit("unknown"))))
        .filter(F.col("event_ts").isNotNull())
    )


def normalize_events(clean_df: DataFrame) -> DataFrame:
    """
    Normalize business fields into a stream-ready shape.

    Steps:
    - normalize location names
    - extract salary_min / salary_max from free-text salary
    - convert skills_text into a clean array
    - standardize title/company/source fields
    """
    text_salary = F.lower(F.coalesce(F.col("salary_text"), F.lit("")))
    first_num = F.regexp_extract(text_salary, r"(\d+)", 1).cast("int")
    second_num = F.regexp_extract(text_salary, r"(\d+)\D+(\d+)", 2).cast("int")

    multiplier = F.when(text_salary.contains("usd"), F.lit(25000)).otherwise(
        F.lit(1000000)
    )

    location_key = F.lower(F.trim(F.coalesce(F.col("location_text"), F.lit("unknown"))))

    skills_array_expr = F.expr("""
        filter(
          transform(
            split(
              regexp_replace(coalesce(skills_text, ''), '\\\\s*,\\\\s*', ','),
              ','
            ),
            x -> trim(x)
          ),
          x -> x <> ''
        )
        """)

    return (
        clean_df.withColumn(
            "location_city",
            F.when(
                location_key.isin("hn", "ha noi", "hà nội", "hanoi"), F.lit("Ha Noi")
            )
            .when(
                location_key.isin(
                    "hcm",
                    "tp hcm",
                    "ho chi minh",
                    "hồ chí minh",
                    "ho chi minh city",
                ),
                F.lit("Ho Chi Minh City"),
            )
            .otherwise(F.initcap(location_key)),
        )
        .withColumn("salary_min", first_num * multiplier)
        .withColumn(
            "salary_max",
            F.when(second_num.isNotNull(), second_num * multiplier).otherwise(
                first_num * multiplier
            ),
        )
        .withColumn("skills", skills_array_expr)
        .withColumn("job_title", F.col("title"))
        .withColumn("company_name", F.initcap(F.col("company_name")))
        .withColumn("source", F.lower(F.col("source")))
        .select(
            "job_id",
            "source",
            "source_url",
            "job_title",
            "company_name",
            "salary_text",
            "salary_min",
            "salary_max",
            "location_text",
            "location_city",
            "skills_text",
            "skills",
            "description_text",
            "event_ts",
            "ingest_ts",
            "raw_json",
        )
    )
