"""PySpark schema for raw TopCV crawler events in Kafka `jobs_raw`."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


COMPANY_DETAILS_SCHEMA = StructType(
    [
        StructField("scale", StringType(), True),
        StructField("field", StringType(), True),
        StructField("address", StringType(), True),
    ]
)

PAYLOAD_SCHEMA = StructType(
    [
        StructField("title", StringType(), True),
        StructField("company_name", StringType(), True),
        StructField("company_details", COMPANY_DETAILS_SCHEMA, True),
        StructField("salary", StringType(), True),
        StructField("location", ArrayType(StringType()), True),
        StructField("monthOfExperience", IntegerType(), True),
        StructField("deadline", LongType(), True),
        StructField("occupationalCategory", StringType(), True),
        StructField("education", StringType(), True),
        StructField("employmentType", StringType(), True),
        StructField("openings", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("requirements", StringType(), True),
        StructField("income", ArrayType(StringType()), True),
        StructField("benefits", StringType(), True),
        StructField("extra_inf", StringType(), True),
        StructField("schedule", StringType(), True),
        StructField("skillsNeeded", ArrayType(StringType()), True),
        StructField("skillsShouldHave", ArrayType(StringType()), True),
        StructField("specialty", ArrayType(StringType()), True),
        StructField("pageText", StringType(), True),
    ]
)

QUALITY_FLAGS_SCHEMA = StructType(
    [
        StructField("has_json_ld", BooleanType(), True),
        StructField("has_page_text", BooleanType(), True),
        StructField("has_structured_company_name_conflict", BooleanType(), True),
        StructField("has_valid_posting_date", BooleanType(), True),
        StructField("has_valid_deadline", BooleanType(), True),
        StructField("has_salary_info", BooleanType(), True),
        StructField("has_location_info", BooleanType(), True),
        StructField("has_experience_info", BooleanType(), True),
        StructField("has_requirements", BooleanType(), True),
        StructField("has_description", BooleanType(), True),
        StructField("has_benefits", BooleanType(), True),
        StructField("has_skills_info", BooleanType(), True),
        StructField("has_education_info", BooleanType(), True),
        StructField("has_specialty", BooleanType(), True),
        StructField("has_schedule", BooleanType(), True),
        StructField("has_employment_type", BooleanType(), True),
        StructField("has_income", BooleanType(), True),
        StructField("has_extra_info", BooleanType(), True),
    ]
)

RAW_JOB_SCHEMA = StructType(
    [
        StructField("source", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("normalized_source_url", StringType(), True),
        StructField("crawl_version", IntegerType(), True),
        StructField("ingest_ts", LongType(), True),
        StructField("event_ts", LongType(), True),
        StructField("original_event_ts", LongType(), True),
        StructField("stream_ingest_ts", LongType(), True),
        StructField("replay_id", StringType(), True),
        StructField("replay_seq", LongType(), True),
        StructField("job_id", StringType(), True),
        StructField("hash_content", StringType(), True),
        StructField("payload", PAYLOAD_SCHEMA, True),
        StructField("quality_flags", QUALITY_FLAGS_SCHEMA, True),
    ]
)
