"""
Spark Batch ETL: Raw JSONL (HDFS) -> Bronze Parquet (HDFS)

Data Flow:
    raw/jobs/ingest_date=YYYY-MM-DD/  ->  bronze/jobs/ingest_date=YYYY-MM-DD/
    Steps: flatten, cast timestamps, merge skills, add metadata, dedup.

Run locally:
    spark-submit raw_to_bronze.py --date 2026-04-30
    spark-submit raw_to_bronze.py

Trigger on Kubernetes:
    kubectl create job --from=cronjob/batch-etl-raw-to-bronze manual-DATE -n spark

Docs:
    docs/raw_to_bronze_runbook.md  - full run guide
    docs/spark_on_minikube.md      - Spark + Minikube ops
    docs/hdfs_data_ingestion.md    - loading raw data into HDFS
"""
import argparse
import logging
import sys
from pathlib import Path

# Configure project root
project_root = Path(__file__).resolve().parents[3]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from configs.logger import get_logger
from configs.settings import settings

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    MapType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType
)

# HDFS NameNode service URL inside Kubernetes:
#   Format: hdfs://<service-name>.<namespace>.svc:<port>
#   'hdfs-namenode' = K8s Service name of HDFS NameNode pod
#   'hdfs'          = namespace where HDFS is deployed
#   9000            = default HDFS RPC port
# For local spark-submit (outside K8s): use hdfs://localhost:9000
RAW_BASE_PATH    = settings.RAW_PATH
BRONZE_BASE_PATH = settings.BRONZE_PATH

# Config logging
logger = get_logger("raw_to_bronze")

### RAW JSON SCHEMA, TYPE CAST WHEN READING DATA
# Schema in file 'payload'
# Schema for nested company_details object
COMPANY_DETAILS_SCHEMA = StructType([
    StructField("scale",    StringType(), True),
    StructField("field",    StringType(), True),
    StructField("address",  StringType(), True)
])

PAYLOAD_SCHEMA = StructType([
    StructField("title",                 StringType(),              True), # Third element = Allow null or not
    StructField("company_name",          StringType(),              True),
    StructField("company_details",       COMPANY_DETAILS_SCHEMA,    True), # Nested: {scale, field, address}
    StructField("salary",                StringType(),              True),
    StructField("location",              ArrayType(StringType()),   True),
    StructField("monthOfExperience",     StringType(),              True), # Mixed type: Integer (3) or String ("Không yêu cầu") → read as String
    StructField("deadline",              LongType(),                True), # Long (Unix ms)
    StructField("occupationalCategory",  StringType(),              True),
    StructField("education",             StringType(),              True),
    StructField("employmentType",        StringType(),              True),
    StructField("openings",              IntegerType(),             True), # Integer in raw
    StructField("description",           StringType(),              True), # Multi-line text, '\n' separated
    StructField("requirements",          StringType(),              True), # Multi-line text, '\n' separated
    StructField("benefits",              StringType(),              True), # Multi-line text, '\n' separated
    StructField("income",                ArrayType(StringType()),   True),
    StructField("schedule",              StringType(),              True),
    StructField("skillsNeeded",          ArrayType(StringType()),   True),
    StructField("skillsShouldHave",      ArrayType(StringType()),   True),
    StructField("specialty",             ArrayType(StringType()),   True),
    StructField("extra_inf",             StringType(),              True),
    StructField("meta_tags",             MapType(StringType(), StringType()), True),
    # json_ld is NOT in this schema — extracted separately via get_json_object
    # because raw data has json_ld as a nested JSON object, not a string.
    StructField("pageText",              StringType(),              True)
])

QUALITY_FLAGS_SCHEMA = MapType(StringType(), BooleanType())

RAW_SCHEMA = StructType([
    StructField("source",                   StringType(),           True),
    StructField("source_url",               StringType(),           True),
    StructField("normalized_source_url",    StringType(),           True),
    StructField("crawl_version",            IntegerType(),          True),
    StructField("ingest_ts",                LongType(),             True),
    StructField("event_ts",                 LongType(),             True),
    StructField("job_id",                   StringType(),           False),
    StructField("hash_content",             StringType(),           True),
    StructField("payload",                  PAYLOAD_SCHEMA,         True),
    StructField("quality_flags",            QUALITY_FLAGS_SCHEMA,   True)
])

### Helpers
def ms_to_timestamp(col_name: str):
    """Convert Unix ms (Long) to timestamp type."""
    return (F.col(col_name) / 1000).cast(TimestampType())


def parse_crawl_domain(col_name: str):
    """Parse domain from URL."""
    return F.parse_url(F.col(col_name), F.lit("HOST"))

### Helpers — null handling
# String fields from raw payload that should be empty string instead of null in Bronze.
# Array/Map/Timestamp fields are excluded (handled separately with coalesce or cast).
_STRING_FIELDS_PAYLOAD = [
    "title", "company_name", "company_scale", "company_field",
    "company_address", "salary", "monthOfExperience",
    "occupationalCategory", "education", "employmentType",
    "openings", "description", "requirements", "benefits",
    "schedule", "extra_inf", "json_ld", "pageText",
]

def fillna_str(col_expr, alias_name: str):
    """Replace null String with empty string ''."""
    return F.coalesce(col_expr, F.lit("")).alias(alias_name)


### CORE TRANSFORMATION
def transform_raw_to_bronze(raw_df):
    """Use DataFrame (from JSONL), return Bronze DataFrame."""
    p = "payload"                # Alias for payload column

    bronze = raw_df.select(
        # From raw root
        F.col("source"),
        F.col("source_url"),
        F.col("normalized_source_url"),
        F.col('crawl_version'),
        ms_to_timestamp("ingest_ts").alias("ingest_ts"), # Name the results column
        ms_to_timestamp("event_ts").alias("event_ts"),
        F.col("job_id"),
        F.col("hash_content"),

        # Payload data — String fields: null → ""
        fillna_str(F.col(f"{p}.title"),            "title"),
        fillna_str(F.col(f"{p}.company_name"),     "company_name"),

        # company_details: nested object → flatten to company_scale/field/address
        fillna_str(F.col(f"{p}.company_details.scale"),    "company_scale"),
        fillna_str(F.col(f"{p}.company_details.field"),    "company_field"),
        fillna_str(F.col(f"{p}.company_details.address"),  "company_address"),

        fillna_str(F.col(f"{p}.salary"),           "salary"),
        F.col(f"{p}.location"),

        # monthOfExperience: mixed type in raw (Integer 3 or String "Không yêu cầu")
        # Schema reads as StringType → both coerced to string automatically
        fillna_str(F.col(f"{p}.monthOfExperience"), "monthOfExperience"),

        ms_to_timestamp(f"{p}.deadline").alias("deadline"),

        fillna_str(F.col(f"{p}.occupationalCategory"), "occupationalCategory"),
        fillna_str(F.col(f"{p}.education"),             "education"),
        fillna_str(F.col(f"{p}.employmentType"),        "employmentType"),

        # openings: Integer in raw → cast to String for Bronze
        F.coalesce(F.col(f"{p}.openings").cast(StringType()), F.lit("")).alias("openings"),

        # description/requirements/benefits: String (multi-line, '\n' separated)
        fillna_str(F.col(f"{p}.description"),    "description"),
        fillna_str(F.col(f"{p}.requirements"),   "requirements"),
        fillna_str(F.col(f"{p}.benefits"),        "benefits"),
        F.col(f"{p}.income"),
        fillna_str(F.col(f"{p}.schedule"),  "schedule"),

        # skills - merge skillsNeeded and skillsShouldHave (remove null + dedup)
        F.array_distinct(
            F.concat(
                F.coalesce(F.col(f"{p}.skillsNeeded"),     F.array()),
                F.coalesce(F.col(f"{p}.skillsShouldHave"), F.array())
            )
        ).alias("skills"),

        F.col(f"{p}.specialty"),
        fillna_str(F.col(f"{p}.extra_inf"),  "extra_inf"),
        F.col(f"{p}.meta_tags"),

        # json_ld: extracted separately via get_json_object (see run())
        # because raw data stores it as a nested JSON object, not a string.
        fillna_str(F.col("_json_ld_str"),    "json_ld"),

        fillna_str(F.col(f"{p}.pageText"),   "pageText"),

        F.col("quality_flags"),

        ### New columns for bronze data
        F.lit(False).alias("is_deleted"),
        parse_crawl_domain("source_url").alias("crawl_domain"),

        # Count metrics
        # description/requirements/benefits are strings → count lines by splitting on '\n'
        F.size(F.split(F.coalesce(F.col(f"{p}.description"),  F.lit("")), "\n")).alias("description_count"),
        F.size(F.split(F.coalesce(F.col(f"{p}.requirements"), F.lit("")), "\n")).alias("requirements_count"),
        F.size(F.split(F.coalesce(F.col(f"{p}.benefits"),     F.lit("")), "\n")).alias("benefits_count"),
        F.size(F.coalesce(F.col(f"{p}.income"),        F.array())).alias("income_count"),

        F.size(
            F.array_distinct(
                F.concat(
                    F.coalesce(F.col(f"{p}.skillsNeeded"),     F.array()),
                    F.coalesce(F.col(f"{p}.skillsShouldHave"), F.array())
                )
            )
        ).alias("skills_count"),

        F.size(F.coalesce(F.col(f"{p}.specialty"), F.array())).alias("specialty_count"),

        # Partition column
        F.date_format(ms_to_timestamp("ingest_ts"), "yyyy-MM-dd").alias("ingest_date")
    )

    # With each job_id, only keep the record with the lastest ingest_ts 
    # record_version: the version of page being crawled
    bronze = bronze.withColumn(
        "record_version",
        # dense_rank: ranking number, if the values ​​are the same, they receive the same number
        F.dense_rank().over(
            Window.partitionBy("job_id").orderBy("ingest_ts")
        ).cast(IntegerType())
    )

    return bronze


def dedup_bronze(bronze_df):
    """With each (job_id, hash_content), only keep the lastest record."""
    w = Window.partitionBy("job_id", "hash_content").orderBy(F.col("ingest_ts").desc())
    return (
        bronze_df
        .withColumn("_rn", F.row_number().over(w))   # row_number: numbering rows
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def build_spark():
    """Initialize Spark Session - entry point of Spark"""
    return (
        SparkSession.builder
        .appName("raw_to_bronze")
        .config("spark.sql.parquet.compression.codec", "snappy")  # Use snappy compression to write data
        .config("spark.sql.shuffle.partitions", "200")            # Number of partitions used for parallel processing
        # Spark only overwrites partitions with ingest_date that are included in raw_df
        .config('spark.sql.sources.partitionOverWriteMode', 'dynamic')
        .getOrCreate()     # If exists -> use, if not -> create new
    )


def run(date: str | None = None):
    """Run the whole pipeline ETL - Transform raw to bronze data."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")    # Only log from WARN or above

    if date:
        raw_path = f"{RAW_BASE_PATH}/ingest_date={date}/"
        bronze_path = f"{BRONZE_BASE_PATH}/ingest_date={date}/"
        logger.info(f"Incremental load: date={date}")
    else:
        raw_path = f"{RAW_BASE_PATH}/"
        bronze_path = f"{BRONZE_BASE_PATH}/"
        logger.info("Full load: all dates...")

    logger.info(f"Reading raw data from {raw_path}...")

    # Read raw JSONL data as text first, then parse.
    # Two-step read because json_ld is a nested JSON object in raw,
    # which cannot be captured by StringType() in a fixed schema.
    # Step 1: Read each line as a raw JSON string
    raw_text = spark.read.text(raw_path)

    # Step 2: Parse structured fields with from_json + extract json_ld separately
    raw_df = (
        raw_text
        .select(
            F.from_json(F.col("value"), RAW_SCHEMA).alias("data"),
            F.get_json_object(F.col("value"), "$.payload.json_ld").alias("_json_ld_str")
        )
        .select("data.*", "_json_ld_str")
    )

    total_raw = raw_df.count()
    logger.info(f"Raw records read: {total_raw:,} lines.")

    # Remove corrupted record
    valid_df = raw_df.filter(F.col("job_id").isNotNull())
    logger.info(f"Valid records (job_id not null): {valid_df.count():,} lines")

    # Transform to bronze
    bronze_df = transform_raw_to_bronze(valid_df)

    # Dedup
    bronze_df = dedup_bronze(bronze_df)
    logger.info(f"Bronze records after dedup: {bronze_df.count():,} lines")

    # Save in parquet-like HDFS
    logger.info(f"Writing bronze data to {bronze_path}...")
    bronze_df.write \
        .mode("append") \
        .partitionBy("ingest_date") \
        .parquet(bronze_path)
    logger.info("Pipeline: tranform raw data to bronze data completed successfully!")
    
    spark.stop()   # End Spark session


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Raw to Bronze ETL")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date format: YYYY-MM-DD. Leave blank to run at full load."
    )
    arg = parser.parse_args()    # Receive parameter input from user
    run(date=arg.date)