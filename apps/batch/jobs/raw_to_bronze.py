"""
Spark Batch ETL: Raw JSONL (HDFS) -> Bronze Parquet (HDFS)

Data Flow:
    /raw/jobs/ingest_date=YYYY-MM-DD/*.jsonl
        -> flatten payload
        -> cast timestamps (Unix ms -> Timestamp)
        -> merge skills
        -> add metadata (record_version, is_deleted, crawl_domain, ingest_date)
        -> add count metrics
        -> dedup (job_id, hash_content) keep latest ingest_ts
        -> write Parquet /bronze/jobs/ingest_date=YYYY-MM-DD/

=======================================================================
How to run — LOCAL (spark-submit)
=======================================================================
    # Incremental (one day)
    spark-submit raw_to_bronze.py --date 2026-04-30

    # Full load (all partitions)
    spark-submit raw_to_bronze.py

=======================================================================
How to run — KUBERNETES (Minikube)
=======================================================================

--- FIRST TIME SETUP ---

    # 1. Start Minikube
    minikube start --memory=4096 --cpus=4

    # 2. Point Docker CLI to Minikube's daemon (build image inside Minikube)
    eval $(minikube docker-env)          # Linux/macOS
    & minikube -p minikube docker-env --shell powershell | Invoke-Expression  # Windows PowerShell

    # 3. Build Spark image from repo root
    docker build -f infra/spark/Dockerfile -t bigdata-job-market/spark-etl:latest .

    # 4. Apply RBAC (ServiceAccount + RoleBinding)
    kubectl apply -f infra/spark/10-rbac.yaml

    # 5. Deploy CronJob (runs daily at 02:00 AM)
    kubectl apply -f infra/kubernetes/batch-etl-cronjob.yaml

--- TRIGGER JOB MANUALLY ---

    # Run job immediately without waiting for schedule
    kubectl create job --from=cronjob/batch-etl-raw-to-bronze \
        manual-$(date +%Y%m%d) -n spark

--- RESUME AFTER MACHINE RESTART ---

    # 1. Restart Minikube
    minikube start

    # 2. Rebuild image if Dockerfile changed (Minikube loses images on restart)
    eval $(minikube docker-env)
    docker build -f infra/spark/Dockerfile -t bigdata-job-market/spark-etl:latest .

    # 3. CronJob persists — no need to re-apply unless YAML changed
    kubectl get cronjob -n spark

--- MONITORING & LOGS ---

    # List all pods in spark namespace
    kubectl get pods -n spark

    # Stream logs of a running/completed driver pod
    kubectl logs -f <driver-pod-name> -n spark

    # Describe pod for events/errors
    kubectl describe pod <pod-name> -n spark

    # List recent jobs
    kubectl get jobs -n spark

    # Delete a failed/completed job manually
    kubectl delete job <job-name> -n spark

    # Check CronJob schedule
    kubectl get cronjob batch-etl-raw-to-bronze -n spark

    # Minikube dashboard (web UI)
    minikube dashboard
"""
import argparse
import logging

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

RAW_BASE_PATH = "hdfs:///raw/jobs"
BRONZE_BASE_PATH = "hdfs:///bronze/jobs"

# Config logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("raw_to_bronze")

### RAW JSON SCHEMA, TYPE CAST WHEN READING DATA
# Schema in file 'payload'
PAYLOAD_SCHEMA = StructType([
    StructField("title",                 StringType(),              True), # Third element = Allow null or not
    StructField("company_name",          StringType(),              True),
    StructField("company_scale",         StringType(),              True),
    StructField("company_field",         StringType(),              True),
    StructField("company_address",       StringType(),              True),
    StructField("salary",                StringType(),              True),
    StructField("location",              ArrayType(StringType()),   True),
    StructField("monthOfExperience",     StringType(),              True),
    StructField("deadline",              LongType(),                True), # Long (Unix ms)
    StructField("occupationalCategory",  StringType(),              True),
    StructField("education",             StringType(),              True),
    StructField("employmentType",        StringType(),              True),
    StructField("openings",              StringType(),              True),
    StructField("description",           ArrayType(StringType()),   True),
    StructField("requirements",          ArrayType(StringType()),   True),
    StructField("benefits",              ArrayType(StringType()),   True),
    StructField("income",                ArrayType(StringType()),   True),
    StructField("schedule",              StringType(),              True),
    StructField("skillsNeeded",          ArrayType(StringType()),   True),
    StructField("skillsShouldHave",      ArrayType(StringType()),   True),
    StructField("specialty",             ArrayType(StringType()),   True),
    StructField("extra_inf",             StringType(),              True),
    StructField("meta_tags",             MapType(StringType(), StringType()), True),
    StructField("json_ld",               StringType(),              True),
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

        # Payload data
        F.col(f"{p}.title"),
        F.col(f"{p}.company_name"),
        F.col(f"{p}.company_scale"),
        F.col(f"{p}.company_field"),
        F.col(f"{p}.company_address"),
        F.col(f"{p}.salary"),
        F.col(f"{p}.location"),
        F.col(f"{p}.monthOfExperience"),

        ms_to_timestamp(f"{p}.deadline").alias("deadline"),

        F.col(f"{p}.occupationalCategory"),
        F.col(f"{p}.education"),
        F.col(f"{p}.employmentType"),
        F.col(f"{p}.openings"),
        F.col(f"{p}.description"),
        F.col(f"{p}.requirements"),
        F.col(f"{p}.benefits"),
        F.col(f"{p}.income"),
        F.col(f"{p}.schedule"),

        # skills - merge skillsNeeded and skillsShouldHave (remove null + dedup)
        F.array_distinct(
            F.concat(
                F.coalesce(F.col(f"{p}.skillsNeeded"),     F.array()),
                F.coalesce(F.col(f"{p}.skillsShouldHave"), F.array())
            )
        ).alias("skills"),

        F.col(f"{p}.specialty"),
        F.col(f"{p}.extra_inf"),
        F.col(f"{p}.meta_tags"),
        F.col(f"{p}.json_ld"),
        F.col(f"{p}.pageText"),

        F.col("quality_flags"),

        ### New columns for bronze data
        F.lit(False).alias("is_deleted"),
        parse_crawl_domain("source_url").alias("crawl_domain"),

        # Count metrics
        F.size(F.coalesce(F.col(f"{p}.description"),   F.array())).alias("description_count"),
        F.size(F.coalesce(F.col(f"{p}.requirements"),  F.array())).alias("requirements_count"),
        F.size(F.coalesce(F.col(f"{p}.benefits"),      F.array())).alias("benefits_count"),
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

    # Read raw JSONL data
    raw_df = (
        spark.read
        .schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")     # Read data even if encounter erroneous rows
        .option("columnNameOfCorruptRecord", "_corrupt_record")   # Push erroneous rows into '_corrupt_record' column
        .json(raw_path)
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