"""
Spark Batch ETL: Silver Parquet (HDFS) -> Gold Parquet (HDFS)

Run locally:
    spark-submit silver_to_gold.py --date 2026-04-29 (incremental)
    spark-submit silver_to_gold.py 2026-04-29        (full load)

Trigger on kubenetes:
    kubectl create job --from=cronjob/batch-etl-silver-to-gold manual-DATE -n spark
"""
import argparse
import sys
from pathlib import Path

# Configure project root
project_root = Path(__file__).resolve().parents[3]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from configs.logger import get_logger
from configs.settings import settings

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

SILVER_PATH = settings.SILVER_PATH
GOLD_PATH = settings.GOLD_PATH

logger = get_logger('silver_to_gold')


### 1. Parse 'schedule' field:
# is_weekend_free: True if schedule is null/empty or does not contain "thứ 7", "chủ nhật", "cn", "thứ bảy"
# schedule_type: Categorical value for UI filtering ("T2-T6", "T2-T7", "T2-CN", "Flexible", "Other")
def parse_schedule(df):
    schedule_col = F.lower(F.col("schedule"))

    # Check if 'schedule' null or empty
    is_empty = schedule_col.isNull() | (F.trim(schedule_col) == "")

    # Check weekend keywords
    has_sat = schedule_col.rlike(r"thứ bảy|thứ 7")
    has_sun = schedule_col.rlike(r"chủ nhật|cn")
    has_fri = schedule_col.rlike(r"thứ sáu|thứ 6")

    # 'is_weekend_free' logic
    df = df.withColumn(
        "is_weekend_free",
        F.when(is_empty, F.lit(True))
        .when(~has_sat & ~has_sun, F.lit(True))
        .otherwise(F.lit(False))
    )

    # 'schedule_type' logic
    df = df.withColumn(
        "schedule_type",
        F.when(is_empty, F.lit("Flexible"))
        .when(has_fri & ~has_sat & ~has_sun, F.lit("T2-T6"))
        .when(has_sat & ~has_sun, F.lit("T2-T7"))
        .when(has_sun, F.lit("T2-CN"))
        .otherwise(F.lit("Other"))
    )

    return df

### 2. Clean and normalize data
# Rename job_id_platform to company_id
# Cast monthOfExperience to IntegerType 
def normalize_gold_fields(df):
    df = df.withColumnRenamed("job_id_platform", "company_id")

    # We will convert string value to null since "Không yêu cầu" means no strict experience required
    df = df.withColumn("monthOfExperience", F.col("monthOfExperience").cast(IntegerType()))

    df = df.withColumn("is_active", F.lit(True))

    return df

### 2. Clean skills - Remove "Thu gọn" and "Xem thêm" values
def clean_skills(df):
    # F.expr - write code in SQL-like mode
    df = df.withColumn(
        "skills",
        F.expr("filter(skills, skill -> trim(lower(skill)) NOT IN ('thu gọn', 'xem thêm'))")
    )
    return df


### 3. Select and drop columns
def select_gold_columns(df):
    gold_columns = [
        # Meta data and dates
        "job_id", "company_id", "source_url", "date_posted", "deadline", "ingest_date", "is_active",

        # Support text search
        "title", "company_name", "description", "requirements", "benefits", 

        # Categorical and filters
        "company_field", "work_country", "occupationalCategory", "employmentType", "education",
        "salary_currency", "salary_unit", "skills", "specialty", "location", "location_detail",

        # Booleans
        "has_remote", "experience_required", "salary_is_negotiable", "is_weekend_free",

        # Schedule
        "schedule_type", "schedule",

        # Numerical metrics
        "salary_min_vnd", "salary_max_vnd", "monthOfExperience", "company_scale", "openings",
        "benefits_count", "requirements_count", "location_count", "skills_count", "specialty_count",

        # Raw display
        "salary", "company_logo", "company_address", "company_url"
    ]

    # Ensure we only select columns that exist in the dataframe to avoid errors
    existing_columns = df.columns
    cols_to_select = [c for c in gold_columns if c in existing_columns]

    return df.select(*cols_to_select)


### FULL TRANSFORMATION PIPELINE
def transform_silver_to_gold(silver_df):
    df = parse_schedule(silver_df)
    df = clean_skills(df)
    df = normalize_gold_fields(df)
    df = select_gold_columns(df)
    return df


# Initialize Spark session
def build_spark():
    return (
        SparkSession.builder
        .appName("silver_to_gold")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.sources.partitionOverWriteMode", "dynamic")
        .getOrCreate()
    )


### ENTRY POINT
def run(date: str | None = None):
    """Run the full pipeline: silver -> gold ETL pipeline."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    if date:
        silver_path = f"{SILVER_PATH}/ingest_date={date}/"
        gold_path = f"{GOLD_PATH}/ingest_date={date}/"
        logger.info(f"Incremental load: date={date}...")
    else:
        silver_path = f"{SILVER_PATH}/"
        gold_path = f"{GOLD_PATH}/"
        logger.info(f"Full load: all dates...")
    
    logger.info(f"Reading silver data from {silver_path}...")
    silver_df = spark.read.parquet(silver_path)
    logger.info(f"Silver records read: {len(silver_df)} rows.")

    gold_df = transform_silver_to_gold(silver_df)
    logger.info("Silver data transformed to gold data successfully!")

    logger.info(f"Writing gold data to {gold_path}...")
    (
        gold_df.write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(gold_path)
    )
    logger.info("Pipeline: 'silver_to_gold' pipeline completed successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver to Gold ETF")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date format: YYYY-MM-DD. Leave blank to run full data."
    )
    arg = parser.parse_args()
    run(date=arg.date)