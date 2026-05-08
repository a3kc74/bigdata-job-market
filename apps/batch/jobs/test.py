"""
Spark Batch ETL: Bronze Parquet (HDFS) -> Silver Parquet (HDFS)

data/bronze/jobs/ingest_date=YYYY-MM-DD/ -> data/silver/jobs/ingest_date=YYYY-MM-DD/

Run locally:
    spark-submit bronze_to_silver.py --date 2026-04-29   (incremental)
    spark-submit bronze_to_silver.py                     (full load)

Trigger on Kubernetes:
    kubectl create job --from=cronjob/batch-etl-bronze-to-silver manual-DATE -n spark

Docs:
    data/silver/silver_data_format.md -> Silver data schema
"""
import argparse
import logging

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType

BRONZE_PATH = "hdfs://hdfs-namenode.hdfs.svc:9000/bronze/jobs"
SILVER_PATH = "hdfs://hdfs-namenode.hdfs.svc:9000/silver/jobs"

# Exchange rate
USD_TO_VND = 25_000

# Config logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bronze_to_silver")


# ---------------------------------------------------------------------------
# Step 1: Parse json_ld → ld_* fields
#
# Outlier cases handled per field:
#
# [salary]
#   Case A: baseSalary always exists. Negotiable salary means $.baseSalary.value.minValue and $.baseSalary.value.maxValue are absent (null).
#   Case B: currency = USD                     → salary_min/max_vnd convert × USD_TO_VND
#   Case C: unitText = YEAR                    → ÷ 12 in canonicalize_salary()
#
# [experience_required]
#   If monthOfExperience = "Thỏa thuận" -> experience_required = False, else True
#
# [jobLocation]
#   Case A: field absent (remote job)          → ld_work_* = null; check ld_job_location_type
#   Case B: single Place object                → get_json_object works directly
#   Case C: Array of Place objects             → $.jobLocation.address fails;
#                                                 → fallback: $.jobLocation[0].address.*
# ---------------------------------------------------------------------------

def parse_json_ld(df):
    """
    Extract structured fields from raw json_ld string using get_json_object.
    No fixed StructType schema — resilient to TopCV JSON-LD schema evolution.
    """
    jl = F.col("json_ld")

    # jobLocation: try single Object first, fallback to Array[0] if null
    def work_addr(field):
        single = F.get_json_object(jl, f"$.jobLocation.address.{field}")
        array0 = F.get_json_object(jl, f"$.jobLocation[0].address.{field}")
        return F.coalesce(single, array0)

    return df.withColumns({
        "ld_deadline":              F.to_timestamp(F.get_json_object(jl, "$.validThrough")),
        "ld_company_url":           F.get_json_object(jl, "$.hiringOrganization.sameAs"),
        "ld_company_logo":          F.get_json_object(jl, "$.hiringOrganization.logo"),
        "ld_work_country":          work_addr("addressCountry"),
        "ld_job_location_type":     F.get_json_object(jl, "$.jobLocationType"),
        "ld_salary_currency":       F.get_json_object(jl, "$.baseSalary.currency"),
        "ld_salary_min":            F.get_json_object(jl, "$.baseSalary.value.minValue").cast(DoubleType()),
        "ld_salary_max":            F.get_json_object(jl, "$.baseSalary.value.maxValue").cast(DoubleType()),
        "ld_salary_unit":           F.get_json_object(jl, "$.baseSalary.value.unitText"),
        "ld_job_id_platform":       F.get_json_object(jl, "$.identifier.value"),
        "ld_occupational_category": F.get_json_object(jl, "$.occupationalCategory"),
    })


# ---------------------------------------------------------------------------
# Step 2: Resolve experience_required
# ---------------------------------------------------------------------------

def resolve_experience(df):
    """
    experience_required = False if monthOfExperience == "Thỏa thuận", else True
    """
    return df.withColumn(
        "experience_required",
        F.when(F.lower(F.col("monthOfExperience")).contains("thỏa thuận"), F.lit(False))
         .otherwise(F.lit(True))
    )


# ---------------------------------------------------------------------------
# Step 3: Salary canonical → salary_min_vnd, salary_max_vnd, salary_is_negotiable
#
# Outlier handling:
#   - ld_salary_min/max = null (negotiable salary)    → salary_min/max_vnd = null
#   - salary string "Thỏa thuận"                       → salary_is_negotiable = true
#   - Regex fallback: "10 - 15 Triệu"                  → min=10M, max=15M VND
#   - If both json_ld and regex fail                    → null (acceptable, downstream handles)
# ---------------------------------------------------------------------------

def canonicalize_salary(df):
    """Normalize salary to VNĐ/month."""
    currency_factor = F.when(
        F.upper(F.col("ld_salary_currency")) == F.lit("USD"),
        F.lit(float(USD_TO_VND))
    ).otherwise(F.lit(1.0))   # VND: no conversion needed

    period_factor = F.when(
        F.upper(F.col("ld_salary_unit")) == F.lit("YEAR"),
        F.lit(1.0 / 12)
    ).otherwise(F.lit(1.0))   # MONTH (default): no change

    ld_min_vnd = (F.col("ld_salary_min") * currency_factor * period_factor).cast(LongType())
    ld_max_vnd = (F.col("ld_salary_max") * currency_factor * period_factor).cast(LongType())

    # Fallback regex on Bronze.salary string
    _RANGE_TRIEU = r"(\d+(?:[.,]\d+)?)\s*-\s*(\d+(?:[.,]\d+)?)\s*[Tt]ri\u1ec7u"
    regex_min = (
        F.regexp_extract(F.col("salary"), _RANGE_TRIEU, 1)
         .cast(DoubleType()) * F.lit(1_000_000.0)
    ).cast(LongType())
    regex_max = (
        F.regexp_extract(F.col("salary"), _RANGE_TRIEU, 2)
         .cast(DoubleType()) * F.lit(1_000_000.0)
    ).cast(LongType())

    return df.withColumns({
        "salary_min_vnd":       F.coalesce(ld_min_vnd, F.when(regex_min > 0, regex_min)),
        "salary_max_vnd":       F.coalesce(ld_max_vnd, F.when(regex_max > 0, regex_max)),
        # salary_is_negotiable: true khi salary string chứa "Thỏa thuận" hoặc cả min và max đều null (vì baseSalary luôn tồn tại)
        "salary_is_negotiable": (
            F.col("salary").rlike(r"(?i)th\u1ecfa\s*thu\u1eadn") |
            (F.col("ld_salary_min").isNull() & F.col("ld_salary_max").isNull())
        ),
    })


# ---------------------------------------------------------------------------
# Step 4: Location canonical → location_count, location_detail, has_remote
# ---------------------------------------------------------------------------

def process_location(df):
    """
    Parse location_detail directly from the location array.
    has_remote: derived from ld_job_location_type = "TELECOMMUTE".
    """
    location_detail_expr = F.transform(
        F.coalesce(F.col("location"), F.array()),
        lambda x: F.struct(
            F.trim(F.split(x, ":", 2).getItem(0)).alias("city"),
            F.when(F.size(F.split(x, ":", 2)) > 1, F.trim(F.split(x, ":", 2).getItem(1)))
             .otherwise(F.lit(None).cast("string")).alias("address")
        )
    )

    df = df.withColumn("location_count",      F.size(F.col("location")))
    df = df.withColumn("location_detail",     location_detail_expr)
    df = df.withColumn("has_remote",          F.col("ld_job_location_type") == F.lit("TELECOMMUTE"))
    return df


# ---------------------------------------------------------------------------
# Dedup: 1 record per job_id (highest record_version)
# ---------------------------------------------------------------------------

def dedup_silver(df):
    """Keep the latest snapshot per job_id."""
    w = Window.partitionBy("job_id").orderBy(F.col("record_version").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ---------------------------------------------------------------------------
# Full transformation pipeline
# ---------------------------------------------------------------------------

def transform_bronze_to_silver(bronze_df):
    df = parse_json_ld(bronze_df)
    df = resolve_experience(df)
    df = canonicalize_salary(df)
    df = process_location(df)
    return df


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------

def build_spark():
    return (
        SparkSession.builder
        .appName("bronze_to_silver")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "200")
        # Dynamic partition overwrite: only replace partitions being written.
        # Without this, mode("overwrite") would delete ALL Silver data.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(date: str | None = None):
    """Run the full Bronze → Silver ETL pipeline."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    if date:
        bronze_path = f"{BRONZE_PATH}/ingest_date={date}/"
        silver_path = f"{SILVER_PATH}/ingest_date={date}/"
        logger.info(f"Incremental load: date={date}")
    else:
        bronze_path = f"{BRONZE_PATH}/"
        silver_path = f"{SILVER_PATH}/"
        logger.info("Full load: all dates...")

    logger.info(f"Reading Bronze data from {bronze_path}...")
    bronze_df = spark.read.parquet(bronze_path)
    logger.info(f"Bronze records read: {bronze_df.count():,}")

    silver_df = transform_bronze_to_silver(bronze_df)
    silver_df = dedup_silver(silver_df)
    logger.info(f"Silver records after dedup: {silver_df.count():,}")

    logger.info(f"Writing Silver data to {silver_path}...")
    (
        silver_df.write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(silver_path)
    )
    logger.info("Pipeline: bronze_to_silver completed successfully!")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze to Silver ETL")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date format: YYYY-MM-DD. Leave blank to run full load."
    )
    arg = parser.parse_args()
    run(date=arg.date)
