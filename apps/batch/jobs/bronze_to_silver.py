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
import sys
from pathlib import Path

# Đảm bảo có thể import module từ thư mục gốc của project
project_root = Path(__file__).resolve().parents[3]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from configs.settings import settings
from configs.logger import get_logger

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType

BRONZE_PATH = settings.BRONZE_PATH
SILVER_PATH = settings.SILVER_PATH

# Config logging
logger = get_logger("bronze_to_silver")


# ---------------------------------------------------------------------------
# Step 1: Parse json_ld → parsed fields
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
#   Case A: field absent (remote job)          -> ld_work_* = null; check ld_job_location_type
#   Case B: single Place object                -> get_json_object works directly
#   Case C: Array of Place objects             -> $.jobLocation.address fails;
#                                              -> fallback: $.jobLocation[0].address.*
# ---------------------------------------------------------------------------


def parse_json_ld(df):
    """
    Extract structured fields from raw JSON_LD string using get_json_object.
    No fixed StructType schema - allow resilient TOPCV JSON_LD schema.
    """
    jl = F.col("json_ld")

    # jobLocation field in json_ld, try single Object first, fallback to Array[0] if null
    def work_address(field):
        single = F.get_json_object(jl, f"$.jobLocation.address.{field}")
        array = F.get_json_object(jl, f"$.jobLocation[0].address.{field}")
        return F.coalesce(single, array)
    
    return df.withColumns({
        "company_url":             F.regexp_replace(F.get_json_object(jl, "$.hiringOrganization.sameAs"), r"\\/", "/"),
        "company_logo":            F.regexp_replace(F.get_json_object(jl, "$.hiringOrganization.logo"), r"\\/", "/"),
        "work_country":            work_address("addressCountry"),
        "job_location_type":       F.get_json_object(jl, "$.jobLocationType"),
        "salary_currency":         F.get_json_object(jl, "$.baseSalary.currency"),
        "salary_min":              F.expr("try_cast(nullif(get_json_object(json_ld, '$.baseSalary.value.minValue'), '') as double)"),
        "salary_max":              F.expr("try_cast(nullif(get_json_object(json_ld, '$.baseSalary.value.maxValue'), '') as double)"),
        "salary_unit":             F.get_json_object(jl, "$.baseSalary.value.unitText"),
        "job_id_platform":         F.get_json_object(jl, "$.identifier.value"),
    })


### 2. Resolve experience_required
def resolve_experience(df):
    """
    experience_required = False if monthOfExperience == "Thỏa thuận" else True
    """
    return df.withColumn(
        "experience_required",
        F.when(F.lower(F.col("monthOfExperience")).contains("thỏa thuận"), F.lit(False))
        .otherwise(F.lit(True))
    )


### 3. Salary canonical -> salary_min_vnd, salary_max_vnd, salary_is_negotiable

# Outlier handling:
#   - salary_min/max = null (negotiable salary)    -> salary_min/max_vnd = null
#   - salary string "Thỏa thuận"                   -> salary_is_negotiable = True
#   - Regex fallback: "10 - 15 Triệu"              -> min=10M, max=15M VND
#   - If both json_ld and regex fail               -> null (acceptable, downstream handles)
def canonicalize_salary(df):
    """Normalize salary to vnd/month."""
    currency_factor = F.when(
        F.upper(F.col("salary_currency")) == F.lit("USD"),
        F.lit(float(settings.USD_TO_VND))
    ).otherwise(F.lit(1.0))      # Help to convert USD -> VND

    period_factor = F.when(
        F.upper(F.col("salary_unit")) == F.lit("YEAR"),
        F.lit(1.0 / 12)
    ).otherwise(F.lit(1.0))      # Help to convert salary/year -> salary/month

    json_min_vnd = (F.col("salary_min") * currency_factor * period_factor).cast(LongType())
    json_max_vnd = (F.col("salary_max") * currency_factor * period_factor).cast(LongType())

    # Fallback regex on Bronze.salary string
    _RANGE_TRIEU = r"(\d+(?:[.,]\d+)?)\s*-\s*(\d+(?:[.,]\d+)?)\s*[Tt]riệu"
    df = (
        df
        .withColumn("_regex_min_raw", F.regexp_replace(F.regexp_extract(F.col("salary"), _RANGE_TRIEU, 1), ",", "."))
        .withColumn("_regex_max_raw", F.regexp_replace(F.regexp_extract(F.col("salary"), _RANGE_TRIEU, 2), ",", "."))
    )

    regex_min = (
        F.expr("try_cast(nullif(_regex_min_raw, '') as double)") * F.lit(1_000_000.0)
    ).cast(LongType())

    regex_max = (
        F.expr("try_cast(nullif(_regex_max_raw, '') as double)") * F.lit(1_000_000.0)
    ).cast(LongType())

    return (
        df.withColumns({
            "salary_min_vnd":         F.coalesce(json_min_vnd, F.when(regex_min > 0, regex_min)),
            "salary_max_vnd":         F.coalesce(json_max_vnd, F.when(regex_max > 0, regex_max)),
            "salary_is_negotiable": (
                F.col("salary").rlike(r"(?i)thỏa\s*thuận") |
                (F.col("salary_min").isNull() & F.col("salary_max").isNull())
            ),
        })
        .drop("_regex_min_raw", "_regex_max_raw")
    )


def canonicalize_salary_v2(df):
    """Normalize salary to VND/month and preserve partial public salary bounds.

    This version fixes cases where display text is "Thoa thuan" but JSON-LD
    still exposes min/max, plus one-sided public salaries such as
    "Tren/Lon hon/Tu 40 trieu" and "Den/Toi/Len den 40 trieu".
    """
    currency_factor = F.when(
        F.upper(F.col("salary_currency")) == F.lit("USD"),
        F.lit(float(settings.USD_TO_VND))
    ).otherwise(F.lit(1.0))

    period_factor = F.when(
        F.upper(F.col("salary_unit")) == F.lit("YEAR"),
        F.lit(1.0 / 12)
    ).otherwise(F.lit(1.0))

    json_min_vnd = (F.col("salary_min") * currency_factor * period_factor).cast(LongType())
    json_max_vnd = (F.col("salary_max") * currency_factor * period_factor).cast(LongType())

    _RANGE_TRIEU = r"(?i)(\d+(?:[.,]\d+)?)\s*-\s*(\d+(?:[.,]\d+)?)\s*(?:tri[eệ]u|tr)"
    _LOWER_BOUND_TRIEU = (
        r"(?i)(?:tr[eê]n|l[oơ]n\s*h[oơ]n|h[oơ]n|t[uừ]|>=|>)\s*"
        r"(\d+(?:[.,]\d+)?)\s*(?:tri[eệ]u|tr)"
    )
    _UPPER_BOUND_TRIEU = (
        r"(?i)(?:d[uư][oớ]i|t[oố]i\s*[dđ]a|l[eê]n\s*[dđ][eế]n|"
        r"[dđ][eế]n|t[oớ]i|up\s*to|<=|<)\s*"
        r"(\d+(?:[.,]\d+)?)\s*(?:tri[eệ]u|tr)"
    )
    _SINGLE_TRIEU = r"(?i)^\s*(\d+(?:[.,]\d+)?)\s*(?:tri[eệ]u|tr)\s*$"

    df = (
        df
        .withColumn("_regex_min_raw", F.regexp_replace(F.regexp_extract(F.col("salary"), _RANGE_TRIEU, 1), ",", "."))
        .withColumn("_regex_max_raw", F.regexp_replace(F.regexp_extract(F.col("salary"), _RANGE_TRIEU, 2), ",", "."))
        .withColumn("_regex_lower_bound_raw", F.regexp_replace(F.regexp_extract(F.col("salary"), _LOWER_BOUND_TRIEU, 1), ",", "."))
        .withColumn("_regex_upper_bound_raw", F.regexp_replace(F.regexp_extract(F.col("salary"), _UPPER_BOUND_TRIEU, 1), ",", "."))
        .withColumn("_regex_single_raw", F.regexp_replace(F.regexp_extract(F.col("salary"), _SINGLE_TRIEU, 1), ",", "."))
    )

    regex_min = (
        F.expr("try_cast(nullif(_regex_min_raw, '') as double)") * F.lit(1_000_000.0)
    ).cast(LongType())
    regex_max = (
        F.expr("try_cast(nullif(_regex_max_raw, '') as double)") * F.lit(1_000_000.0)
    ).cast(LongType())
    regex_lower_bound = (
        F.expr("try_cast(nullif(_regex_lower_bound_raw, '') as double)") * F.lit(1_000_000.0)
    ).cast(LongType())
    regex_upper_bound = (
        F.expr("try_cast(nullif(_regex_upper_bound_raw, '') as double)") * F.lit(1_000_000.0)
    ).cast(LongType())
    regex_single = (
        F.expr("try_cast(nullif(_regex_single_raw, '') as double)") * F.lit(1_000_000.0)
    ).cast(LongType())

    resolved_min_vnd = F.coalesce(
        json_min_vnd,
        F.when(regex_min > 0, regex_min),
        F.when(regex_lower_bound > 0, regex_lower_bound),
        F.when(regex_single > 0, regex_single),
    )
    resolved_max_vnd = F.coalesce(
        json_max_vnd,
        F.when(regex_max > 0, regex_max),
        F.when(regex_upper_bound > 0, regex_upper_bound),
        F.when(regex_single > 0, regex_single),
    )

    has_usable_salary = resolved_min_vnd.isNotNull() | resolved_max_vnd.isNotNull()
    salary_text = F.lower(F.coalesce(F.col("salary"), F.lit("")))
    looks_negotiable = (
        salary_text.rlike(r"thá»a\s*thuáº­n|thoáº£\s*thuáº­n|negotiable|cáº¡nh\s*tranh")
        | (F.trim(salary_text) == "")
    )

    return (
        df.withColumns({
            "salary_min_vnd":         resolved_min_vnd,
            "salary_max_vnd":         resolved_max_vnd,
            "salary_is_negotiable":   (~has_usable_salary) & looks_negotiable,
        })
        .drop(
            "_regex_min_raw",
            "_regex_max_raw",
            "_regex_lower_bound_raw",
            "_regex_upper_bound_raw",
            "_regex_single_raw",
        )
    )


def canonicalize_salary_v3(df):
    """Normalize salary to VND/month with a strict public-salary-first rule.

    Why this exists:
    - Some records display "Thoa thuan" but JSON-LD still has min/max salary.
      In that case JSON-LD is treated as real public salary, so ML must not
      overwrite it.
    - One-sided public salaries such as "Lon hon 40 trieu" still carry useful
      information. We keep the known bound and leave the other side null.
    - Only records with no usable min/max are marked negotiable for prediction.
    """

    currency_factor = F.when(
        F.upper(F.col("salary_currency")) == F.lit("USD"),
        F.lit(float(settings.USD_TO_VND)),
    ).otherwise(F.lit(1.0))

    period_factor = F.when(
        F.upper(F.col("salary_unit")) == F.lit("YEAR"),
        F.lit(1.0 / 12),
    ).otherwise(F.lit(1.0))

    json_min_vnd = (F.col("salary_min") * currency_factor * period_factor).cast(LongType())
    json_max_vnd = (F.col("salary_max") * currency_factor * period_factor).cast(LongType())

    million_unit = r"(?:tri\S*u|tr)"
    range_trieu = rf"(?i)(\d+(?:[.,]\d+)?)\s*-\s*(\d+(?:[.,]\d+)?)\s*{million_unit}"
    lower_bound_trieu = (
        rf"(?i)(?:trên|tren|lớn\s*hơn|lon\s*hon|hơn|hon|từ|tu|>=|>)\s*"
        rf"(\d+(?:[.,]\d+)?)\s*{million_unit}"
    )
    upper_bound_trieu = (
        rf"(?i)(?:dưới|duoi|tối\s*đa|toi\s*da|lên\s*đến|len\s*den|"
        rf"đến|den|tới|toi|up\s*to|<=|<)\s*"
        rf"(\d+(?:[.,]\d+)?)\s*{million_unit}"
    )
    single_trieu = rf"(?i)^\s*(\d+(?:[.,]\d+)?)\s*{million_unit}\s*$"

    df = (
        df.withColumn("_regex_min_raw", F.regexp_replace(F.regexp_extract(F.col("salary"), range_trieu, 1), ",", "."))
        .withColumn("_regex_max_raw", F.regexp_replace(F.regexp_extract(F.col("salary"), range_trieu, 2), ",", "."))
        .withColumn(
            "_regex_lower_bound_raw",
            F.regexp_replace(F.regexp_extract(F.col("salary"), lower_bound_trieu, 1), ",", "."),
        )
        .withColumn(
            "_regex_upper_bound_raw",
            F.regexp_replace(F.regexp_extract(F.col("salary"), upper_bound_trieu, 1), ",", "."),
        )
        .withColumn("_regex_single_raw", F.regexp_replace(F.regexp_extract(F.col("salary"), single_trieu, 1), ",", "."))
    )

    regex_min = (F.expr("try_cast(nullif(_regex_min_raw, '') as double)") * F.lit(1_000_000.0)).cast(LongType())
    regex_max = (F.expr("try_cast(nullif(_regex_max_raw, '') as double)") * F.lit(1_000_000.0)).cast(LongType())
    regex_lower_bound = (
        F.expr("try_cast(nullif(_regex_lower_bound_raw, '') as double)") * F.lit(1_000_000.0)
    ).cast(LongType())
    regex_upper_bound = (
        F.expr("try_cast(nullif(_regex_upper_bound_raw, '') as double)") * F.lit(1_000_000.0)
    ).cast(LongType())
    regex_single = (F.expr("try_cast(nullif(_regex_single_raw, '') as double)") * F.lit(1_000_000.0)).cast(LongType())

    resolved_min_vnd = F.coalesce(
        json_min_vnd,
        F.when(regex_min > 0, regex_min),
        F.when(regex_lower_bound > 0, regex_lower_bound),
        F.when(regex_single > 0, regex_single),
    )
    resolved_max_vnd = F.coalesce(
        json_max_vnd,
        F.when(regex_max > 0, regex_max),
        F.when(regex_upper_bound > 0, regex_upper_bound),
        F.when(regex_single > 0, regex_single),
    )

    has_usable_salary = resolved_min_vnd.isNotNull() | resolved_max_vnd.isNotNull()
    salary_text = F.lower(F.coalesce(F.col("salary"), F.lit("")))
    looks_negotiable = (
        salary_text.rlike(r"th\S*a\s*thu\S*n|tho\S*a\s*thu\S*n|negotiable|canh\s*tranh|c\S*nh\s*tranh")
        | (F.trim(salary_text) == "")
    )

    return (
        df.withColumns(
            {
                "salary_min_vnd": resolved_min_vnd,
                "salary_max_vnd": resolved_max_vnd,
                "salary_is_negotiable": (~has_usable_salary) & looks_negotiable,
            }
        )
        .drop(
            "_regex_min_raw",
            "_regex_max_raw",
            "_regex_lower_bound_raw",
            "_regex_upper_bound_raw",
            "_regex_single_raw",
        )
    )


### 4. Location canonical -> location_count, location_detail, has_remote
def process_location(df):
    """
    Parse location_detail directly from the location array.
    has_remote: derived from job_location_type = "TELECOMMUTE".
    """
    location_detail_expr = F.transform(    # Use 'transform' to iterate each element in array
        F.coalesce(F.col("location"), F.array()),
        lambda x: F.struct(
            F.trim(F.split(x, ":", 2).getItem(0)).alias("city"),
            F.when(F.size(F.split(x, ":", 2)) > 1, F.trim(F.split(x, ":", 2).getItem(1)))
            .otherwise(F.lit(None).cast("string")).alias("address")
        )
    )

    df = df.withColumn("location_count",      F.size(F.col("location")))
    df = df.withColumn("location_detail",     location_detail_expr)
    df = df.withColumn("has_remote",          F.col("job_location_type") == F.lit("TELECOMMUTE"))
    return df


### 5. Dedup: 1 record per job_id (highest record version)
def dedup_silver(df):
    """Keep the lastest snapshot per job_id."""
    w = Window.partitionBy("job_id").orderBy(F.col("record_version").desc())
    # row_number: number each row in each partition, start from 1
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


### FULL TRANSFORMATION PIPELINE
def transform_bronze_to_silver(bronze_df):
    # Rename column for readability
    df = bronze_df.withColumnRenamed("event_ts", "date_posted")
    df = parse_json_ld(df)
    df = resolve_experience(df)
    df = canonicalize_salary_v3(df)
    df = process_location(df)
    return df


### Initialize a Spark session
def build_spark():
    return (
        SparkSession.builder
        .appName("bronze_to_silver")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "200")
        # Spark only overwrites partitions with ingest_date that are included in bronze_df
        .config('spark.sql.sources.partitionOverWriteMode', 'dynamic')
        .getOrCreate()   # If exists -> use, if not -> create new
    )


### ENTRY POINT
def run(date: str | None = None):
    """Run the full pipeline Bronze -> Silver ETL pipeline."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    if date:
        bronze_path = f"{BRONZE_PATH}/ingest_date={date}/"
        silver_path = f"{SILVER_PATH}/ingest_date={date}/"
        logger.info(f"Incremental load: date={date}")
    else:
        bronze_path = f"{BRONZE_PATH}/"
        silver_path = f"{SILVER_PATH}/"
        logger.info(f"Full load: all dates...")

    logger.info(f"Reading Bronze data from {bronze_path}...")
    bronze_df = spark.read.parquet(bronze_path)
    logger.info(f"Bronze records read: {bronze_df.count():,} rows")

    silver_df = transform_bronze_to_silver(bronze_df)
    silver_df = dedup_silver(silver_df)
    logger.info(f"Silver records after dedup: {silver_df.count():,} rows")

    logger.info(f"Writing silver data to {silver_path}...")
    (
        silver_df.write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(silver_path)
    )
    logger.info("Pipeline: bronze_to_silver completed successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze to Silver ETL")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date format: YYYY-MM-DD. Leave blank to run full data."
    )
    arg = parser.parse_args()
    run(date=arg.date)
