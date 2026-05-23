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
# schedule_type: Categorical value for UI filtering ("T2-T6", "T2-T7", "T2-CN", "Flexible", "Other")
# is_weekend_free: Derived from schedule_type — True if T2-T6 or Flexible
#
# Strategy: 3-step rule-based approach (header-first)
#   Step 1: Normalize — strip JSON array wrapper, literal \n, Unicode NFC
#   Step 2: Parse header — extract day-range from first line
#   Step 3: Decision tree — header > Saturday body signals > free-text keywords
def parse_schedule(df):

    # ── Step 1: Normalize schedule text ──
    cleaned = F.col("schedule")

    # Strip JSON array wrapper: ["..."] → content
    cleaned = F.when(
        cleaned.startswith('["') & cleaned.endswith('"]'),
        F.expr("substring(schedule, 3, length(schedule) - 4)")
    ).otherwise(cleaned)

    # Replace literal \n and escaped quotes
    cleaned = F.regexp_replace(cleaned, r"\\n", "\n")
    cleaned = F.regexp_replace(cleaned, r'""', '"')

    # Lowercase for matching
    cleaned = F.lower(cleaned)

    # Store normalized schedule for pattern matching
    df = df.withColumn("_sched_norm", cleaned)

    # Extract first line (before first newline)
    first_line = F.when(
        F.col("_sched_norm").contains("\n"),
        F.expr("substring(_sched_norm, 1, instr(_sched_norm, '\n') - 1)")
    ).otherwise(F.col("_sched_norm"))

    df = df.withColumn("_sched_first_line", first_line)

    # ── Step 2: Parse header from first line ──
    # Detect "Thứ 2 - Chủ nhật" → CN
    is_header_cn = F.col("_sched_first_line").rlike(
        r"thứ\s*2\s*[-–]\s*chủ\s*nhật"
    )
    # Detect "Thứ 2 - Thứ 7" or "Thứ 2 - Thứ bảy" → T7
    is_header_t7 = F.col("_sched_first_line").rlike(
        r"thứ\s*2\s*[-–]\s*thứ\s*(7|bảy)"
    )
    # Detect "Thứ 2 - Thứ 6" (or 3/4/5, sáu/năm/tư/ba) → T6
    is_header_t6 = F.col("_sched_first_line").rlike(
        r"thứ\s*2\s*[-–]\s*thứ\s*([3-6]|sáu|năm|tư|ba)"
    )

    # ── Step 3: Saturday work signals (for T2-T6 header + body check) ──
    norm = F.col("_sched_norm")

    has_sat_work = (
        # Separate line "Thứ 7 (...)" or "Thứ 7:" or "Thứ Bảy:"
        norm.rlike(r"(?m)^\s*[-•_]*\s*thứ\s*(7|bảy)\s*[\(:]")
        # "sáng/buổi/làm/N thứ 7"
        | norm.rlike(r"(sáng|buổi|làm|\d+)\s+(ngày\s+)?thứ\s*(7|bảy)")
        # "thứ 7 cách/xen/tuần/làm/wfh/online/..."
        | norm.rlike(r"thứ\s*(7|bảy)\s+(cách|xen|tuần|làm|wfh|online|remote|linh|đầu|cuối|hàng|chỉ|được|nếu|có|nghỉ\s+(cách|xen|luân))")
        # "N thứ 7/tháng" or "T7/tháng"
        | norm.rlike(r"(\d+\s*)?(ngày\s+)?(thứ\s*(7|bảy)|t7)\s*/\s*tháng")
        # "và/thêm/+ thứ 7"
        | norm.rlike(r"(và|thêm|\+)\s+(\d+\s+)?(ngày\s+)?thứ\s*(7|bảy)")
        # "thứ 7: HH:MM" (time pattern)
        | norm.rlike(r"thứ\s*(7|bảy)\s*:\s*\d")
        # "thứ 7 Nh" or "thứ 7 từ"
        | norm.rlike(r"thứ\s*(7|bảy)\s+(\d+h|từ)")
        # "hết/đến sáng thứ 7"
        | norm.rlike(r"(hết|đến)\s+sáng\s+thứ\s*(7|bảy)")
        # "sáng thứ bảy" / "sáng thứ 7"
        | norm.rlike(r"sáng\s+thứ\s*(7|bảy)")
        # T7 with context (NOT standalone)
        | norm.rlike(r"(sáng|làm|2|02|hai)\s*t7")
        | norm.rlike(r"t7\s*(cách|xen|làm|wfh|online|tuần|trong)")
        # "Thứ Bảy làm/từ/buổi"
        | norm.rlike(r"thứ\s*bảy\s+(làm|từ|buổi)")
        # "saturday"
        | norm.rlike(r"saturday")
        # "N ngày/buổi thứ 7"
        | norm.rlike(r"\d+\s+(buổi\s+)?(sáng\s+)?(ngày\s+)?thứ\s*(7|bảy)")
        # "thứ 7 so le / trong / xen kẽ / luân"
        | norm.rlike(r"thứ\s*(7|bảy)\s+(so\s*le|trong|xen\s*kẽ|luân)")
    )

    # ── Free-text patterns (when no standard header) ──
    is_freetext_t2t7 = (
        norm.rlike(r"t2\s*[-–]\s*(sáng\s*)?t7")
        | norm.rlike(r"thứ\s*2\s*đến\s*(sáng\s+)?thứ\s*(7|bảy)")
        | norm.rlike(r"thứ\s*(hai|2)\s*[-–đến]+\s*(sáng\s+)?thứ\s*(bảy|7)")
        | norm.rlike(r"6\s*ngày")
        | norm.rlike(r"monday\s*(to|[-–])\s*saturday")
        | norm.rlike(r"thứ\s*2\s*[-–]\s*6")
        | norm.rlike(r"thứ\s*(2|hai).*(và|&)\s*thứ\s*(7|bảy)")
        | norm.rlike(r"đến\s+hết\s+sáng\s+thứ\s*(7|bảy)")
    )

    is_freetext_t2t6 = (
        norm.rlike(r"monday\s*(to|[-–])\s*friday")
        | norm.rlike(r"mon\s*[-–]\s*fri")
        | norm.rlike(r"5\s*(days|ngày)")
        | norm.rlike(r"t2\s*[-–]\s*t6")
        | norm.rlike(r"thứ\s*(hai|2)\s*[-–đến]+\s*thứ\s*(sáu|6)")
        | norm.rlike(r"\d+\s*days\s*/\s*week")
    )

    is_shift = norm.rlike(
        r"xoay\s*ca|theo\s*ca|làm\s*(việc\s*)?theo\s*ca|(ca|3\s*ca)\s*(sáng|chiều|đêm|1|2|3)|rotation\s*shift|trực\s*ca"
    )

    is_cn_work = (
        norm.rlike(r"thứ\s*(2|hai)\s*(đến|[-–])\s*chủ\s*nhật")
        | norm.rlike(r"(?m)^\s*chủ\s*nhật\s*[\(:]")
    )

    is_flexible_kw = norm.rlike(
        r"linh\s*hoạt|flexible|remote|làm\s*việc\s*tại\s*nhà|tại\s*nhà|work\s*from\s*home|tự\s*do|check\s*in.*linh|bán\s*thời\s*gian|part\s*-?\s*time"
    )

    is_empty = F.col("_sched_norm").isNull() | (F.trim(F.col("_sched_norm")) == "")

    # ── Decision tree ──
    schedule_type = (
        F.when(is_empty, F.lit("Flexible"))
        # Header-based classification
        .when(is_header_cn, F.lit("T2-CN"))
        .when(is_header_t7, F.lit("T2-T7"))
        .when(is_header_t6 & has_sat_work, F.lit("T2-T7"))
        .when(is_header_t6, F.lit("T2-T6"))
        # Free-text classification (no standard header)
        .when(is_freetext_t2t7, F.lit("T2-T7"))
        .when(is_freetext_t2t6 & has_sat_work, F.lit("T2-T7"))
        .when(is_freetext_t2t6, F.lit("T2-T6"))
        .when(has_sat_work, F.lit("T2-T7"))
        .when(is_cn_work, F.lit("T2-CN"))
        .when(is_shift, F.lit("Other"))
        .when(is_flexible_kw, F.lit("Flexible"))
        .otherwise(F.lit("Other"))
    )

    df = df.withColumn("schedule_type", schedule_type)

    # Derive is_weekend_free from schedule_type
    df = df.withColumn(
        "is_weekend_free",
        F.col("schedule_type").isin("T2-T6", "Flexible")
    )

    # Drop temp columns
    df = df.drop("_sched_norm", "_sched_first_line")

    return df

### 2. Clean and normalize data
# Rename job_id_platform to company_id
# Cast monthOfExperience to IntegerType 
def normalize_gold_fields(df):
    df = df.withColumnRenamed("job_id_platform", "company_id")

    # We will convert string value to null since "Không yêu cầu" means no strict experience required
    df = df.withColumn("monthOfExperience", F.col("monthOfExperience").cast(IntegerType()))

    # is_active: False if deadline has passed, True otherwise (null deadline = active)
    df = df.withColumn(
        "is_active",
        F.when(F.col("deadline").isNull(), F.lit(True))
         .otherwise(F.col("deadline") >= F.current_timestamp())
    )

    # has_remote: coalesce null -> False (Silver returns null when job_location_type is null,
    # Elasticsearch does not index null values, making the field invisible in Kibana)
    df = df.withColumn("has_remote", F.coalesce(F.col("has_remote"), F.lit(False)))

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