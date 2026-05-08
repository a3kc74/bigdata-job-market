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
#   Case A: $.baseSalary.value.minValue and $.baseSalary.value.maxValue are absent (null) 
#   -> salary_is_negotiable = True
#   Case B: currency = USD                                                                  
#   -> salary_min/max_vnd convert × USD_TO_VND
#   Case C: unitText = YEAR                    
#   -> ÷ 12 in canonicalize_salary()
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

