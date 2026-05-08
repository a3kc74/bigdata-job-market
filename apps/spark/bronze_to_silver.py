import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    to_json,
    to_timestamp,
    trim,
    lower,
    regexp_replace,
    regexp_extract,
    when,
    concat_ws,
    coalesce,
    lit,
    from_unixtime,
    to_date,
)


def parse_ts_safe(cname):
    c = trim(col(cname))
    return (
        when(c.rlike(r"^\d{13}$"), to_timestamp(from_unixtime(c.cast("double") / 1000)))
        .when(c.rlike(r"^\d{10}$"), to_timestamp(from_unixtime(c.cast("double"))))
        .when(c.rlike(r"^\d{4}-\d{2}-\d{2}([ T].*)?$"), to_timestamp(c))
        .otherwise(lit(None).cast("timestamp"))
    )


def parse_date_safe(cname):
    c = trim(col(cname))
    return (
        when(c.rlike(r"^\d{13}$"), to_date(from_unixtime(c.cast("double") / 1000)))
        .when(c.rlike(r"^\d{10}$"), to_date(from_unixtime(c.cast("double"))))
        .when(c.rlike(r"^\d{4}-\d{2}-\d{2}$"), to_date(c))
        .when(c.rlike(r"^\d{4}-\d{2}-\d{2} "), to_date(to_timestamp(c)))
        .otherwise(lit(None).cast("date"))
    )


def normalize_text(col_expr):
    return F.trim(F.lower(F.regexp_replace(col_expr, r"\s+", " ")))


def extract_salary_bucket(col_expr):
    return (
        F.when(col_expr.isNull(), F.lit("UNKNOWN"))
        .when(col_expr < 5_000_000, F.lit("<5M"))
        .when((col_expr >= 5_000_000) & (col_expr < 10_000_000), F.lit("5-10M"))
        .when((col_expr >= 10_000_000) & (col_expr < 20_000_000), F.lit("10-20M"))
        .when((col_expr >= 20_000_000) & (col_expr < 30_000_000), F.lit("20-30M"))
        .when((col_expr >= 30_000_000) & (col_expr < 50_000_000), F.lit("30-50M"))
        .otherwise(F.lit(">50M"))
    )


def classify_salary_type(raw_salary_col):
    raw = F.lower(F.coalesce(raw_salary_col.cast("string"), F.lit("")))
    return (
        F.when(raw.rlike(r"th[oỏ]a thu[ậa]n|negotiable"), F.lit("NEGOTIABLE"))
        .when(raw.rlike(r"minvalue|maxvalue"), F.lit("RANGE"))
        .when(raw == "", F.lit("UNKNOWN"))
        .otherwise(F.lit("UNKNOWN"))
    )


def classify_job_type(desc_col):
    txt = F.lower(F.coalesce(desc_col, F.lit("")))
    return (
        F.when(txt.rlike(r"bán thời gian|part[ -]?time"), F.lit("part_time"))
        .when(txt.rlike(r"thực tập|intern"), F.lit("internship"))
        .when(txt.rlike(r"hợp đồng|contract"), F.lit("contract"))
        .otherwise(F.lit("full_time"))
    )


def classify_seniority(title_col, exp_min_col):
    title = F.lower(F.coalesce(title_col, F.lit("")))
    return (
        F.when(title.rlike(r"\bsenior\b|sr\.?"), F.lit("senior"))
        .when(title.rlike(r"\blead\b|manager|trưởng"), F.lit("lead"))
        .when(exp_min_col >= 3, F.lit("mid"))
        .otherwise(F.lit("junior"))
    )


def experience_bucket(exp_min_col):
    return (
        F.when(exp_min_col.isNull(), F.lit("unknown"))
        .when(exp_min_col < 1, F.lit("0-1"))
        .when((exp_min_col >= 1) & (exp_min_col < 3), F.lit("1-3"))
        .when((exp_min_col >= 3) & (exp_min_col < 5), F.lit("3-5"))
        .otherwise(F.lit("5+"))
    )


def array_contains_keywords(source_col, keywords):
    lower_col = F.lower(F.coalesce(source_col, F.lit("")))
    arr = [
        F.when(lower_col.rlike(fr"(^|[^a-z]){kw}([^a-z]|$)"), F.lit(kw))
        for kw in keywords
    ]
    return F.array_remove(F.array(*arr), F.lit(None))


def main():
    bronze_path = os.getenv("BRONZE_PATH", "s3a://bronze/job_postings/")
    silver_path = os.getenv("SILVER_PATH", "s3a://silver/job_postings/")

    spark = (
        SparkSession.builder
        .appName("bronze-to-silver")
        .getOrCreate()
    )

    try:
        bronze_df = spark.read.parquet(bronze_path)
    except Exception:
        bronze_df = spark.read.json(bronze_path)

    text_for_mode = lower(
        concat_ws(
            " ",
            coalesce(col("payload.title"), lit("")),
            coalesce(col("payload.description"), lit("")),
        )
    )

    parsed = bronze_df.select(
        col("job_id"),
        col("source"),
        parse_ts_safe("ingest_ts").alias("ingest_ts"),
        parse_ts_safe("event_ts").alias("event_ts"),
        col("payload.title").alias("title_raw"),
        col("payload.company_name").alias("company_name"),
        col("payload.salary").alias("salary_text_raw"),
        col("payload.description").alias("description_clean"),
        col("payload.deadline").alias("deadline_raw"),
        col("payload.location").alias("location_raw"),
        to_json(col("payload")).alias("payload_json"),
        trim(
            regexp_extract(
                regexp_replace(coalesce(col("payload.location"), lit("")), r'[\[\]"]', ""),
                r"-\s*([^:,\]]+)",
                1,
            )
        ).alias("province"),
        when(text_for_mode.rlike(r"\bremote\b|làm việc từ xa|work from home|wfh|online"), lit("remote"))
        .when(text_for_mode.rlike(r"\bhybrid\b"), lit("hybrid"))
        .otherwise(lit("onsite"))
        .alias("work_mode"),
        trim(
            lower(
                regexp_replace(coalesce(col("payload.title"), lit("")), r"\s+", " ")
            )
        ).alias("title_normalized"),
    )

    parsed = (
        parsed
        .withColumn("salary_min_vnd", F.get_json_object("payload_json", "$.salary.minValue").cast("long"))
        .withColumn("salary_max_vnd", F.get_json_object("payload_json", "$.salary.maxValue").cast("long"))
        .withColumn("salary_mid_vnd", ((F.col("salary_min_vnd") + F.col("salary_max_vnd")) / 2).cast("long"))
        .withColumn("salary_type", classify_salary_type(F.col("salary_text_raw")))
        .withColumn(
            "salary_mid_vnd",
            F.when(F.col("salary_type") == "NEGOTIABLE", F.lit(None).cast("long"))
             .otherwise(F.col("salary_mid_vnd"))
        )
        .withColumn("salary_bucket", extract_salary_bucket(F.col("salary_mid_vnd")))
        .withColumn(
            "is_salary_disclosed",
            F.when(F.col("salary_mid_vnd").isNotNull(), F.lit(True)).otherwise(F.lit(False))
        )
    )

    parsed = (
        parsed
        .withColumn(
            "experience_text_raw",
            F.regexp_extract(F.coalesce(F.col("description_clean"), F.lit("")), r"(\d+)\s*năm", 1)
        )
        .withColumn(
            "experience_years_min",
            F.when(F.col("experience_text_raw") != "", F.col("experience_text_raw").cast("int"))
             .otherwise(F.lit(None))
        )
        .withColumn("experience_bucket", experience_bucket(F.col("experience_years_min")))
        .withColumn("seniority", classify_seniority(F.col("title_raw"), F.col("experience_years_min")))
    )

    parsed = (
        parsed
        .withColumn("job_type", classify_job_type(F.col("description_clean")))
        .withColumn("is_remote", F.when(F.col("work_mode") == "remote", F.lit(True)).otherwise(F.lit(False)))
        .withColumn("company_normalized_name", normalize_text(F.col("company_name")))
    )

    full_text = F.lower(
        F.concat_ws(
            " ",
            F.coalesce(F.col("title_raw"), F.lit("")),
            F.coalesce(F.col("description_clean"), F.lit("")),
        )
    )

    parsed = (
        parsed
        .withColumn(
            "category_level1",
            F.when(full_text.rlike(r"data|etl|spark|kafka|warehouse|python"), F.lit("Công nghệ Thông tin"))
             .when(full_text.rlike(r"kinh doanh|sales|bán hàng"), F.lit("Kinh doanh/Bán hàng"))
             .when(full_text.rlike(r"giáo viên|giảng dạy|đào tạo"), F.lit("Giáo dục/Đào tạo"))
             .otherwise(F.lit("Khác"))
        )
        .withColumn(
            "category_level2",
            F.when(full_text.rlike(r"data engineer|etl|spark|kafka"), F.lit("Dữ liệu"))
             .when(full_text.rlike(r"sales|b2b|b2c|telemarketing"), F.lit("Kinh doanh"))
             .when(full_text.rlike(r"giáo viên tiếng anh|english"), F.lit("Giảng dạy"))
             .otherwise(F.lit("Khác"))
        )
        .withColumn(
            "category_level3",
            F.when(full_text.rlike(r"data engineer"), F.lit("Data Engineer"))
             .when(full_text.rlike(r"kinh doanh phần mềm|sales phần mềm|software sales"), F.lit("Kinh doanh phần mềm"))
             .when(full_text.rlike(r"giáo viên tiếng anh"), F.lit("Giáo viên tiếng Anh"))
             .otherwise(F.lit("Khác"))
        )
    )

    parsed = (
        parsed
        .withColumn(
            "frameworks",
            array_contains_keywords(
                F.col("description_clean"),
                ["spark", "hadoop", "airflow", "dbt", "flink", "kubernetes"],
            )
        )
        .withColumn(
            "languages",
            array_contains_keywords(
                F.col("description_clean"),
                ["python", "java", "scala", "sql", "javascript", "go", "php"],
            )
        )
        .withColumn(
            "skills",
            F.array_distinct(
                F.flatten(
                    F.array(
                        F.col("frameworks"),
                        F.col("languages"),
                        array_contains_keywords(
                            F.col("description_clean"),
                            ["aws", "gcp", "azure", "etl", "warehouse", "telemarketing", "b2b", "b2c", "sales"],
                        ),
                    )
                )
            )
        )
    )

    parsed = (
        parsed
        .withColumn("posting_ts", F.col("ingest_ts"))
        .withColumn("ingest_date", parse_date_safe("ingest_ts"))
        .withColumn("deadline_date", parse_date_safe("deadline_raw"))
        .withColumn(
            "is_active",
            F.when(F.col("deadline_date").isNull(), F.lit(True))
             .otherwise(F.col("deadline_date") >= F.current_date())
        )
    )

    silver_df = parsed.select(
        F.col("job_id"),
        F.col("source"),
        F.col("title_raw"),
        F.col("title_normalized"),
        F.col("company_name"),
        F.col("company_normalized_name"),
        F.col("province"),
        F.col("category_level1"),
        F.col("category_level2"),
        F.col("category_level3"),
        F.col("salary_type"),
        F.col("salary_text_raw"),
        F.col("salary_min_vnd"),
        F.col("salary_max_vnd"),
        F.col("salary_mid_vnd"),
        F.col("salary_bucket"),
        F.col("is_salary_disclosed"),
        F.col("experience_text_raw"),
        F.col("experience_years_min"),
        F.col("experience_bucket"),
        F.col("seniority"),
        F.col("job_type"),
        F.col("work_mode"),
        F.col("is_remote"),
        F.col("skills"),
        F.col("frameworks"),
        F.col("languages"),
        F.col("description_clean"),
        F.col("ingest_date"),
        F.col("posting_ts"),
        F.col("deadline_date"),
        F.col("is_active"),
        F.col("payload_json"),
    ).dropDuplicates(["job_id"])

    record_count = silver_df.count()
    silver_df.write.mode("overwrite").parquet(silver_path)
    print(f"Completed bronze-to-silver transformation. Wrote {record_count} records to {silver_path}")

    spark.stop()


if __name__ == "__main__":
    main()