import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    silver_path = os.getenv("SILVER_PATH", "s3a://silver/job_postings/")
    gold_jobs_flat_path = os.getenv("GOLD_JOBS_FLAT_PATH", "s3a://gold/gold_jobs_flat/")
    gold_daily_stats_path = os.getenv("GOLD_DAILY_STATS_PATH", "s3a://gold/gold_daily_stats/")
    gold_salary_bucket_stats_path = os.getenv("GOLD_SALARY_BUCKET_STATS_PATH", "s3a://gold/gold_salary_bucket_stats/")
    gold_language_stats_path = os.getenv("GOLD_LANGUAGE_STATS_PATH", "s3a://gold/gold_language_salary_stats/")
    gold_framework_stats_path = os.getenv("GOLD_FRAMEWORK_STATS_PATH", "s3a://gold/gold_framework_salary_stats/")
    gold_category_stats_path = os.getenv("GOLD_CATEGORY_STATS_PATH", "s3a://gold/gold_category_stats/")

    spark = SparkSession.builder.appName("silver-to-gold").getOrCreate()

    silver_df = spark.read.parquet(silver_path)

    # 1) gold_jobs_flat
    gold_jobs_flat = silver_df.select(
        F.concat_ws("_", F.col("source"), F.col("job_id")).alias("_id"),
        "job_id",
        "source",
        "title_raw",
        "title_normalized",
        "company_name",
        "company_normalized_name",
        "province",
        "category_level1",
        "category_level2",
        "category_level3",
        "salary_type",
        "salary_text_raw",
        "salary_min_vnd",
        "salary_max_vnd",
        "salary_mid_vnd",
        "salary_bucket",
        "is_salary_disclosed",
        "experience_years_min",
        "experience_bucket",
        "seniority",
        "job_type",
        "work_mode",
        "is_remote",
        "skills",
        "frameworks",
        "languages",
        "ingest_date",
        "deadline_date",
        "is_active",
        "hash_content"
    )

    # 2) gold_daily_stats
    gold_daily_stats = silver_df.groupBy("ingest_date").agg(
        F.countDistinct("job_id").alias("total_jobs"),
        F.sum(F.when(F.col("is_active"), 1).otherwise(0)).alias("active_jobs"),
        F.sum(F.when(F.col("is_salary_disclosed"), 1).otherwise(0)).alias("salary_disclosed_jobs"),
        F.sum(F.when(F.col("salary_type") == "NEGOTIABLE", 1).otherwise(0)).alias("salary_negotiable_jobs"),
        F.sum(F.when(F.col("is_remote"), 1).otherwise(0)).alias("remote_jobs"),
        F.avg("salary_mid_vnd").cast("long").alias("avg_salary_mid_vnd")
    ).withColumn(
        "_id", F.col("ingest_date").cast("string")
    )

    # 3) gold_salary_bucket_stats
    total_by_date = silver_df.groupBy("ingest_date").agg(F.countDistinct("job_id").alias("total_jobs_date"))

    gold_salary_bucket_stats = (
        silver_df.groupBy("ingest_date", "salary_bucket")
        .agg(F.countDistinct("job_id").alias("job_count"))
        .join(total_by_date, on="ingest_date", how="left")
        .withColumn("job_pct", F.col("job_count") / F.col("total_jobs_date"))
        .withColumn("_id", F.concat_ws("_", F.col("ingest_date").cast("string"), F.col("salary_bucket")))
        .select("_id", "ingest_date", "salary_bucket", "job_count", "job_pct")
    )

    # 4) gold_language_salary_stats
    lang_df = silver_df.withColumn("language", F.explode_outer("languages")).filter(F.col("language").isNotNull())
    total_lang_by_date = lang_df.groupBy("ingest_date").agg(F.count("language").alias("total_language_mentions"))

    gold_language_salary_stats = (
        lang_df.groupBy("ingest_date", "language")
        .agg(
            F.countDistinct("job_id").alias("job_count"),
            F.avg("salary_mid_vnd").cast("long").alias("avg_salary_mid_vnd")
        )
        .join(total_lang_by_date, on="ingest_date", how="left")
        .withColumn("job_pct", F.col("job_count") / F.col("total_language_mentions"))
        .withColumn("_id", F.concat_ws("_", F.col("ingest_date").cast("string"), F.col("language")))
        .select("_id", "ingest_date", "language", "job_count", "job_pct", "avg_salary_mid_vnd")
    )

    # 5) gold_framework_salary_stats
    fw_df = silver_df.withColumn("framework", F.explode_outer("frameworks")).filter(F.col("framework").isNotNull())

    gold_framework_salary_stats = (
        fw_df.groupBy("ingest_date", "framework")
        .agg(
            F.countDistinct("job_id").alias("job_count"),
            F.avg("salary_mid_vnd").cast("long").alias("avg_salary_mid_vnd")
        )
        .withColumn("_id", F.concat_ws("_", F.col("ingest_date").cast("string"), F.col("framework")))
        .select("_id", "ingest_date", "framework", "job_count", "avg_salary_mid_vnd")
    )

    # 6) gold_category_stats
    total_cat_by_date = silver_df.groupBy("ingest_date").agg(F.countDistinct("job_id").alias("total_jobs_date"))

    gold_category_stats = (
        silver_df.groupBy("ingest_date", "category_level1")
        .agg(F.countDistinct("job_id").alias("job_count"))
        .join(total_cat_by_date, on="ingest_date", how="left")
        .withColumn("job_pct", F.col("job_count") / F.col("total_jobs_date"))
        .withColumn("_id", F.concat_ws("_", F.col("ingest_date").cast("string"), F.col("category_level1")))
        .select("_id", "ingest_date", "category_level1", "job_count", "job_pct")
    )

    gold_jobs_flat.write.mode("overwrite").parquet(gold_jobs_flat_path)
    gold_daily_stats.write.mode("overwrite").parquet(gold_daily_stats_path)
    gold_salary_bucket_stats.write.mode("overwrite").parquet(gold_salary_bucket_stats_path)
    gold_language_salary_stats.write.mode("overwrite").parquet(gold_language_stats_path)
    gold_framework_salary_stats.write.mode("overwrite").parquet(gold_framework_stats_path)
    gold_category_stats.write.mode("overwrite").parquet(gold_category_stats_path)

    spark.stop()


if __name__ == "__main__":
    main()