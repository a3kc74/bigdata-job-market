"""Realtime salary-bin stateful aggregation."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_salary_bins_hourly(clean_df: DataFrame) -> DataFrame:
    """Aggregate jobs by hourly salary bin, primary city, and category."""

    return (
        clean_df.withWatermark("event_time", "2 hours")
        .groupBy(
            F.window(F.col("event_time"), "1 hour").alias("time_window"),
            F.coalesce(F.col("primary_city"), F.lit("unknown")).alias("primary_city"),
            F.coalesce(F.col("occupationalCategory"), F.lit("unknown")).alias("occupationalCategory"),
            F.coalesce(F.col("salary_bin"), F.lit("unknown")).alias("salary_bin"),
        )
        .agg(
            F.approx_count_distinct("job_id").cast("long").alias("job_count"),
            F.avg("salary_min_million").alias("avg_salary_min_million"),
            F.avg("salary_max_million").alias("avg_salary_max_million"),
            F.expr("percentile_approx(salary_avg_million, 0.5)").alias("median_salary_million"),
        )
        .select(
            F.to_date(F.col("time_window.start")).alias("bucket_date"),
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "primary_city",
            "occupationalCategory",
            "salary_bin",
            "job_count",
            "avg_salary_min_million",
            "avg_salary_max_million",
            "median_salary_million",
            F.current_timestamp().alias("updated_at"),
        )
    )
