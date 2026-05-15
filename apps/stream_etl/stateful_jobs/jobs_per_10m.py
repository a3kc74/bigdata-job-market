"""Jobs-per-10-minutes stateful aggregation."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_jobs_per_10m(clean_df: DataFrame) -> DataFrame:
    """Count distinct jobs and companies by 10-minute event-time window."""

    return (
        clean_df.withWatermark("event_time", "60 minutes")
        .groupBy(
            F.window(F.col("event_time"), "10 minutes"),
            F.coalesce(F.col("source"), F.lit("unknown")).alias("source"),
            F.coalesce(F.col("city"), F.lit("unknown")).alias("city"),
            F.coalesce(F.col("category"), F.lit("unknown")).alias("category"),
        )
        .agg(
            F.approx_count_distinct("job_id").cast("long").alias("job_count"),
            F.approx_count_distinct("company_name").cast("long").alias("distinct_company_count"),
        )
        .select(
            F.to_date(F.col("window.start")).alias("bucket_date"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "source",
            "city",
            "category",
            "job_count",
            "distinct_company_count",
            F.current_timestamp().alias("updated_at"),
        )
    )
