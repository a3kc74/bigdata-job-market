"""Hourly top-skills stateful aggregation."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_skill_counts_hourly(clean_df: DataFrame) -> DataFrame:
    """Count distinct jobs per skill in 1-hour event-time windows."""

    skill_events = (
        clean_df.withWatermark("event_time", "2 hours")
        .select(
            F.window(F.col("event_time"), "1 hour").alias("time_window"),
            F.explode(F.coalesce(F.col("skills"), F.array().cast("array<string>"))).alias("skill"),
            F.col("job_id"),
        )
        .filter(F.col("skill").isNotNull() & (F.length(F.trim(F.col("skill"))) > 0))
        .withColumn("skill", F.trim(F.col("skill")))
    )

    return (
        skill_events.groupBy("time_window", "skill")
        .agg(F.approx_count_distinct("job_id").cast("long").alias("job_count"))
        .select(
            F.to_date(F.col("time_window.start")).alias("bucket_date"),
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "skill",
            "job_count",
            F.current_timestamp().alias("updated_at"),
        )
    )
