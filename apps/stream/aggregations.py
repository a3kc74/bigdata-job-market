"""
Gold-layer window aggregations.

- build_job_counts_10m:  job count by location_city in 10-minute windows
- build_skill_counts_30m: skill count in 30-minute sliding windows (10-min slide)

"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_job_counts_10m(main_stream_df: DataFrame) -> DataFrame:
    return main_stream_df.groupBy(
        F.window("event_ts", "10 minutes"),
        F.col("location_city"),
    ).count()


def build_skill_counts_30m(main_stream_df: DataFrame) -> DataFrame:
    exploded = (
        main_stream_df.select(
            "event_ts",
            F.explode_outer("skills").alias("skill"),
        )
        .filter(F.col("skill").isNotNull())
        .filter(F.length(F.col("skill")) > 0)
    )

    return exploded.groupBy(
        F.window("event_ts", "30 minutes", "10 minutes"),
        F.col("skill"),
    ).count()
