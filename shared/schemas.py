"""
Defines the schema Spark uses to parse Kafka JSON into columns.

Layer: BRONZE
The schema is applied when reading raw JSON from Kafka to produce
typed, structured rows.
"""

from pyspark.sql.types import StringType, StructField, StructType

RAW_EVENT_SCHEMA = StructType(
    [
        StructField("job_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("company_name", StringType(), True),
        StructField("salary_text", StringType(), True),
        StructField("location_text", StringType(), True),
        StructField("skills_text", StringType(), True),
        StructField("description_text", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("ingest_ts", StringType(), True),
    ]
)
