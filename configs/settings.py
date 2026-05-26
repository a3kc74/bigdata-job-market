from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    USD_TO_VND: float = 25_000.0
    RAW_PATH: str = "hdfs://hdfs-namenode.hdfs.svc:9000/raw/jobs"
    BRONZE_PATH: str = "hdfs://hdfs-namenode.hdfs.svc:9000/bronze/jobs"
    SILVER_PATH: str = "hdfs://hdfs-namenode.hdfs.svc:9000/silver/jobs"
    GOLD_PATH: str = "hdfs://hdfs-namenode.hdfs.svc:9000/gold/jobs/job_market_index"
    # Spark ML salary model trained from public salary jobs. Batch and speed
    # both read this path so salary prediction stays consistent across layers.
    SALARY_MODEL_PATH: str = "hdfs://hdfs-namenode.hdfs.svc:9000/models/salary_prediction/latest"
    SALARY_MODEL_METRICS_PATH: str = "hdfs://hdfs-namenode.hdfs.svc:9000/models/salary_prediction/metrics/latest"


    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"   # Allow extra variables like PYTHONPATH in .env
    )


settings = Settings()
