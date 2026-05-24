"""Train Spark ML salary prediction model from Silver jobs.

Training data:
    X = title, skills, experience, location, company, remote
    y = real public salary from jobs with salary_min_vnd/salary_max_vnd

The saved PipelineModel is reused by:
    - batch silver_to_gold.py to enrich Gold serving records
    - speed stream_main.py to enrich realtime Elasticsearch documents
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

project_root = Path(__file__).resolve().parents[3]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from apps.ml.salary_prediction import (  # noqa: E402
    LABEL_COL,
    PREDICTION_LOG_COL,
    build_salary_prediction_pipeline,
    with_salary_model_features,
    with_salary_training_label,
)
from configs.logger import get_logger  # noqa: E402
from configs.settings import settings  # noqa: E402


logger = get_logger("train_salary_model")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("train_salary_model")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def run(
    *,
    silver_path: str | None = None,
    model_path: str | None = None,
    metrics_path: str | None = None,
    min_rows: int = 10,
) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Production training always reads the whole Silver table, not one date
    # partition, so every batch run learns from old data + newly crawled data.
    source_path = silver_path or settings.SILVER_PATH
    target_model_path = model_path or settings.SALARY_MODEL_PATH
    target_metrics_path = metrics_path or settings.SALARY_MODEL_METRICS_PATH

    logger.info(f"Reading Silver training data from {source_path}")
    silver_df = spark.read.parquet(source_path)

    # Only public salaries become labels. Negotiable jobs are excluded from
    # training because they are exactly the population we need to predict later.
    training_df = with_salary_training_label(
        with_salary_model_features(
            silver_df,
            experience_col="monthOfExperience",
            location_col="location",
            company_col="company_name",
            remote_col="has_remote",
        )
    )

    public_rows = training_df.count()
    if public_rows < min_rows:
        raise ValueError(
            f"Not enough public salary rows to train model: {public_rows} < {min_rows}. "
            "Crawl more data or lower --min-rows for a demo run."
        )

    train_df, test_df = training_df.randomSplit([0.8, 0.2], seed=42)
    if test_df.rdd.isEmpty():
        train_df = training_df
        test_df = training_df

    pipeline = build_salary_prediction_pipeline()
    logger.info(f"Training salary model with {public_rows:,} public salary rows")
    model = pipeline.fit(train_df)

    predictions = model.transform(test_df)
    rmse_log = RegressionEvaluator(
        labelCol=LABEL_COL,
        predictionCol=PREDICTION_LOG_COL,
        metricName="rmse",
    ).evaluate(predictions)
    mae_log = RegressionEvaluator(
        labelCol=LABEL_COL,
        predictionCol=PREDICTION_LOG_COL,
        metricName="mae",
    ).evaluate(predictions)

    logger.info(f"Saving salary model to {target_model_path}")
    model.write().overwrite().save(target_model_path)

    # Metrics are written as JSON so Airflow/Kubernetes runs leave an auditable
    # training artifact next to the model. `model_family` makes it explicit
    # that the current production model is Spark ML GBTRegressor, not the old
    # linear-regression baseline.
    metrics_df = spark.createDataFrame(
        [
            {
                "model_path": target_model_path,
                "model_family": "GBTRegressor",
                "training_rows": public_rows,
                "test_rows": test_df.count(),
                "rmse_log_salary": float(rmse_log),
                "mae_log_salary": float(mae_log),
            }
        ]
    ).withColumn("created_at", F.current_timestamp())
    metrics_df.write.mode("overwrite").json(target_metrics_path)
    logger.info(f"Saved salary model metrics to {target_metrics_path}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Spark ML salary prediction model")
    parser.add_argument(
        "--silver-path",
        type=str,
        default=None,
        help="Override full Silver input table path. Use a complete Silver directory, not a single date partition.",
    )
    parser.add_argument("--model-path", type=str, default=None, help="Override output PipelineModel path")
    parser.add_argument("--metrics-path", type=str, default=None, help="Override output JSON metrics path")
    parser.add_argument("--min-rows", type=int, default=10, help="Minimum public salary rows required")
    args = parser.parse_args()

    run(
        silver_path=args.silver_path,
        model_path=args.model_path,
        metrics_path=args.metrics_path,
        min_rows=args.min_rows,
    )
