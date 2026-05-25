"""Spark ML feature engineering and scoring for salary prediction.

Business goal:
    X = title, skills, experience, location, company, remote
    y = real public salary from jobs that disclose salary
    f(x) = predicted salary for jobs whose salary is negotiable/hidden
"""

from __future__ import annotations

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import CountVectorizer, OneHotEncoder, RegexTokenizer, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType


MODEL_VERSION = "spark_ml_salary_gbt_v1"
LABEL_COL = "label_log_salary"
PREDICTION_LOG_COL = "prediction_log_salary"


def _quote_identifier(column_name: str) -> str:
    return f"`{column_name.replace('`', '``')}`"


def _null_string_array() -> F.Column:
    return F.array().cast("array<string>")


def _text_from_column(df: DataFrame, column_name: str | None) -> F.Column:
    """Return a text column even when source data is an array or missing.

    Batch Silver keeps location as array<string>, while speed layer keeps both
    city and location_raw. This helper lets both layers feed the same ML model.
    """

    if not column_name or column_name not in df.columns:
        return F.lit("")

    field_type = df.schema[column_name].dataType
    if isinstance(field_type, ArrayType):
        return F.concat_ws(" ", F.coalesce(F.col(column_name), _null_string_array()))

    return F.coalesce(F.col(column_name).cast("string"), F.lit(""))


def with_salary_model_features(
    df: DataFrame,
    *,
    experience_col: str = "monthOfExperience",
    location_col: str = "location",
    company_col: str = "company_name",
    remote_col: str = "has_remote",
) -> DataFrame:
    """Create the exact feature columns consumed by the Spark ML Pipeline.

    The feature contract is intentionally shared by batch and speed so the
    speed layer can reuse the model trained from historical public salaries.
    """

    if experience_col in df.columns:
        experience_value = F.expr(f"try_cast({_quote_identifier(experience_col)} as double)")
    else:
        experience_value = F.lit(None).cast("double")

    if "skills" in df.columns and isinstance(df.schema["skills"].dataType, ArrayType):
        raw_skills = F.coalesce(F.col("skills"), _null_string_array())
    else:
        raw_skills = _null_string_array()

    skills_tokens = F.array_sort(
        F.array_distinct(
            F.filter(
                F.transform(raw_skills, lambda skill: F.lower(F.trim(skill))),
                lambda skill: skill.isNotNull() & (skill != ""),
            )
        )
    )
    skills_tokens = F.when(F.size(skills_tokens) > 0, skills_tokens).otherwise(F.array(F.lit("unknown_skill")))

    remote_value = (
        F.coalesce(F.col(remote_col).cast("boolean"), F.lit(False))
        if remote_col in df.columns
        else F.lit(False)
    )
    title_text = F.lower(F.coalesce(F.col("title").cast("string"), F.lit(""))) if "title" in df.columns else F.lit("")
    title_text = F.when(F.trim(title_text) != "", title_text).otherwise(F.lit("unknown_title"))
    location_text = F.lower(_text_from_column(df, location_col))
    company_text = F.lower(_text_from_column(df, company_col))

    return (
        df.withColumn("title_text", title_text)
        .withColumn("skills_tokens", skills_tokens)
        .withColumn("experience_months_feature", F.coalesce(experience_value, F.lit(0.0)))
        .withColumn("location_text", F.when(F.trim(location_text) != "", location_text).otherwise(F.lit("unknown_location")))
        .withColumn("company_text", F.when(F.trim(company_text) != "", company_text).otherwise(F.lit("unknown_company")))
        .withColumn("has_remote_feature", F.when(remote_value, F.lit(1.0)).otherwise(F.lit(0.0)))
    )


def with_salary_training_label(df: DataFrame) -> DataFrame:
    """Keep rows with any public real salary bound and build the label."""

    salary_mid_vnd = (
        F.when(
            F.col("salary_min_vnd").isNotNull() & F.col("salary_max_vnd").isNotNull(),
            (F.col("salary_min_vnd") + F.col("salary_max_vnd")) / F.lit(2.0),
        )
        .when(F.col("salary_min_vnd").isNotNull(), F.col("salary_min_vnd").cast("double"))
        .when(F.col("salary_max_vnd").isNotNull(), F.col("salary_max_vnd").cast("double"))
    )

    return (
        df.withColumn("_salary_mid_vnd", salary_mid_vnd)
        # If JSON-LD exposes min/max, that bound is real training data even
        # when the display string says "Thoa thuan".
        .filter(F.col("_salary_mid_vnd") > F.lit(0))
        .withColumn(LABEL_COL, F.log1p(F.col("_salary_mid_vnd")))
    )


def build_salary_prediction_pipeline() -> Pipeline:
    """Build a Spark ML Pipeline that learns f(X) -> salary from public jobs."""

    title_tokenizer = RegexTokenizer(
        inputCol="title_text",
        outputCol="title_tokens",
        pattern=r"\W+",
        minTokenLength=2,
        toLowercase=True,
    )
    title_vectorizer = CountVectorizer(
        inputCol="title_tokens",
        outputCol="title_features",
        vocabSize=2000,
        minDF=1.0,
    )
    skills_vectorizer = CountVectorizer(
        inputCol="skills_tokens",
        outputCol="skills_features",
        vocabSize=2000,
        minDF=1.0,
    )
    location_indexer = StringIndexer(
        inputCol="location_text",
        outputCol="location_index",
        handleInvalid="keep",
    )
    company_indexer = StringIndexer(
        inputCol="company_text",
        outputCol="company_index",
        handleInvalid="keep",
    )
    one_hot_encoder = OneHotEncoder(
        inputCols=["location_index", "company_index"],
        outputCols=["location_features", "company_features"],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=[
            "title_features",
            "skills_features",
            "location_features",
            "company_features",
            "experience_months_feature",
            "has_remote_feature",
        ],
        outputCol="features",
        handleInvalid="keep",
    )
    regressor = GBTRegressor(
        featuresCol="features",
        labelCol=LABEL_COL,
        predictionCol=PREDICTION_LOG_COL,
        maxIter=60,
        maxDepth=5,
        maxBins=64,
        minInstancesPerNode=5,
        stepSize=0.05,
        subsamplingRate=0.8,
        seed=42,
    )

    return Pipeline(
        stages=[
            title_tokenizer,
            title_vectorizer,
            skills_vectorizer,
            location_indexer,
            company_indexer,
            one_hot_encoder,
            assembler,
            regressor,
        ]
    )


def load_salary_prediction_model(spark: SparkSession, model_path: str) -> PipelineModel | None:
    """Load a previously trained PipelineModel, returning None if unavailable."""

    try:
        return PipelineModel.load(model_path)
    except Exception as exc:
        print(f"[salary_prediction] model not loaded from {model_path}: {exc}", flush=True)
        return None


def _salary_source_column(
    *,
    salary_min: F.Column,
    salary_max: F.Column,
    prediction_applied: F.Column,
) -> F.Column:
    """Classify where the serving salary came from for Kibana filters."""

    return (
        F.when(prediction_applied, F.lit("predicted"))
        .when(salary_min.isNotNull() & salary_max.isNotNull(), F.lit("parsed_range"))
        .when(salary_min.isNotNull() & salary_max.isNull(), F.lit("parsed_min_only"))
        .when(salary_min.isNull() & salary_max.isNotNull(), F.lit("parsed_max_only"))
        .otherwise(F.lit("unknown"))
    )


def with_empty_salary_prediction_columns(df: DataFrame) -> DataFrame:
    """Add nullable prediction columns when the model is not available yet."""

    salary_min = F.col("salary_min_vnd") if "salary_min_vnd" in df.columns else F.lit(None).cast(LongType())
    salary_max = F.col("salary_max_vnd") if "salary_max_vnd" in df.columns else F.lit(None).cast(LongType())
    salary_avg = (
        F.when(salary_min.isNotNull() & salary_max.isNotNull(), ((salary_min + salary_max) / F.lit(2.0)).cast(LongType()))
        .when(salary_min.isNotNull(), salary_min)
        .when(salary_max.isNotNull(), salary_max)
    )
    prediction_applied = F.lit(False)

    return (
        df.withColumn("salary_predicted_avg_vnd", F.lit(None).cast(LongType()))
        .withColumn("salary_predicted_min_vnd", F.lit(None).cast(LongType()))
        .withColumn("salary_predicted_max_vnd", F.lit(None).cast(LongType()))
        .withColumn("salary_display_avg_vnd", salary_avg)
        .withColumn("salary_display_min_vnd", salary_min)
        .withColumn("salary_display_max_vnd", salary_max)
        .withColumn("salary_prediction_applied", prediction_applied)
        .withColumn(
            "salary_source",
            _salary_source_column(
                salary_min=salary_min,
                salary_max=salary_max,
                prediction_applied=prediction_applied,
            ),
        )
        .withColumn("salary_prediction_model_version", F.lit(None).cast("string"))
    )


def score_salary_predictions(
    df: DataFrame,
    model: PipelineModel,
    *,
    experience_col: str = "monthOfExperience",
    location_col: str = "location",
    company_col: str = "company_name",
    remote_col: str = "has_remote",
    needs_prediction_col: F.Column | None = None,
) -> DataFrame:
    """Apply a trained model and fill display salary for negotiable jobs only."""

    featured_df = with_salary_model_features(
        df,
        experience_col=experience_col,
        location_col=location_col,
        company_col=company_col,
        remote_col=remote_col,
    )
    scored_df = model.transform(featured_df)

    predicted_avg = F.greatest(F.expm1(F.col(PREDICTION_LOG_COL)), F.lit(0.0))
    predicted_avg_long = predicted_avg.cast(LongType())
    predicted_min_long = (predicted_avg * F.lit(0.9)).cast(LongType())
    predicted_max_long = (predicted_avg * F.lit(1.1)).cast(LongType())

    salary_min = F.col("salary_min_vnd") if "salary_min_vnd" in df.columns else F.lit(None).cast(LongType())
    salary_max = F.col("salary_max_vnd") if "salary_max_vnd" in df.columns else F.lit(None).cast(LongType())
    has_public_salary = salary_min.isNotNull() | salary_max.isNotNull()
    requested_prediction = needs_prediction_col if needs_prediction_col is not None else F.lit(True)
    # Hard business rule: never overwrite a real min/max salary bound. This
    # protects records whose display text says "Thoa thuan" but JSON-LD still
    # contains salary_min_vnd/salary_max_vnd.
    needs_prediction = requested_prediction & (~has_public_salary)
    salary_avg = (
        F.when(salary_min.isNotNull() & salary_max.isNotNull(), ((salary_min + salary_max) / F.lit(2.0)).cast(LongType()))
        .when(salary_min.isNotNull(), salary_min)
        .when(salary_max.isNotNull(), salary_max)
    )
    prediction_applied = needs_prediction & predicted_avg_long.isNotNull()
    salary_source = _salary_source_column(
        salary_min=salary_min,
        salary_max=salary_max,
        prediction_applied=prediction_applied,
    )

    # salary_display_* is the serving-friendly value: real salary when public,
    # model salary only when the job is negotiable or missing public salary.
    return (
        scored_df.withColumn("salary_predicted_avg_vnd", F.when(needs_prediction, predicted_avg_long))
        .withColumn("salary_predicted_min_vnd", F.when(needs_prediction, predicted_min_long))
        .withColumn("salary_predicted_max_vnd", F.when(needs_prediction, predicted_max_long))
        .withColumn("salary_display_avg_vnd", F.when(needs_prediction, predicted_avg_long).otherwise(salary_avg))
        .withColumn("salary_display_min_vnd", F.when(needs_prediction, predicted_min_long).otherwise(salary_min))
        .withColumn("salary_display_max_vnd", F.when(needs_prediction, predicted_max_long).otherwise(salary_max))
        .withColumn("salary_prediction_applied", prediction_applied)
        .withColumn("salary_source", salary_source)
        .withColumn(
            "salary_prediction_model_version",
            F.when(F.col("salary_prediction_applied"), F.lit(MODEL_VERSION)),
        )
        .drop(
            "title_text",
            "skills_tokens",
            "experience_months_feature",
            "location_text",
            "company_text",
            "has_remote_feature",
            "title_tokens",
            "title_features",
            "skills_features",
            "location_index",
            "company_index",
            "location_features",
            "company_features",
            "features",
            PREDICTION_LOG_COL,
        )
    )
