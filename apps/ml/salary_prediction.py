"""Spark ML feature engineering and scoring for salary prediction.

Business goal:
    X = title, skills, company, employment, education, experience, location
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


MODEL_VERSION = "spark_ml_salary_gbt_v2"
LABEL_COL = "label_log_salary"
PREDICTION_LOG_COL = "prediction_log_salary"


def _quote_identifier(column_name: str) -> str:
    return f"`{column_name.replace('`', '``')}`"


def _null_string_array() -> F.Column:
    return F.array().cast("array<string>")


def _first_existing_column(df: DataFrame, *candidates: str | None) -> str | None:
    for candidate in candidates:
        if candidate and candidate in df.columns:
            return candidate
    return None


def _normalize_text_column(column: F.Column, unknown_value: str) -> F.Column:
    normalized = F.lower(F.trim(F.coalesce(column.cast("string"), F.lit(""))))
    return F.when(normalized != "", normalized).otherwise(F.lit(unknown_value))


def _clean_title_text(column: F.Column) -> F.Column:
    title = F.lower(F.trim(F.coalesce(column.cast("string"), F.lit(""))))
    title = F.regexp_replace(title, r"\bsr\.?\b", " senior ")
    title = F.regexp_replace(title, r"\bjr\.?\b", " junior ")
    title = F.regexp_replace(title, r"\bdev\b", " developer ")
    title = F.regexp_replace(title, r"\bswe\b", " software engineer ")
    title = F.regexp_replace(title, r"\s+", " ")
    title = F.trim(title)
    return F.when(title != "", title).otherwise(F.lit("unknown_title"))


def _employment_type_column(df: DataFrame) -> F.Column:
    source_col = _first_existing_column(df, "employmentType", "employment_type")
    if source_col is None:
        return F.lit("unknown_employment")

    text = F.lower(F.trim(F.coalesce(F.col(source_col).cast("string"), F.lit(""))))
    return (
        F.when(text == "", F.lit("unknown_employment"))
        .when(text.rlike(r"full[\s_-]?time|to[aà]n\s*th[ờo]i\s*gian|ch[ií]nh\s*th[ứu]c"), F.lit("full_time"))
        .when(text.rlike(r"part[\s_-]?time|b[aá]n\s*th[ờo]i\s*gian"), F.lit("part_time"))
        .when(text.rlike(r"intern|th[ựu]c\s*t[ậa]p|trainee"), F.lit("intern"))
        .when(text.rlike(r"freelance|freelancer|c[oộ]ng\s*t[aá]c\s*vi[eê]n"), F.lit("freelance"))
        .when(text.rlike(r"contract|h[ợo]p\s*[đd][ồo]ng|temporary"), F.lit("contract"))
        .otherwise(F.lit("other_employment"))
    )


def _education_column(df: DataFrame) -> F.Column:
    source_col = _first_existing_column(df, "education")
    if source_col is None:
        return F.lit("unknown_education")

    text = F.lower(F.trim(F.coalesce(F.col(source_col).cast("string"), F.lit(""))))
    return (
        F.when(text == "", F.lit("unknown_education"))
        .when(text.rlike(r"kh[oô]ng\s*y[eê]u\s*c[ầa]u|no\s*requirement|none"), F.lit("none"))
        .when(text.rlike(r"cao\s*[đd][ẳa]ng|college"), F.lit("college"))
        .when(text.rlike(r"[đd][ạa]i\s*h[oọ]c|university|c[ửu]\s*nh[aâ]n|bachelor"), F.lit("university"))
        .when(text.rlike(r"th[ạa]c\s*s[ii]|master|ti[eê]n\s*s[ii]|phd|doctor|sau\s*[đd][ạa]i\s*h[oọ]c"), F.lit("postgrad"))
        .otherwise(F.lit("other_education"))
    )


def _company_scale_column(df: DataFrame) -> F.Column:
    source_col = _first_existing_column(df, "company_scale")
    if source_col is None:
        return F.lit("unknown_company_scale")

    text = F.lower(F.trim(F.coalesce(F.col(source_col).cast("string"), F.lit(""))))
    return (
        F.when(text == "", F.lit("unknown_company_scale"))
        .when(text.rlike(r"d[ướu]i\s*50|duoi\s*50|<\s*50|1\s*[-–]\s*50|11\s*[-–]\s*50|10\s*[-–]\s*50"), F.lit("1_50"))
        .when(text.rlike(r"51\s*[-–]\s*200|50\s*[-–]\s*200|100\s*[-–]\s*200|d[ướu]i\s*200|duoi\s*200|<\s*200"), F.lit("51_200"))
        .when(text.rlike(r"201\s*[-–]\s*500|200\s*[-–]\s*500|300\s*[-–]\s*500"), F.lit("201_500"))
        .when(text.rlike(r"500\+|tr[eê]n\s*500|h[oơ]n\s*500|1000"), F.lit("500_plus"))
        .otherwise(_normalize_text_column(F.col(source_col), "unknown_company_scale"))
    )


def _company_name_column(df: DataFrame, company_col: str) -> F.Column:
    source_col = _first_existing_column(df, company_col, "company_name")
    if source_col is None:
        return F.lit("unknown_company")
    return _normalize_text_column(F.col(source_col), "unknown_company")


def _company_field_column(df: DataFrame) -> F.Column:
    source_col = _first_existing_column(df, "company_field", "category")
    if source_col is None:
        return F.lit("unknown_company_field")
    return _normalize_text_column(F.col(source_col), "unknown_company_field")


def _occupational_category_column(df: DataFrame) -> F.Column:
    source_col = _first_existing_column(df, "occupationalCategory", "level")
    if source_col is None:
        return F.lit("unknown_category")
    return _normalize_text_column(F.col(source_col), "unknown_category")


def _primary_city_column(df: DataFrame, location_col: str) -> F.Column:
    explicit_city_col = _first_existing_column(df, "primary_city", "city")
    if explicit_city_col is not None:
        return _normalize_text_column(F.col(explicit_city_col), "unknown_primary_city")

    if "location_detail" in df.columns:
        city = F.expr("element_at(location_detail, 1).city")
        return _normalize_text_column(city, "unknown_primary_city")

    source_col = _first_existing_column(df, location_col, "location", "location_raw")
    if source_col is None:
        return F.lit("unknown_primary_city")

    field_type = df.schema[source_col].dataType
    if isinstance(field_type, ArrayType):
        first_location = F.element_at(F.coalesce(F.col(source_col), _null_string_array()), 1)
        first_city = F.trim(F.split(F.coalesce(first_location, F.lit("")), ":", 2).getItem(0))
        return _normalize_text_column(first_city, "unknown_primary_city")

    return _normalize_text_column(F.col(source_col), "unknown_primary_city")


def _location_count_column(df: DataFrame, location_col: str) -> F.Column:
    count_col = _first_existing_column(df, "location_count")
    if count_col is not None:
        return F.coalesce(F.col(count_col).cast("double"), F.lit(0.0))

    source_col = _first_existing_column(df, location_col, "location", "location_raw")
    if source_col is not None:
        field_type = df.schema[source_col].dataType
        if isinstance(field_type, ArrayType):
            return F.size(F.coalesce(F.col(source_col), _null_string_array())).cast("double")

    city_col = _first_existing_column(df, "primary_city", "city")
    if city_col is not None:
        text = F.trim(F.coalesce(F.col(city_col).cast("string"), F.lit("")))
        return F.when(text != "", F.lit(1.0)).otherwise(F.lit(0.0))

    return F.lit(0.0)


def _experience_months_column(df: DataFrame, experience_col: str) -> tuple[F.Column, str | None]:
    source_col = _first_existing_column(df, experience_col, "experience_months", "monthOfExperience")
    if source_col is None:
        return F.lit(0.0), None

    experience_value = F.expr(f"try_cast({_quote_identifier(source_col)} as double)")
    return F.coalesce(experience_value, F.lit(0.0)), source_col


def _experience_required_column(df: DataFrame, experience_source_col: str | None) -> F.Column:
    explicit_col = _first_existing_column(df, "experience_required")
    if explicit_col is not None:
        return F.when(F.coalesce(F.col(explicit_col).cast("boolean"), F.lit(False)), F.lit(1.0)).otherwise(F.lit(0.0))

    if experience_source_col is None:
        return F.lit(0.0)

    raw_text = F.lower(F.trim(F.coalesce(F.col(experience_source_col).cast("string"), F.lit(""))))
    inferred = (
        F.when(raw_text == "", F.lit(False))
        .when(raw_text.rlike(r"kh[oô]ng\s*y[eê]u\s*c[ầa]u|th[ỏo]a?\s*thu[ậa]n"), F.lit(False))
        .otherwise(F.lit(True))
    )
    return F.when(inferred, F.lit(1.0)).otherwise(F.lit(0.0))


def _skills_tokens_column(df: DataFrame) -> F.Column:
    if "skills" in df.columns and isinstance(df.schema["skills"].dataType, ArrayType):
        raw_skills = F.coalesce(F.col("skills"), _null_string_array())
    else:
        raw_skills = _null_string_array()

    cleaned_skills = F.transform(
        raw_skills,
        lambda skill: F.lower(F.trim(skill)),
    )
    cleaned_skills = F.transform(
        cleaned_skills,
        lambda skill: (
            F.when(skill.rlike(r"^js$|^javascript$"), F.lit("javascript"))
            .when(skill.rlike(r"^node(\.js|js| js)?$"), F.lit("nodejs"))
            .when(skill.rlike(r"^react(\.js|js| js)?$"), F.lit("react"))
            .when(skill.rlike(r"^vue(\.js|js| js)?$"), F.lit("vue"))
            .when(skill.rlike(r"^postgres(ql)?$"), F.lit("postgres"))
            .otherwise(skill)
        ),
    )
    cleaned_skills = F.filter(
        cleaned_skills,
        lambda skill: skill.isNotNull()
        & (skill != "")
        & (~skill.isin("thu gọn", "xem thêm", "thu gon", "xem them")),
    )
    cleaned_skills = F.array_sort(F.array_distinct(cleaned_skills))
    return F.when(F.size(cleaned_skills) > 0, cleaned_skills).otherwise(F.array(F.lit("unknown_skill")))


def with_salary_model_features(
    df: DataFrame,
    *,
    experience_col: str = "monthOfExperience",
    location_col: str = "location",
    company_col: str = "company_name",
    remote_col: str = "has_remote",
) -> DataFrame:
    """Create the feature columns consumed by the Spark ML pipeline.

    The contract intentionally works for both batch Silver rows and speed-layer
    rows by resolving aliases and filling missing values with deterministic
    `unknown_*` fallbacks.
    """

    experience_months, resolved_experience_source = _experience_months_column(df, experience_col)
    skills_tokens = _skills_tokens_column(df)
    remote_source_col = _first_existing_column(df, remote_col, "has_remote")
    remote_value = (
        F.coalesce(F.col(remote_source_col).cast("boolean"), F.lit(False))
        if remote_source_col is not None
        else F.lit(False)
    )
    title_text = _clean_title_text(F.col("title")) if "title" in df.columns else F.lit("unknown_title")

    return (
        df.withColumn("title_text", title_text)
        .withColumn("skills_tokens", skills_tokens)
        .withColumn("company_name_text", _company_name_column(df, company_col))
        .withColumn("employment_type_text", _employment_type_column(df))
        .withColumn("education_text", _education_column(df))
        .withColumn("occupational_category_text", _occupational_category_column(df))
        .withColumn("company_field_text", _company_field_column(df))
        .withColumn("company_scale_text", _company_scale_column(df))
        .withColumn("primary_city_text", _primary_city_column(df, location_col))
        .withColumn("experience_months_feature", experience_months)
        .withColumn("experience_required_feature", _experience_required_column(df, resolved_experience_source))
        .withColumn("has_remote_feature", F.when(remote_value, F.lit(1.0)).otherwise(F.lit(0.0)))
        .withColumn("location_count_feature", _location_count_column(df, location_col))
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
        .filter(F.col("_salary_mid_vnd") > F.lit(0))
        .withColumn(LABEL_COL, F.log1p(F.col("_salary_mid_vnd")))
    )


def build_salary_prediction_pipeline() -> Pipeline:
    """Build a Spark ML pipeline that learns f(X) -> salary from public jobs."""

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
        vocabSize=4000,
        minDF=1.0,
    )
    skills_vectorizer = CountVectorizer(
        inputCol="skills_tokens",
        outputCol="skills_features",
        vocabSize=3000,
        minDF=1.0,
    )

    categorical_cols = [
        "company_name_text",
        "employment_type_text",
        "education_text",
        "occupational_category_text",
        "company_field_text",
        "company_scale_text",
        "primary_city_text",
    ]
    categorical_index_cols = [f"{name}_index" for name in categorical_cols]
    categorical_feature_cols = [f"{name}_features" for name in categorical_cols]

    indexers = [
        StringIndexer(
            inputCol=input_col,
            outputCol=output_col,
            handleInvalid="keep",
        )
        for input_col, output_col in zip(categorical_cols, categorical_index_cols)
    ]
    encoder = OneHotEncoder(
        inputCols=categorical_index_cols,
        outputCols=categorical_feature_cols,
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=[
            "title_features",
            "skills_features",
            *categorical_feature_cols,
            "experience_months_feature",
            "experience_required_feature",
            "has_remote_feature",
            "location_count_feature",
        ],
        outputCol="features",
        handleInvalid="keep",
    )
    regressor = GBTRegressor(
        featuresCol="features",
        labelCol=LABEL_COL,
        predictionCol=PREDICTION_LOG_COL,
        maxIter=80,
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
            *indexers,
            encoder,
            assembler,
            regressor,
        ]
    )


def load_salary_prediction_model(spark: SparkSession, model_path: str) -> PipelineModel | None:
    """Load a previously trained pipeline model, returning None if unavailable."""

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

    drop_cols = [
        "title_text",
        "skills_tokens",
        "company_name_text",
        "employment_type_text",
        "education_text",
        "occupational_category_text",
        "company_field_text",
        "company_scale_text",
        "primary_city_text",
        "experience_months_feature",
        "experience_required_feature",
        "has_remote_feature",
        "location_count_feature",
        "title_tokens",
        "title_features",
        "skills_features",
        "features",
        PREDICTION_LOG_COL,
    ]
    drop_cols.extend(
        [
            "company_name_text_index",
            "employment_type_text_index",
            "education_text_index",
            "occupational_category_text_index",
            "company_field_text_index",
            "company_scale_text_index",
            "primary_city_text_index",
            "company_name_text_features",
            "employment_type_text_features",
            "education_text_features",
            "occupational_category_text_features",
            "company_field_text_features",
            "company_scale_text_features",
            "primary_city_text_features",
        ]
    )

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
        .drop(*drop_cols)
    )
