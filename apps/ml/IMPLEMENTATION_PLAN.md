# Salary Prediction Implementation Plan

## Goal

Upgrade salary prediction while keeping one shared Spark ML pipeline that can be used by:

- `apps/batch/jobs/train_salary_model.py`
- `apps/batch/jobs/silver_to_gold.py`
- `apps/stream_etl/stream_main.py`

## Locked Decisions

- Model family: Spark ML `GBTRegressor`
- Training target: `log1p(mid_salary_vnd)`
- Prediction strategy: predict `mid` only
- Derived range: fixed rule from predicted mid
  - default `pred_min = pred_mid * 0.9`
  - default `pred_max = pred_mid * 1.1`
- Public salary precedence:
  - If both `salary_min_vnd` and `salary_max_vnd` exist, do not predict
  - If only one bound exists, keep the public bound and fill the missing side
- `company_name` stays as a main feature, no bucketing
- Speed layer does not use embedding
- Speed trigger target: `30s`

## Feature Set

### Text features

- `title`
- `skills`

### Structured categorical features

- `company_name`
- `employmentType`
- `education`
- `occupationalCategory`
- `company_field`
- `company_scale`
- `primary_city`

### Numeric and boolean features

- `experience_months`
- `experience_required`
- `has_remote`
- `location_count`

## Feature Handling

### Title

Use Spark-native bag-of-words for v1:

- Lowercase
- Trim
- Regex tokenize
- Optional stopword cleanup only if clearly noisy in data review
- `CountVectorizer`

Reason:

- Keeps one artifact for batch and speed
- Avoids adding an embedding inference path to streaming
- Is easy to debug against real TopCV titles

### Skills

- Lowercase
- Trim
- Remove empty values
- Deduplicate
- Remove known UI noise such as `thu gọn`, `xem thêm`
- `CountVectorizer`

### Company name

- Keep raw company name as a real feature
- Normalize only casing and surrounding whitespace
- Map empty to `unknown_company`

### Employment type and education

Normalize into stable categories before indexing.

Examples:

- `employmentType`: `FULL_TIME`, `PART_TIME`, `CONTRACT`, `INTERN`, `FREELANCE`, `OTHER`, `UNKNOWN`
- `education`: `NONE`, `COLLEGE`, `UNIVERSITY`, `POSTGRAD`, `OTHER`, `UNKNOWN`

### Experience

- Parse `monthOfExperience` into numeric months
- Keep `experience_required` as a separate boolean
- Do not collapse `Khong yeu cau` and parse failure into the same semantic bucket without a flag

### Location

- Derive `primary_city` from normalized city field
- Keep `location_count` as a supporting feature
- Keep `has_remote` separate from city

## Model Pipeline

Shared Spark ML pipeline:

1. Build normalized feature columns
2. `RegexTokenizer` for `title`
3. `CountVectorizer` for `title`
4. `CountVectorizer` for `skills`
5. `StringIndexer` + `OneHotEncoder` for structured categoricals
6. `VectorAssembler`
7. `GBTRegressor`

Recommended first tuning baseline:

- `maxIter=80`
- `maxDepth=5`
- `maxBins=64`
- `minInstancesPerNode=5`
- `stepSize=0.05`
- `subsamplingRate=0.8`

Start from the current parameters, then tune only after feature parity between
batch and speed is stable.

## Serving Rules

### When no public salary exists

- Predict `mid`
- Write:
  - `salary_predicted_avg_vnd`
  - `salary_predicted_min_vnd`
  - `salary_predicted_max_vnd`
  - `salary_display_*` from predicted values
  - `salary_source = predicted`

### When only public min exists

- Keep `salary_display_min_vnd = public min`
- Predict `mid`
- Derive `predicted max`
- `salary_display_avg_vnd = max(predicted_mid, public_min)`
- `salary_display_max_vnd = max(predicted_max, public_min)`
- `salary_source = parsed_min_only`

### When only public max exists

- Keep `salary_display_max_vnd = public max`
- Predict `mid`
- Derive `predicted min`
- `salary_display_avg_vnd = min(predicted_mid, public_max)`
- `salary_display_min_vnd = min(predicted_min, public_max)`
- `salary_source = parsed_max_only`

### When both public bounds exist

- Keep public salary
- Do not predict
- `salary_source = parsed_range`

## Batch and Speed Contract

Batch and speed must expose the same logical features before scoring:

- `title`
- `skills`
- `company_name`
- `employmentType`
- `education`
- `occupationalCategory`
- `company_field`
- `company_scale`
- `experience_months`
- `experience_required`
- `has_remote`
- `primary_city`
- `location_count`

If speed cannot provide a field yet, add a deterministic fallback such as
`unknown_*` rather than silently dropping the column.

## Speed Layer Settings

Current crawler and producer behavior emits jobs incrementally while the crawl is
still running, so speed should stay near-realtime even without embedding.

Recommended setting:

- `TRIGGER_SECONDS=30`

Reason:

- Producer polls JSONL every 2 seconds
- One job detail commonly lands every few seconds
- 30 seconds gives a larger micro-batch without making ES noticeably stale

## Validation and Acceptance

Minimum checks before rollout:

1. Feature parity between batch and speed
2. Train job completes and saves model
3. `silver_to_gold` can score with the new artifact
4. `stream_main` can score with the same artifact
5. Prediction never overwrites full public salary ranges
6. Model metrics improve or at least stay stable after adding new fields
