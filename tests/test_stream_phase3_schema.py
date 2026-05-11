from apps.stream_etl.schemas.raw_job_schema import RAW_JOB_SCHEMA
from apps.stream_etl.transform import build_clean_jobs, build_dead_letter, parse_raw_kafka, validate_raw_jobs


def test_raw_job_schema_contains_phase3_envelope_fields():
    field_names = set(RAW_JOB_SCHEMA.fieldNames())

    assert "job_id" in field_names
    assert "hash_content" in field_names
    assert "payload" in field_names
    assert "quality_flags" in field_names
    assert "event_ts" in field_names
    assert "stream_ingest_ts" in field_names

    payload_fields = set(RAW_JOB_SCHEMA["payload"].dataType.fieldNames())
    assert {"title", "company_name", "salary", "location", "skillsNeeded"}.issubset(payload_fields)


def test_phase3_transform_entrypoints_are_importable():
    assert callable(parse_raw_kafka)
    assert callable(validate_raw_jobs)
    assert callable(build_clean_jobs)
    assert callable(build_dead_letter)
