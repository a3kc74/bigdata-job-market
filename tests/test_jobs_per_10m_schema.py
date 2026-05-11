from apps.stream_etl.sinks.jobs_per_10m_sink import ES_INDEX_JOB_COUNTS_10M
from apps.stream_etl.stateful_jobs.jobs_per_10m import build_jobs_per_10m


def test_jobs_per_10m_entrypoint_is_importable():
    assert callable(build_jobs_per_10m)


def test_jobs_per_10m_es_index_default_matches_plan():
    assert ES_INDEX_JOB_COUNTS_10M == "realtime_job_counts_10m_v1"
