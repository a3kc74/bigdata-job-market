from apps.stream_etl.sinks.top_skills_hourly_sink import (
    ES_INDEX_SKILL_COUNTS_HOURLY,
    ES_INDEX_TOP_SKILLS_HOURLY,
    TOP_N,
)
from apps.stream_etl.stateful_jobs.top_skills_hourly import build_skill_counts_hourly


def test_top_skills_hourly_entrypoint_is_importable():
    assert callable(build_skill_counts_hourly)


def test_top_skills_hourly_defaults_match_plan():
    assert TOP_N == 10
    assert ES_INDEX_SKILL_COUNTS_HOURLY == "realtime_skill_counts_hourly_v1"
    assert ES_INDEX_TOP_SKILLS_HOURLY == "realtime_top_skills_hourly_v1"
