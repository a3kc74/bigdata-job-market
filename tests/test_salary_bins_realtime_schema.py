from apps.stream_etl.sinks.salary_bins_realtime_sink import ES_INDEX_SALARY_BINS_HOURLY
from apps.stream_etl.stateful_jobs.salary_bins_realtime import build_salary_bins_hourly
from shared.udfs.salary_parser import parse_salary_text


def test_salary_bins_hourly_entrypoint_is_importable():
    assert callable(build_salary_bins_hourly)


def test_salary_bins_es_index_default_matches_plan():
    assert ES_INDEX_SALARY_BINS_HOURLY == "realtime_salary_bins_hourly_v1"


def test_salary_parser_assigns_expected_bins():
    parsed = parse_salary_text("14 - 15 triệu")
    assert parsed.salary_min_million == 14
    assert parsed.salary_max_million == 15
    assert parsed.salary_avg_million == 14.5
    assert parsed.salary_bin == "10_20m"

    assert parse_salary_text("30 - 40 triệu").salary_bin == "30_50m"
    assert parse_salary_text("Thỏa thuận").salary_bin == "negotiable"
