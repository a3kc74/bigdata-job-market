import os

from apps.producer.crawler_jsonl_producer import (
    coerce_payload,
    ensure_list,
    get_start_position,
    get_message_key,
    join_if_list,
    parse_int_or_none,
    prepare_event,
    select_latest_path,
)


def test_ensure_list_handles_strings_and_lists():
    assert ensure_list("Ha Noi, Ho Chi Minh") == ["Ha Noi", "Ho Chi Minh"]
    assert ensure_list(["Python", "", None, "SQL"]) == ["Python", "SQL"]
    assert ensure_list("") is None


def test_join_if_list_handles_structured_values():
    assert join_if_list(["A", "B"]) == "A\nB"
    assert join_if_list({"b": 2, "a": 1}) == '{"a": 1, "b": 2}'
    assert join_if_list(" text ") == "text"


def test_parse_int_or_none_extracts_first_integer():
    assert parse_int_or_none("3 nam") == 3
    assert parse_int_or_none("1,000") == 1000
    assert parse_int_or_none("khong yeu cau") is None


def test_coerce_payload_matches_raw_schema_types():
    payload = coerce_payload(
        {
            "title": "Data Engineer",
            "location": "Ha Noi, Remote",
            "requirements": ["Python", "Spark"],
            "benefits": ["Bonus"],
            "monthOfExperience": "24 months",
            "openings": "2 nguoi",
            "skillsNeeded": "Python; SQL",
            "company_details": {"scale": 100, "field": ["IT"], "address": None},
        }
    )

    assert payload["location"] == ["Ha Noi", "Remote"]
    assert payload["requirements"] == "Python\nSpark"
    assert payload["benefits"] == "Bonus"
    assert payload["monthOfExperience"] == 24
    assert payload["openings"] == 2
    assert payload["skillsNeeded"] == ["Python", "SQL"]
    assert payload["company_details"] == {"scale": "100", "field": "IT", "address": None}


def test_prepare_event_adds_stream_metadata_and_coerces_payload():
    event = {
        "job_id": "job-1",
        "ingest_ts": "1777812000000",
        "event_ts": "1777811999000",
        "crawl_version": "1",
        "payload": {"location": "Da Nang", "requirements": ["A"]},
        "quality_flags": {"has_location_info": 1},
    }

    prepared = prepare_event(
        event,
        event_time_mode="now",
        replay_id="replay-1",
        replay_seq=7,
        timestamp_ms=1777812000123,
    )

    assert get_message_key(prepared) == "job-1"
    assert prepared["stream_ingest_ts"] == 1777812000123
    assert prepared["event_ts"] == 1777812000123
    assert prepared["original_event_ts"] == 1777811999000
    assert prepared["crawl_version"] == 1
    assert prepared["payload"]["location"] == ["Da Nang"]
    assert prepared["payload"]["requirements"] == "A"
    assert prepared["quality_flags"]["has_location_info"] is True


def test_select_latest_path_picks_newest_matching_file(tmp_path):
    older = tmp_path / "jobs_speed_old.jsonl"
    newer = tmp_path / "jobs_speed_new.jsonl"
    older.write_text("{}\n", encoding="utf-8")
    newer.write_text("{}\n", encoding="utf-8")
    os.utime(older, (1, 1))
    os.utime(newer, (2, 2))

    assert select_latest_path([older, newer]) == [newer]


def test_get_start_position_resets_when_file_was_truncated():
    assert get_start_position({"offset": 100, "line_number": 10}, file_size=5) == (0, 0)
    assert get_start_position({"offset": 4, "line_number": 2}, file_size=5) == (4, 2)
