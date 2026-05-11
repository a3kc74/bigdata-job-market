from apps.producer.fake_crawler_producer import get_message_key, parse_json_line, prepare_event


def test_prepare_event_original_mode_keeps_event_ts():
    event = {"job_id": "abc", "event_ts": 1776877200000}

    prepared = prepare_event(
        event,
        event_time_mode="original",
        replay_id="replay-1",
        replay_seq=1,
        timestamp_ms=1777812000123,
    )

    assert prepared["event_ts"] == 1776877200000
    assert prepared["stream_ingest_ts"] == 1777812000123
    assert prepared["replay_id"] == "replay-1"
    assert prepared["replay_seq"] == 1
    assert "original_event_ts" not in prepared


def test_prepare_event_now_mode_rewrites_event_ts_and_preserves_original():
    event = {"job_id": "abc", "event_ts": 1776877200000}

    prepared = prepare_event(
        event,
        event_time_mode="now",
        replay_id="replay-1",
        replay_seq=1,
        timestamp_ms=1777812000123,
    )

    assert prepared["event_ts"] == 1777812000123
    assert prepared["original_event_ts"] == 1776877200000
    assert prepared["stream_ingest_ts"] == 1777812000123


def test_parse_json_line_rejects_malformed_json():
    assert parse_json_line("{not-json", 1) is None


def test_get_message_key_requires_non_empty_job_id():
    assert get_message_key({"job_id": " job-1 "}) == "job-1"
    assert get_message_key({"job_id": ""}) is None
    assert get_message_key({}) is None
