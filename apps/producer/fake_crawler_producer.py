"""Replay raw crawler JSONL records into Kafka for the speed layer demo."""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

DEFAULT_INPUT = "data/raw/raw_jobs_batch.jsonl"
DEFAULT_TOPIC = "jobs_raw"
DEFAULT_BOOTSTRAP = "localhost:9092"


@dataclass
class ProduceStats:
    read: int = 0
    sent: int = 0
    skipped: int = 0
    delivered: int = 0
    failed: int = 0


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_json_line(line: str, line_number: int) -> dict[str, Any] | None:
    stripped = line.strip()
    if not stripped:
        return None

    try:
        event = json.loads(stripped)
    except json.JSONDecodeError as exc:
        logger.warning("Skipping malformed JSON at line=%s: %s", line_number, exc)
        return None

    if not isinstance(event, dict):
        logger.warning("Skipping non-object JSON at line=%s", line_number)
        return None

    return event


def get_message_key(event: dict[str, Any]) -> str | None:
    job_id = event.get("job_id")
    if job_id is None:
        return None

    key = str(job_id).strip()
    return key or None


def prepare_event(
    event: dict[str, Any],
    *,
    event_time_mode: str,
    replay_id: str,
    replay_seq: int,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Attach stream metadata and optionally rewrite event_ts to current time."""

    if event_time_mode not in {"original", "now"}:
        raise ValueError(f"Unsupported event_time_mode: {event_time_mode}")

    timestamp = now_ms() if timestamp_ms is None else timestamp_ms
    prepared = dict(event)
    prepared["stream_ingest_ts"] = timestamp
    prepared["replay_id"] = replay_id
    prepared["replay_seq"] = replay_seq

    if event_time_mode == "now":
        if "original_event_ts" not in prepared:
            prepared["original_event_ts"] = prepared.get("event_ts")
        prepared["event_ts"] = timestamp

    return prepared


def iter_jsonl_events(path: Path) -> Iterable[tuple[int, dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            event = parse_json_line(line, line_number)
            if event is not None:
                yield line_number, event


def delivery_report(stats: ProduceStats):
    def _callback(err, msg) -> None:
        if err is not None:
            stats.failed += 1
            logger.error("Delivery failed: %s", err)
            return

        stats.delivered += 1
        logger.debug(
            "Delivered topic=%s partition=%s offset=%s key=%r",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.key(),
        )

    return _callback


def produce_with_backpressure(producer: Any, **kwargs: Any) -> None:
    while True:
        try:
            producer.produce(**kwargs)
            return
        except BufferError:
            producer.poll(1.0)


def replay_file(args: argparse.Namespace) -> ProduceStats:
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'confluent_kafka'. Install project dependencies before "
            "running the fake crawler producer."
        ) from exc

    input_path = Path(args.input)
    if not input_path.is_file():
        raise FileNotFoundError(f"Input JSONL file not found: {input_path}")

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    stats = ProduceStats()
    replay_id = args.replay_id or str(uuid.uuid4())
    sleep_seconds = max(args.sleep_ms, 0) / 1000

    logger.info(
        "Starting fake crawler replay input=%s topic=%s bootstrap=%s mode=%s loop=%s",
        input_path,
        args.topic,
        args.bootstrap_servers,
        args.event_time_mode,
        args.loop,
    )

    try:
        while True:
            sent_this_pass = 0
            for line_number, event in iter_jsonl_events(input_path):
                stats.read += 1
                key = get_message_key(event)
                if key is None:
                    stats.skipped += 1
                    logger.warning("Skipping record without job_id at line=%s", line_number)
                    continue

                replay_seq = stats.sent + 1
                prepared = prepare_event(
                    event,
                    event_time_mode=args.event_time_mode,
                    replay_id=replay_id,
                    replay_seq=replay_seq,
                )
                value = json.dumps(prepared, ensure_ascii=False, separators=(",", ":")).encode(
                    "utf-8"
                )

                produce_with_backpressure(
                    producer,
                    topic=args.topic,
                    key=key,
                    value=value,
                    callback=delivery_report(stats),
                )
                producer.poll(0)

                stats.sent += 1
                sent_this_pass += 1

                if stats.sent == 1 or stats.sent % args.log_every == 0:
                    logger.info("Sent records=%s latest_job_id=%s", stats.sent, key)

                if args.max_records and stats.sent >= args.max_records:
                    return stats

                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

            if not args.loop:
                return stats

            if sent_this_pass == 0:
                logger.warning("No valid records found in input; stopping loop")
                return stats

            logger.info("Replay pass completed; looping from start")
    finally:
        remaining = producer.flush(timeout=args.flush_timeout_seconds)
        if remaining:
            logger.warning("%s message(s) were not delivered before shutdown", remaining)
        logger.info(
            "Replay finished read=%s sent=%s delivered=%s failed=%s skipped=%s",
            stats.read,
            stats.sent,
            stats.delivered,
            stats.failed,
            stats.skipped,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Replay raw_jobs_batch.jsonl records into Kafka jobs_raw."
    )
    parser.add_argument(
        "--input",
        default=os.getenv("FAKE_CRAWLER_INPUT", DEFAULT_INPUT),
        help=f"JSONL input path (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("RAW_TOPIC", DEFAULT_TOPIC),
        help=f"Kafka topic to produce to (default: {DEFAULT_TOPIC})",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
        help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP})",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=int(os.getenv("FAKE_CRAWLER_SLEEP_MS", "1000")),
        help="Delay between records in milliseconds (default: 1000)",
    )
    parser.add_argument(
        "--event-time-mode",
        choices=("original", "now"),
        default=os.getenv("FAKE_CRAWLER_EVENT_TIME_MODE", "original"),
        help="Keep source event_ts or rewrite it to current time (default: original)",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        default=os.getenv("FAKE_CRAWLER_LOOP", "").lower() in {"1", "true", "yes"},
        help="Replay the input file repeatedly",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=int(os.getenv("FAKE_CRAWLER_MAX_RECORDS", "0")),
        help="Stop after N produced records; 0 means unlimited",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=int(os.getenv("FAKE_CRAWLER_LOG_EVERY", "10")),
        help="Log progress after every N sent records",
    )
    parser.add_argument(
        "--replay-id",
        default=os.getenv("FAKE_CRAWLER_REPLAY_ID", ""),
        help="Optional replay identifier added to each message",
    )
    parser.add_argument(
        "--flush-timeout-seconds",
        type=float,
        default=float(os.getenv("FAKE_CRAWLER_FLUSH_TIMEOUT_SECONDS", "30")),
        help="Seconds to wait for pending Kafka messages on shutdown",
    )
    return parser


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    args = build_parser().parse_args()
    if args.log_every < 1:
        raise ValueError("--log-every must be >= 1")

    replay_file(args)


if __name__ == "__main__":
    main()
