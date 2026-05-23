"""Publish real crawler JSONL records to Kafka `jobs_raw`.

The TopCV crawler writes rich JSONL records. This adapter keeps that file-first
workflow, then coerces records into the raw speed-layer contract before sending
them to Kafka.
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

DEFAULT_INPUT = "data/raw/jobs/source=topcv/ingest_date=*/jobs_speed_*.jsonl"
DEFAULT_TOPIC = "jobs_raw"
DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_CHECKPOINT_FILE = "runtime/producer/crawler_jsonl_offsets.json"

PAYLOAD_FIELDS = {
    "title",
    "company_name",
    "company_details",
    "salary",
    "location",
    "monthOfExperience",
    "deadline",
    "occupationalCategory",
    "education",
    "employmentType",
    "openings",
    "description",
    "requirements",
    "income",
    "benefits",
    "extra_inf",
    "schedule",
    "skillsNeeded",
    "skillsShouldHave",
    "specialty",
    "pageText",
}


@dataclass
class ProduceStats:
    read: int = 0
    sent: int = 0
    skipped: int = 0
    delivered: int = 0
    failed: int = 0


def now_ms() -> int:
    return int(time.time() * 1000)


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def parse_json_line(line: str, line_number: int, path: Path | None = None) -> dict[str, Any] | None:
    stripped = line.strip()
    if not stripped:
        return None

    try:
        event = json.loads(stripped)
    except json.JSONDecodeError as exc:
        location = f"{path}:{line_number}" if path else f"line={line_number}"
        logger.warning("Skipping malformed JSON at %s: %s", location, exc)
        return None

    if not isinstance(event, dict):
        location = f"{path}:{line_number}" if path else f"line={line_number}"
        logger.warning("Skipping non-object JSON at %s", location)
        return None

    return event


def ensure_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        result = [str(item).strip() for item in value if item is not None and str(item).strip()]
        return result or None
    if isinstance(value, tuple | set):
        result = [str(item).strip() for item in value if item is not None and str(item).strip()]
        return result or None

    text = str(value).strip()
    if not text:
        return None

    parts = [part.strip() for part in re.split(r"\s*(?:,|;|\||\n)\s*", text) if part.strip()]
    return parts or None


def join_if_list(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list | tuple | set):
        parts = [str(item).strip() for item in value if item is not None and str(item).strip()]
        return "\n".join(parts) if parts else None
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    text = str(value).strip()
    return text or None


def parse_int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)

    text = str(value).strip()
    if not text:
        return None

    match = re.search(r"-?\d+", text.replace(".", "").replace(",", ""))
    if not match:
        return None
    return int(match.group(0))


def parse_ms_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    parsed = parse_int_or_none(value)
    return parsed


def coerce_company_details(value: Any) -> dict[str, str | None] | None:
    if not isinstance(value, dict):
        return None
    return {
        "scale": join_if_list(value.get("scale")),
        "field": join_if_list(value.get("field")),
        "address": join_if_list(value.get("address")),
    }


def coerce_payload(raw_payload: Any) -> dict[str, Any]:
    payload = dict(raw_payload) if isinstance(raw_payload, dict) else {}
    coerced = {field: payload.get(field) for field in PAYLOAD_FIELDS}

    coerced["company_details"] = coerce_company_details(coerced.get("company_details"))
    coerced["location"] = ensure_list(coerced.get("location"))
    coerced["income"] = ensure_list(coerced.get("income"))
    coerced["skillsNeeded"] = ensure_list(coerced.get("skillsNeeded"))
    coerced["skillsShouldHave"] = ensure_list(coerced.get("skillsShouldHave"))
    coerced["specialty"] = ensure_list(coerced.get("specialty"))

    coerced["requirements"] = join_if_list(coerced.get("requirements"))
    coerced["benefits"] = join_if_list(coerced.get("benefits"))
    coerced["description"] = join_if_list(coerced.get("description"))
    coerced["extra_inf"] = join_if_list(coerced.get("extra_inf"))
    coerced["schedule"] = join_if_list(coerced.get("schedule"))

    coerced["monthOfExperience"] = parse_int_or_none(coerced.get("monthOfExperience"))
    coerced["openings"] = parse_int_or_none(coerced.get("openings"))
    coerced["deadline"] = parse_ms_or_none(coerced.get("deadline"))

    for field in ("title", "company_name", "salary", "occupationalCategory", "education", "employmentType", "pageText"):
        coerced[field] = join_if_list(coerced.get(field))

    return coerced


def coerce_quality_flags(value: Any) -> dict[str, bool | None]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): flag if isinstance(flag, bool) or flag is None else bool(flag)
        for key, flag in value.items()
    }


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
    if event_time_mode not in {"original", "now"}:
        raise ValueError(f"Unsupported event_time_mode: {event_time_mode}")

    timestamp = now_ms() if timestamp_ms is None else timestamp_ms
    prepared = dict(event)
    prepared["payload"] = coerce_payload(event.get("payload"))
    prepared["quality_flags"] = coerce_quality_flags(event.get("quality_flags"))
    prepared["stream_ingest_ts"] = timestamp
    prepared["replay_id"] = replay_id
    prepared["replay_seq"] = replay_seq
    prepared["crawl_version"] = parse_int_or_none(prepared.get("crawl_version"))
    prepared["ingest_ts"] = parse_ms_or_none(prepared.get("ingest_ts")) or timestamp
    prepared["event_ts"] = parse_ms_or_none(prepared.get("event_ts")) or prepared["ingest_ts"]
    prepared["original_event_ts"] = parse_ms_or_none(prepared.get("original_event_ts"))

    if event_time_mode == "now":
        if prepared["original_event_ts"] is None:
            prepared["original_event_ts"] = prepared.get("event_ts")
        prepared["event_ts"] = timestamp

    return prepared


def expand_input_paths(patterns: Iterable[str]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        matches = glob.glob(pattern, recursive=True)
        if matches:
            paths.extend(Path(match) for match in matches)
        else:
            paths.append(Path(pattern))

    unique = sorted(dict.fromkeys(path for path in paths))
    return unique


def select_latest_path(paths: Iterable[Path]) -> list[Path]:
    existing = [path for path in paths if path.is_file()]
    if not existing:
        return []

    latest = max(existing, key=lambda path: (path.stat().st_mtime, str(path)))
    return [latest]


def iter_jsonl_events(paths: Iterable[Path]) -> Iterable[tuple[Path, int, dict[str, Any]]]:
    for path in paths:
        if not path.is_file():
            raise FileNotFoundError(f"Input JSONL file not found: {path}")

        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                event = parse_json_line(line, line_number, path)
                if event is not None:
                    yield path, line_number, event


def checkpoint_key(path: Path) -> str:
    try:
        return str(path.resolve())
    except OSError:
        return str(path)


def load_checkpoint(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"version": 1, "files": {}}

    try:
        with path.open("r", encoding="utf-8") as handle:
            checkpoint = json.load(handle)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Ignoring unreadable checkpoint %s: %s", path, exc)
        return {"version": 1, "files": {}}

    if not isinstance(checkpoint, dict):
        return {"version": 1, "files": {}}

    files = checkpoint.get("files")
    if not isinstance(files, dict):
        checkpoint["files"] = {}

    checkpoint.setdefault("version", 1)
    return checkpoint


def save_checkpoint_file(path: Path, checkpoint: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(checkpoint, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    tmp_path.replace(path)


def get_checkpoint_entry(checkpoint: dict[str, Any], path: Path) -> dict[str, Any]:
    files = checkpoint.setdefault("files", {})
    key = checkpoint_key(path)
    entry = files.get(key)
    if not isinstance(entry, dict):
        entry = {}
        files[key] = entry
    return entry


def get_start_offset(entry: dict[str, Any], file_size: int) -> int:
    offset = parse_int_or_none(entry.get("offset")) or 0
    if offset < 0 or offset > file_size:
        return 0
    return offset


def get_start_position(entry: dict[str, Any], file_size: int) -> tuple[int, int]:
    raw_offset = parse_int_or_none(entry.get("offset")) or 0
    if raw_offset < 0 or raw_offset > file_size:
        return 0, 0
    return raw_offset, parse_int_or_none(entry.get("line_number")) or 0


def iter_jsonl_events_from_offset(
    path: Path,
    *,
    offset: int,
    starting_line_number: int,
) -> Iterable[tuple[int, int, dict[str, Any] | None]]:
    if not path.is_file():
        raise FileNotFoundError(f"Input JSONL file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        handle.seek(offset)
        line_number = starting_line_number
        while True:
            before = handle.tell()
            line = handle.readline()
            if not line:
                break
            after = handle.tell()
            line_number += 1
            event = parse_json_line(line, line_number, path)
            yield line_number, after, event


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


def publish_event(
    producer: Any,
    *,
    stats: ProduceStats,
    event: dict[str, Any],
    topic: str,
    event_time_mode: str,
    replay_id: str,
    log_every: int,
) -> str | None:
    key = get_message_key(event)
    if key is None:
        stats.skipped += 1
        return None

    replay_seq = stats.sent + 1
    prepared = prepare_event(
        event,
        event_time_mode=event_time_mode,
        replay_id=replay_id,
        replay_seq=replay_seq,
    )
    value = json.dumps(prepared, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    produce_with_backpressure(
        producer,
        topic=topic,
        key=key,
        value=value,
        callback=delivery_report(stats),
    )
    producer.poll(0)

    stats.sent += 1

    if stats.sent == 1 or stats.sent % log_every == 0:
        logger.info("Sent records=%s latest_job_id=%s", stats.sent, key)

    return key


def replay_files(args: argparse.Namespace) -> ProduceStats:
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'confluent_kafka'. Install project dependencies before "
            "running the crawler JSONL producer."
        ) from exc

    input_paths = expand_input_paths(args.input)
    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    stats = ProduceStats()
    replay_id = args.replay_id or str(uuid.uuid4())
    sleep_seconds = max(args.sleep_ms, 0) / 1000

    logger.info(
        "Starting crawler JSONL replay files=%s topic=%s bootstrap=%s mode=%s",
        len(input_paths),
        args.topic,
        args.bootstrap_servers,
        args.event_time_mode,
    )

    try:
        for path, line_number, event in iter_jsonl_events(input_paths):
            stats.read += 1
            key = publish_event(
                producer,
                stats=stats,
                event=event,
                topic=args.topic,
                event_time_mode=args.event_time_mode,
                replay_id=replay_id,
                log_every=args.log_every,
            )
            if key is None:
                logger.warning("Skipping record without job_id at %s:%s", path, line_number)
                continue

            if args.max_records and stats.sent >= args.max_records:
                return stats

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        return stats
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


def process_checkpointed_paths(
    producer: Any,
    *,
    paths: list[Path],
    checkpoint: dict[str, Any],
    args: argparse.Namespace,
    stats: ProduceStats,
    replay_id: str,
) -> int:
    pending_offsets: dict[str, dict[str, Any]] = {}
    sent_before = stats.sent
    sleep_seconds = max(args.sleep_ms, 0) / 1000

    for path in paths:
        if not path.is_file():
            continue

        stat = path.stat()
        entry = get_checkpoint_entry(checkpoint, path)
        start_offset, starting_line_number = get_start_position(entry, stat.st_size)

        if start_offset == stat.st_size:
            continue

        logger.info("Reading %s from offset=%s size=%s", path, start_offset, stat.st_size)

        latest_offset = start_offset
        latest_line_number = starting_line_number
        latest_key = checkpoint_key(path)

        for line_number, offset_after_line, event in iter_jsonl_events_from_offset(
            path,
            offset=start_offset,
            starting_line_number=starting_line_number,
        ):
            latest_offset = offset_after_line
            latest_line_number = line_number
            stats.read += 1

            if event is None:
                stats.skipped += 1
            else:
                key = publish_event(
                    producer,
                    stats=stats,
                    event=event,
                    topic=args.topic,
                    event_time_mode=args.event_time_mode,
                    replay_id=replay_id,
                    log_every=args.log_every,
                )
                if key is None:
                    logger.warning("Skipping record without job_id at %s:%s", path, line_number)

            pending_offsets[latest_key] = {
                "path": str(path),
                "offset": latest_offset,
                "line_number": latest_line_number,
                "size": stat.st_size,
                "mtime": stat.st_mtime,
                "updated_at": now_iso(),
            }

            if args.max_records and stats.sent >= args.max_records:
                break

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        if args.max_records and stats.sent >= args.max_records:
            break

    sent_in_scan = stats.sent - sent_before
    if pending_offsets:
        remaining = producer.flush(timeout=args.flush_timeout_seconds)
        if remaining:
            logger.warning("%s message(s) were not delivered before checkpoint update", remaining)

        files = checkpoint.setdefault("files", {})
        for key, value in pending_offsets.items():
            files[key] = value

        checkpoint["updated_at"] = now_iso()
        save_checkpoint_file(Path(args.checkpoint_file), checkpoint)

    return sent_in_scan


def follow_files(args: argparse.Namespace) -> ProduceStats:
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'confluent_kafka'. Install project dependencies before "
            "running the crawler JSONL producer."
        ) from exc

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    stats = ProduceStats()
    replay_id = args.replay_id or str(uuid.uuid4())
    checkpoint_path = Path(args.checkpoint_file)
    checkpoint = load_checkpoint(checkpoint_path)

    logger.info(
        "Starting checkpointed crawler JSONL producer input=%s topic=%s bootstrap=%s "
        "checkpoint=%s watch=%s latest_only=%s",
        args.input,
        args.topic,
        args.bootstrap_servers,
        checkpoint_path,
        args.watch,
        args.latest_only,
    )

    try:
        while True:
            input_paths = expand_input_paths(args.input)
            if args.latest_only:
                input_paths = select_latest_path(input_paths)

            if not input_paths:
                logger.info("No matching JSONL files yet for input=%s", args.input)
            else:
                sent_in_scan = process_checkpointed_paths(
                    producer,
                    paths=input_paths,
                    checkpoint=checkpoint,
                    args=args,
                    stats=stats,
                    replay_id=replay_id,
                )
                if sent_in_scan:
                    logger.info(
                        "Checkpointed scan sent=%s total_sent=%s checkpoint=%s",
                        sent_in_scan,
                        stats.sent,
                        checkpoint_path,
                    )

            if args.max_records and stats.sent >= args.max_records:
                break

            if not args.watch:
                break

            time.sleep(args.poll_seconds)

        return stats
    finally:
        remaining = producer.flush(timeout=args.flush_timeout_seconds)
        if remaining:
            logger.warning("%s message(s) were not delivered before shutdown", remaining)
        logger.info(
            "Checkpointed producer stopped read=%s sent=%s delivered=%s failed=%s skipped=%s",
            stats.read,
            stats.sent,
            stats.delivered,
            stats.failed,
            stats.skipped,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish crawler JSONL records into Kafka jobs_raw.")
    parser.add_argument(
        "--input",
        nargs="+",
        default=os.getenv("CRAWLER_JSONL_INPUT", DEFAULT_INPUT).split(os.pathsep),
        help=f"JSONL input path or glob (default: {DEFAULT_INPUT})",
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
        default=int(os.getenv("CRAWLER_JSONL_SLEEP_MS", "0")),
        help="Delay between records in milliseconds (default: 0)",
    )
    parser.add_argument(
        "--event-time-mode",
        choices=("original", "now"),
        default=os.getenv("CRAWLER_JSONL_EVENT_TIME_MODE", "original"),
        help="Keep source event_ts or rewrite it to current time (default: original)",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=int(os.getenv("CRAWLER_JSONL_MAX_RECORDS", "0")),
        help="Stop after N produced records; 0 means unlimited",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=int(os.getenv("CRAWLER_JSONL_LOG_EVERY", "10")),
        help="Log progress after every N sent records",
    )
    parser.add_argument(
        "--replay-id",
        default=os.getenv("CRAWLER_JSONL_REPLAY_ID", ""),
        help="Optional replay identifier added to each message",
    )
    parser.add_argument(
        "--flush-timeout-seconds",
        type=float,
        default=float(os.getenv("CRAWLER_JSONL_FLUSH_TIMEOUT_SECONDS", "30")),
        help="Seconds to wait for pending Kafka messages on shutdown",
    )
    parser.add_argument(
        "--checkpoint-file",
        default=os.getenv("CRAWLER_JSONL_CHECKPOINT_FILE", DEFAULT_CHECKPOINT_FILE),
        help=f"Producer offset checkpoint file (default: {DEFAULT_CHECKPOINT_FILE})",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        default=os.getenv("CRAWLER_JSONL_WATCH", "").lower() in {"1", "true", "yes"},
        help="Keep polling input files and publish newly appended JSONL records",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=float(os.getenv("CRAWLER_JSONL_POLL_SECONDS", "2")),
        help="Seconds between watch-mode scans (default: 2)",
    )
    parser.add_argument(
        "--latest-only",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("CRAWLER_JSONL_LATEST_ONLY", "true").lower() in {"1", "true", "yes"},
        help="Only follow the newest matching JSONL file (default: true)",
    )
    parser.add_argument(
        "--replay",
        action="store_true",
        default=os.getenv("CRAWLER_JSONL_REPLAY", "").lower() in {"1", "true", "yes"},
        help="Ignore producer checkpoint and replay matching files from the beginning",
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
    if args.poll_seconds <= 0:
        raise ValueError("--poll-seconds must be > 0")

    if args.replay:
        replay_files(args)
    else:
        follow_files(args)


if __name__ == "__main__":
    main()
