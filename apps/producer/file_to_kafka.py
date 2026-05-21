import argparse
import glob
import json
import os
from confluent_kafka import Producer
from apps.common.logger import get_logger

logger = get_logger("producer")

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"[KAFKA ERROR] Delivery failed: {err}")
    else:
        logger.info(
            f"[KAFKA OK] topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def iter_jsonl_files(input_pattern):
    files = sorted(glob.glob(input_pattern))
    if not files:
        raise FileNotFoundError(f"No files matched: {input_pattern}")
    return files


def publish_file(producer, topic, file_path):
    sent = 0

    with open(file_path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                logger.warning(f"[SKIP] Invalid JSON {file_path}:{line_no} | {e}")
                continue

            job_id = record.get("job_id")
            if not job_id:
                logger.warning(f"[SKIP] Missing job_id {file_path}:{line_no}")
                continue

            producer.produce(
                topic=topic,
                key=job_id,
                value=json.dumps(record, ensure_ascii=False),
                callback=delivery_report,
            )

            sent += 1
            producer.poll(0)

    producer.flush()
    return sent


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="JSONL file path or glob pattern, e.g. data/raw/jobs/source=topcv/ingest_date=2026-05-21/jobs_speed_*.jsonl",
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )

    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC_JOBS_RAW", "jobs_raw"),
    )

    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "topcv-crawler-producer",
        "acks": "all",
    })

    total_sent = 0

    for file_path in iter_jsonl_files(args.input):
        logger.info(f"[KAFKA] Publishing file: {file_path}")
        sent = publish_file(producer, args.topic, file_path)
        logger.info(f"[KAFKA] Sent {sent} records from {file_path}")
        total_sent += sent

    logger.info(f"[KAFKA] Total sent: {total_sent}")


if __name__ == "__main__":
    main()