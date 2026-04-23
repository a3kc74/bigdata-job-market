"""
Kafka producer for the speed layer.

Uses the StreamingCrawler to crawl real job postings from TopCV
and publishes them as JSON into the Kafka `raw_events` topic.

The crawled data is mapped through process.process_crawled_data()
to match the RAW_EVENT_SCHEMA before being sent to Kafka.

Throughput is controlled by crawler config (--delay-jobs, --delay-pages)
and the PRODUCER_DELAY_SECONDS env var (extra delay after each produce).

Usage:
    # Default: crawl all jobs, 2s between jobs
    python producer.py

    # Custom: specific keyword, faster throughput
    python producer.py --keyword "data engineer" --delay-jobs 1.0

    # Limit pages and events
    python producer.py --max-pages 5 --max-events 100 --no-loop
"""

import argparse
import json
import logging
import os
import sys
import time

from confluent_kafka import Producer
from dotenv import load_dotenv

# Add project root to sys.path for imports
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(os.path.dirname(_current_dir))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from apps.ingestion.crawler import StreamingCrawler, StreamingCrawlerConfig
from apps.ingestion.process import process_crawled_data

load_dotenv()

logger = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw_events")
PRODUCER_DELAY = float(os.getenv("PRODUCER_DELAY_SECONDS", "0"))


def delivery_report(err, msg):
    """Callback invoked once per produced message to report delivery status."""
    if err is not None:
        logger.error("Delivery failed: %s", err)
    else:
        logger.info(
            "Produced topic=%s partition=%s offset=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def main():
    parser = argparse.ArgumentParser(
        description="Speed Layer Producer — Crawl & Publish to Kafka"
    )
    parser.add_argument(
        "--keyword", default="", help="TopCV search keyword (default: all jobs)"
    )
    parser.add_argument("--location", default="", help="City filter")
    parser.add_argument(
        "--delay-jobs",
        type=float,
        default=2.0,
        help="Seconds between crawling each job (default: 2.0)",
    )
    parser.add_argument(
        "--delay-pages",
        type=float,
        default=5.0,
        help="Seconds between listing pages (default: 5.0)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Max listing pages per cycle (0=unlimited)",
    )
    parser.add_argument(
        "--max-events", type=int, default=0, help="Stop after N events (0=unlimited)"
    )
    parser.add_argument(
        "--no-loop", action="store_true", help="Don't restart after exhausting pages"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    # Build crawler config from CLI args
    crawler_config = StreamingCrawlerConfig(
        keyword=args.keyword,
        location=args.location,
        delay_between_jobs=args.delay_jobs,
        delay_between_pages=args.delay_pages,
        max_pages=args.max_pages,
        max_events=args.max_events,
        loop=not args.no_loop,
    )

    crawler = StreamingCrawler(crawler_config)
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    logger.info("Producer started → topic=%s, bootstrap=%s", RAW_TOPIC, BOOTSTRAP)
    logger.info(
        "Crawler config: keyword=%r, delay_jobs=%.1f, delay_pages=%.1f, "
        "max_pages=%d, max_events=%d, loop=%s",
        crawler_config.keyword,
        crawler_config.delay_between_jobs,
        crawler_config.delay_between_pages,
        crawler_config.max_pages,
        crawler_config.max_events,
        crawler_config.loop,
    )

    try:
        for raw_data in crawler.stream():
            # Map crawler output → schema-compatible dict
            event = process_crawled_data(raw_data)

            # Publish to Kafka
            producer.produce(
                RAW_TOPIC,
                key=event["job_id"],
                value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)

            logger.info(
                "Sent event #%d: %s | %s | %s",
                crawler.total_yielded,
                event["job_id"][:12],
                event["title"],
                event["location_text"],
            )

            # Optional extra delay after producing
            if PRODUCER_DELAY > 0:
                time.sleep(PRODUCER_DELAY)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user. Total events: %d", crawler.total_yielded)
    finally:
        # Flush any remaining messages
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("%d message(s) were not delivered", remaining)
        logger.info("Producer shut down.")


if __name__ == "__main__":
    main()
