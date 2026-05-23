import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path

from apps.ingestion import topcv_crawler as crawler


def build_output_path(mode: str) -> str:
    now = datetime.now()
    ingest_date = now.strftime("%Y-%m-%d")
    ts = now.strftime("%Y%m%d_%H%M%S")

    local_output_base = Path(os.getenv("CRAWLER_LOCAL_OUTPUT_DIR", "/tmp/topcv-crawler-output"))
    output_dir = local_output_base / "source=topcv" / f"ingest_date={ingest_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    return str(output_dir / f"jobs_{mode}_{ts}.jsonl")


def run_speed(args):
    output_path = build_output_path("speed")
    crawler.DATA_FILE = output_path
    crawler.DEBUG_EXPORT_CARD_LINKS = args.debug_card_links

    print(f"[SPEED] Output: {output_path}")
    print(f"[SPEED] max_pages={args.max_pages}")
    print(f"[SPEED] updated_within_minutes={args.updated_within_minutes}")
    print(f"[SPEED] speed_processed_cache_ttl_days={args.processed_ttl_days}")
    print(f"[SPEED] debug_card_links={args.debug_card_links}")

    crawler.run_master_crawler(
        max_pages=args.max_pages,
        list_pages_per_chunk=args.list_pages_per_chunk,
        detail_batch_size=args.detail_batch_size,
        updated_within_minutes=args.updated_within_minutes,
        use_checkpoint=False,
        use_processed_cache=True,
        processed_ttl_days=args.processed_ttl_days,
        start_page=1,
        batch_checkpoint=None,
    )


def run_batch(args):
    crawler.DEBUG_EXPORT_CARD_LINKS = args.debug_card_links

    if args.resume:
        checkpoint = crawler.load_batch_checkpoint()
        if checkpoint is None:
            raise RuntimeError(
                "Không có batch checkpoint hợp lệ để resume. "
                "Hãy chạy batch mới bằng: python -m apps.ingestion.run_crawler --mode batch"
            )

        crawler.DATA_FILE = checkpoint["output_file"]
        threshold_time = datetime.fromisoformat(checkpoint["threshold_time"])
        start_page = int(checkpoint.get("next_page", 1) or 1)

        print(f"[BATCH RESUME] Resume from page: {start_page}")
        print(f"[BATCH RESUME] Output: {crawler.DATA_FILE}")
        print(f"[BATCH RESUME] Threshold: {threshold_time}")
        print(f"[BATCH RESUME] debug_card_links={args.debug_card_links}")

    else:
        now = datetime.now()
        run_id = now.strftime("batch_%Y%m%d_%H%M%S")
        threshold_time = now - timedelta(days=args.days)
        output_path = build_output_path("batch")

        crawler.DATA_FILE = output_path
        start_page = 1

        checkpoint = {
            "mode": "batch",
            "run_id": run_id,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "threshold_time": threshold_time.isoformat(),
            "next_page": 1,
            "output_file": output_path,
            "completed": False,
        }
        crawler.save_batch_checkpoint(checkpoint)

        print(f"[BATCH NEW] Output: {output_path}")
        print(f"[BATCH NEW] Threshold: {threshold_time}")
        print("[BATCH NEW] Cache disabled: batch will recrawl jobs inside threshold")
        print(f"[BATCH NEW] debug_card_links={args.debug_card_links}")

    crawler.run_master_crawler(
        max_pages=args.max_pages,
        list_pages_per_chunk=args.list_pages_per_chunk,
        detail_batch_size=args.detail_batch_size,
        threshold_time=threshold_time,
        use_checkpoint=False,
        use_processed_cache=False,
        start_page=start_page,
        batch_checkpoint=checkpoint,
    )


def main():
    parser = argparse.ArgumentParser(description="TopCV crawler runner")

    parser.add_argument(
        "--mode",
        choices=["speed", "batch"],
        required=True,
        help="speed = crawl nhanh định kỳ; batch = crawl sâu theo cửa sổ ngày",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Speed mặc định 15. Batch mặc định 0, nghĩa là auto get_total_pages(session).",
    )
    parser.add_argument(
        "--list-pages-per-chunk",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--detail-batch-size",
        type=int,
        default=40,
    )
    parser.add_argument(
        "--updated-within-minutes",
        type=int,
        default=30,
        help="Speed mode: chỉ crawl job updated trong N phút gần nhất.",
    )
    parser.add_argument(
        "--processed-ttl-days",
        type=int,
        default=29,
        help="Speed mode: TTL của speed processed cache.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Batch mode: crawl job updated trong N ngày gần nhất.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Chỉ dùng cho batch: resume phiên batch lỗi từ checkpoint còn hạn.",
    )
    parser.add_argument(
        "--debug-card-links",
        action="store_true",
        help="Export toàn bộ URL candidates của từng job card vào runtime/crawler/debug_card_links/.",
    )

    args = parser.parse_args()

    if args.mode == "speed":
        if args.resume:
            raise ValueError("--resume chỉ dùng cho batch mode, không dùng cho speed.")
        if args.max_pages is None:
            args.max_pages = 15
        run_speed(args)

    elif args.mode == "batch":
        if args.max_pages is None:
            args.max_pages = 0
        run_batch(args)


if __name__ == "__main__":
    main()
