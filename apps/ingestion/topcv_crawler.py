import asyncio
import json
import hashlib
import re
import time
import random
import unicodedata
import shutil
from urllib.parse import urlparse, urlunparse
from datetime import datetime, UTC, timezone, timedelta
import logging
import sys
import os
import contextlib
import inspect

from bs4 import BeautifulSoup
from curl_cffi import requests
from apps.common.logger import get_logger

logger = get_logger("crawler")
# ===========================================================
# 1. CẤU HÌNH GLOBAL & LOGGING
# ===========================================================
FAILED_LINKS_FILE = "runtime/crawler/failed_links.json"
DATA_FILE = "data/raw/jobs/source=topcv/ingest_date=manual/jobs.jsonl"
CHECKPOINT_FILE = "runtime/crawler/checkpoint.txt"
COOKIE_CACHE_FILE = "runtime/crawler/cookie_cache.json"
MISSING_JOBS_LOG_FILE = "runtime/crawler/missing_jobs.log"

# Chỉ dùng cho SPEED mode để tránh bắn/crawl trùng job đã xử lý gần đây.
# BATCH mode không dùng cache này, vì batch cần crawl lại theo threshold để bắt job được cập nhật.
SPEED_PROCESSED_JOBS_FILE = "runtime/crawler/speed_processed_jobs_29d.json"
SPEED_PROCESSED_TTL_DAYS = 29

# Chỉ dùng cho BATCH mode để resume một phiên batch bị lỗi/block/server dừng.
# Checkpoint hết hạn sau 2 ngày để tránh tuần sau resume nhầm từ page cũ.
BATCH_CHECKPOINT_FILE = "runtime/crawler/batch_checkpoint.json"
BATCH_CHECKPOINT_TTL_DAYS = 2

# Debug tùy chọn: export toàn bộ URL candidates của các job card trên list page.
# Mặc định False để không sinh nhiều file debug khi chạy thật.
DEBUG_EXPORT_CARD_LINKS = False
CARD_LINKS_DEBUG_DIR = "runtime/crawler/debug_card_links"

logger = get_logger("crawler")

BASE_URL = "https://www.topcv.vn"
SOURCE = "topcv"

MAX_LIST_PAGE_REQUEUE = 2
MAX_DETAIL_REQUEUE = 2

# Biến đếm toàn cục phục vụ chiến lược nghỉ sau mỗi 40 requests của Hoàng
global_request_count = 0 
# ===========================================================
# 2. HELPER & UTILITIES (GIỮ NGUYÊN HOÀN TOÀN LOGIC CỦA BẠN)
# ===========================================================
def save_jsonl(record, file_path=None):
    try:
        if file_path is None:
            file_path = DATA_FILE

        output_dir = os.path.dirname(file_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

        return True

    except Exception as e:
        logger.error(f"Lỗi khi lưu JSONL: {e}")
        return False

def log_failed_link(url, reason):
    try:
        os.makedirs(os.path.dirname(FAILED_LINKS_FILE), exist_ok=True)

        fail_data = {
            "url": url,
            "reason": str(reason),
            "time": datetime.now().isoformat()
        }

        data = []
        if os.path.exists(FAILED_LINKS_FILE):
            with open(FAILED_LINKS_FILE, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except:
                    pass

        data.append(fail_data)

        with open(FAILED_LINKS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    except Exception as e:
        logger.error(f"Lỗi khi log failed link: {e}")

def get_last_checkpoint(checkpoint_file=CHECKPOINT_FILE):
    try:
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, "r") as f:
                content = f.read().strip()
                if content.isdigit(): return int(content)
        return 0
    except: return 0

def save_checkpoint(page_num, checkpoint_file=CHECKPOINT_FILE):
    try:
        with open(checkpoint_file, "w") as f:
            f.write(str(page_num))
    except: pass

def save_cookie_cache(cookie_dict, user_agent):
    try:
        os.makedirs(os.path.dirname(COOKIE_CACHE_FILE), exist_ok=True)
        with open(COOKIE_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"cookies": cookie_dict, "ua": user_agent}, f)
        logger.info("[CACHE] Đã lưu bộ Cookie VIP mới vào ổ cứng.")
    except Exception as e:
        logger.error(f"[CACHE] Lỗi khi lưu vé VIP ra ổ cứng: {e}")

def load_cookie_cache():
    try:
        if os.path.exists(COOKIE_CACHE_FILE):
            with open(COOKIE_CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("cookies"), data.get("ua")
    except Exception as e:
        logger.error(f"[CACHE] Lỗi khi đọc vé VIP từ ổ cứng: {e}")
    return None, None

def normalize_url(url):
    try:
        p = urlparse(url)
        return urlunparse((p.scheme.lower(), p.netloc.lower(), p.path, "", "", ""))
    except: return url

def normalize_text(text): 
    return re.sub(r"\s+", " ", text.strip().lower()) if text else ""

def sha256_hash(text): 
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def now_ms():
    return int(datetime.now(UTC).timestamp() * 1000)

def build_job_id_from_url(url):
    normalized_url = normalize_url(url)
    return sha256_hash(f"{SOURCE}|{normalized_url}")

def load_speed_processed_jobs(
    file_path=SPEED_PROCESSED_JOBS_FILE,
    ttl_days=SPEED_PROCESSED_TTL_DAYS,
):
    """
    Load cache job_id đã xử lý trong SPEED mode.
    Cache này chỉ nhằm giảm request và tránh bắn/crawl trùng trong speed layer.
    Batch mode KHÔNG dùng cache này.
    """
    try:
        if not os.path.exists(file_path):
            return {}

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return {}

        cutoff_ms = now_ms() - ttl_days * 24 * 60 * 60 * 1000

        return {
            job_id: ts
            for job_id, ts in data.items()
            if isinstance(job_id, str) and isinstance(ts, int) and ts >= cutoff_ms
        }

    except Exception as e:
        logger.warning(f"[SPEED CACHE] Không đọc được cache speed processed jobs: {e}")
        return {}

def save_speed_processed_jobs(processed_jobs, file_path=SPEED_PROCESSED_JOBS_FILE):
    try:
        output_dir = os.path.dirname(file_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(processed_jobs, f, ensure_ascii=False, indent=2)

    except Exception as e:
        logger.error(f"[SPEED CACHE] Không lưu được cache speed processed jobs: {e}")

def mark_speed_processed_job(processed_jobs, job_id):
    if job_id:
        processed_jobs[job_id] = now_ms()

def save_batch_checkpoint(checkpoint, file_path=BATCH_CHECKPOINT_FILE):
    """
    Lưu checkpoint cho một phiên batch đang chạy.
    Checkpoint này chỉ dùng để resume khi batch bị lỗi/block/server dừng giữa chừng.
    """
    try:
        output_dir = os.path.dirname(file_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        checkpoint["updated_at"] = datetime.now().isoformat()

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)

    except Exception as e:
        logger.error(f"[BATCH CHECKPOINT] Không lưu được checkpoint: {e}")

def load_batch_checkpoint(
    file_path=BATCH_CHECKPOINT_FILE,
    ttl_days=BATCH_CHECKPOINT_TTL_DAYS,
):
    """
    Load batch checkpoint nếu còn hợp lệ.
    Hợp lệ khi:
    - mode == batch
    - completed == False
    - created_at chưa quá ttl_days
    """
    try:
        if not os.path.exists(file_path):
            return None

        with open(file_path, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)

        if checkpoint.get("mode") != "batch":
            logger.warning("[BATCH CHECKPOINT] File checkpoint không phải batch checkpoint. Bỏ qua.")
            return None

        if checkpoint.get("completed") is True:
            logger.info("[BATCH CHECKPOINT] Checkpoint đã completed. Bỏ qua.")
            return None

        created_at_raw = checkpoint.get("created_at")
        if not created_at_raw:
            logger.warning("[BATCH CHECKPOINT] Checkpoint thiếu created_at. Bỏ qua.")
            return None

        created_at = datetime.fromisoformat(created_at_raw)
        if datetime.now() - created_at > timedelta(days=ttl_days):
            logger.warning(f"[BATCH CHECKPOINT] Checkpoint đã quá hạn {ttl_days} ngày. Bỏ qua.")
            return None

        return checkpoint

    except Exception as e:
        logger.error(f"[BATCH CHECKPOINT] Không đọc được checkpoint: {e}")
        return None

def mark_batch_checkpoint_completed(checkpoint):
    if checkpoint is None:
        return

    checkpoint["completed"] = True
    checkpoint["completed_at"] = datetime.now().isoformat()
    save_batch_checkpoint(checkpoint)

def log_missing_fields(record, url, log_file=None):
    try:
        if log_file is None:
            log_file = MISSING_JOBS_LOG_FILE

        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        if not record or "quality_flags" not in record:
            return False
        flags = record["quality_flags"]
        critical_flags = {
            "salary": "has_salary_info", "location": "has_location_info",
            "experience": "has_experience_info", "requirements": "has_requirements",
            "description": "has_description", "benefits": "has_benefits"
        }
        missing_fields = [field for field, flag in critical_flags.items() if not flags.get(flag)]
        if missing_fields:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] THIẾU {missing_fields} | URL: {url}\n")
            return True 
        return False 
    except: return False

def looks_blocked_or_empty(html: str) -> bool:
    #if not html or len(html) < 1000: return True
    if not html: return True
    return False
def check_and_rotate_session(current_session, threshold=40):
    """
    Không còn tự tạo socket/session mới sau mỗi threshold request.

    Lý do:
    - Với TopCV + Cloudflare, tạo socket định kỳ quá thường xuyên có thể làm pattern lạ hơn.
    - Session mới chỉ được tạo trong recover_blocked_request(), sau khi đã thử cookie mới
      trên session hiện tại mà vẫn bị chặn.
    """
    return current_session


def create_topcv_session(cookie_dict, user_agent):
    """
    Tạo session curl_cffi giả lập Chrome.
    Chỉ gọi khi khởi tạo ban đầu hoặc khi cần tạo socket/session mới sau block.
    """
    return requests.Session(
        impersonate="chrome120",
        headers={"User-Agent": user_agent, "Referer": "https://www.topcv.vn/"},
        cookies=cookie_dict
    )


def is_blocked_response(response):
    """
    Check block ở mức tối thiểu, không dùng blocked_markers để tránh nhận nhầm HTML hợp lệ.
    """
    if response is None:
        return True
    return response.status_code in [403, 401] or looks_blocked_or_empty(response.text)


def recover_blocked_request(session, url, context="REQUEST"):
    """
    Cơ chế hồi phục khi bị chặn:
    1. Lấy cookie mới bằng nodriver.
    2. Thử lại trên session/socket hiện tại trước.
    3. Nếu vẫn bị chặn mới đóng session và tạo socket/session mới.
    4. Thử lại thêm một lần bằng session mới.
    """
    logger.warning(f"[{context}] Bị chặn. Thử lấy cookie mới trước, chưa tạo socket mới ngay.")
    time.sleep(random.uniform(180, 300))

    cookie_dict, user_agent = execute_harvester_with_breaker()
    if not cookie_dict:
        logger.error(f"[{context}] Không lấy được cookie mới.")
        return session, None

    save_cookie_cache(cookie_dict, user_agent)

    # Bước 1: thử cookie mới trên session hiện tại.
    try:
        session.cookies.clear()
        session.cookies.update(cookie_dict)
        session.headers.update({"User-Agent": user_agent, "Referer": "https://www.topcv.vn/"})

        check_and_trigger_global_cooldown()
        response = session.get(url, timeout=15)
    except Exception as e:
        logger.error(f"[{context}] Lỗi khi retry bằng cookie mới trên session hiện tại: {e}")
        response = None

    if response is not None and not is_blocked_response(response):
        logger.info(f"[{context}] Hồi phục thành công bằng cookie mới trên session hiện tại.")
        return session, response

    # Bước 2: vẫn bị chặn -> lúc này mới tạo socket/session mới.
    logger.warning(f"[{context}] Vẫn bị chặn sau khi thử cookie mới. Tạo socket/session mới và thử lại một lần.")
    try:
        session.close()
    except Exception:
        pass

    time.sleep(random.uniform(10, 20))
    session = create_topcv_session(cookie_dict, user_agent)

    try:
        check_and_trigger_global_cooldown()
        response = session.get(url, timeout=15)
    except Exception as e:
        logger.error(f"[{context}] Lỗi khi retry bằng session mới: {e}")
        response = None

    return session, response

def check_and_trigger_global_cooldown():
    global global_request_count
    global_request_count += 1
    if global_request_count % 40 == 0:
        cooldown = random.uniform(20, 30) # Nghỉ dài 2.5 - 4.5 phút
        logger.info(f"[GLOBAL COOLDOWN] Đạt ngưỡng {global_request_count} requests. Hệ thống nghỉ xả nhiệt {cooldown:.1f} giây...")
        time.sleep(cooldown)


def requeue_list_page_or_fail(page_queue, page_meta, list_url, reason, stats):
    """
    Đưa page list lỗi về cuối queue thay vì retry dồn tại chỗ.
    Sau MAX_LIST_PAGE_REQUEUE lần vẫn lỗi thì mới ghi failed_links.
    """
    retry_count = int(page_meta.get("retry_count", 0))

    if retry_count < MAX_LIST_PAGE_REQUEUE:
        page_meta["retry_count"] = retry_count + 1
        page_queue.append(page_meta)
        logger.warning(
            f"[LIST PAGE RETRY QUEUE] Page {page_meta.get('page')} lỗi, đưa về cuối queue "
            f"lần {page_meta['retry_count']}/{MAX_LIST_PAGE_REQUEUE}: {reason}"
        )
        return True

    logger.error(
        f"[LIST PAGE] Bỏ page {page_meta.get('page')} sau nhiều lần quay lại: {reason}"
    )
    log_failed_link(list_url, reason)
    stats["failed_requests"] += 1
    return False


def requeue_detail_or_fail(links_queue, job_meta, link, reason, stats):
    """
    Đưa detail link lỗi về cuối queue thay vì retry dồn tại chỗ.
    Sau MAX_DETAIL_REQUEUE lần vẫn lỗi thì mới ghi failed_links.
    """
    retry_count = int(job_meta.get("detail_retry_count", 0))

    if retry_count < MAX_DETAIL_REQUEUE:
        job_meta["detail_retry_count"] = retry_count + 1
        links_queue.append(job_meta)
        logger.warning(
            f"[DETAIL RETRY QUEUE] Link lỗi, đưa về cuối queue "
            f"lần {job_meta['detail_retry_count']}/{MAX_DETAIL_REQUEUE}: {link} | {reason}"
        )
        return True

    logger.error(f"[DETAIL] Bỏ link sau nhiều lần quay lại: {link} | {reason}")
    log_failed_link(link, reason)
    stats["failed_requests"] += 1
    return False
# ===========================================================
# 3. GIAI ĐOẠN 1: HARVESTER (LẤY VÉ VIP BẰNG NODRIVER)
# ===========================================================
async def get_vip_ticket():
    """
    Lấy cookie TopCV bằng nodriver.

    Bản merge:
    - Giữ cleanup kỹ của topcv_crawler_1.py để giảm lỗi event loop/subprocess.
    - Thêm cấu hình chạy ổn hơn trong Docker/Kubernetes từ topcv_crawler.py:
      headless env, no_sandbox, --disable-dev-shm-usage, Chrome profile riêng.
    """
    browser = None
    chrome_profile_dir = None

    try:
        import nodriver as uc

        logger.info("[HARVESTER] Đang mở Chrome tàng hình (nodriver)...")

        headless = os.getenv("CRAWLER_HEADLESS", "true").lower() in {"1", "true", "yes"}
        browser_executable_path = os.getenv("BROWSER_EXECUTABLE_PATH")

        chrome_profile_dir = os.getenv(
            "CRAWLER_CHROME_PROFILE_DIR",
            f"/tmp/topcv_chrome_profile_{os.getpid()}",
        )
        shutil.rmtree(chrome_profile_dir, ignore_errors=True)
        os.makedirs(chrome_profile_dir, exist_ok=True)

        browser_kwargs = {
            "headless": headless,
            "no_sandbox": True,
            "user_data_dir": chrome_profile_dir,
            "browser_args": [
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--window-size=1366,768",
                "--disable-blink-features=AutomationControlled",
            ],
        }

        if browser_executable_path:
            browser_kwargs["browser_executable_path"] = browser_executable_path
        elif os.path.exists("/usr/bin/google-chrome"):
            browser_kwargs["browser_executable_path"] = "/usr/bin/google-chrome"
        elif os.path.exists("/usr/bin/chromium"):
            browser_kwargs["browser_executable_path"] = "/usr/bin/chromium"
        elif os.path.exists("/usr/bin/chromium-browser"):
            browser_kwargs["browser_executable_path"] = "/usr/bin/chromium-browser"

        browser = await uc.start(**browser_kwargs)

        logger.info("[HARVESTER] Đang tiến vào TopCV...")
        page = await browser.get("https://www.topcv.vn")

        logger.info("[HARVESTER] Đang rà soát màng lọc Cloudflare...")
        for _ in range(90):
            title = await page.evaluate("document.title")
            if "Just a moment" not in title and "Cloudflare" not in title:
                break
            await asyncio.sleep(1)

        await asyncio.sleep(10)

        cookies = await browser.cookies.get_all()
        cookie_dict = {c.name: c.value for c in cookies}
        user_agent = await page.evaluate("navigator.userAgent")

        if cookie_dict:
            logger.info(f"[HARVESTER] Tuyệt vời! Đã hốt trọn bộ {len(cookie_dict)} Cookies.")
            return cookie_dict, user_agent

        logger.error("[HARVESTER] Thất bại! Không kéo được Cookie nào về.")
        return None, None

    except Exception as e:
        logger.error(f"[HARVESTER] Lỗi nghiêm trọng: {e}")
        return None, None

    finally:
        if browser is not None:
            # Đóng tab nếu có.
            with contextlib.suppress(Exception):
                for tab in list(getattr(browser, "tabs", []) or []):
                    await tab.close()

            # Đóng websocket/CDP connection nếu còn.
            with contextlib.suppress(Exception):
                conn = getattr(browser, "connection", None)
                if conn:
                    await conn.aclose()

            # Gọi stop của nodriver.
            with contextlib.suppress(Exception):
                browser.stop()

            # Chờ process Chrome chết hẳn nếu nodriver còn giữ _process.
            proc = getattr(browser, "_process", None)
            if proc is not None:
                with contextlib.suppress(Exception):
                    proc.terminate()

                with contextlib.suppress(Exception):
                    wait_result = proc.wait()
                    if inspect.isawaitable(wait_result):
                        await wait_result

        if chrome_profile_dir and not os.getenv("CRAWLER_KEEP_CHROME_PROFILE"):
            with contextlib.suppress(Exception):
                shutil.rmtree(chrome_profile_dir, ignore_errors=True)


# Cơ chế circuit breaker: nếu xin cookie lỗi thì nghỉ một khoảng rồi thử lại lần cuối.
async def execute_harvester_with_breaker_async():
    cookie_dict, user_agent = await get_vip_ticket()

    if not cookie_dict:
        sleep_seconds = int(os.getenv("HARVESTER_BREAKER_SLEEP_SECONDS", "60"))
        logger.warning(f"[⚠️ BREAKER] Không lấy được vé VIP. Nghỉ {sleep_seconds}s rồi thử lại...")
        await asyncio.sleep(sleep_seconds)

        logger.info("[⚠️ BREAKER] Hết thời gian cách ly. Tiến hành thử lại lần cuối cùng...")
        cookie_dict, user_agent = await get_vip_ticket()

    return cookie_dict, user_agent


def execute_harvester_with_breaker():
    return asyncio.run(execute_harvester_with_breaker_async())

# ===========================================================
# 4. DATA EXTRACTORS (GIỮ NGUYÊN 100% LOGIC TRÍCH XUẤT CỦA BẠN)
# ===========================================================
def parse_topcv_time(time_str):
    """
    Chuyển chuỗi thời gian update trên TopCV thành datetime.
    Ví dụ:
    - "Cập nhật vừa xong"
    - "Cập nhật 12 giây trước"
    - "Cập nhật 12 phút trước"
    - "Cập nhật 2 giờ trước"
    - "Cập nhật hôm qua"
    - "Cập nhật 3 ngày trước"
    - "21/05/2026"
    """
    if not time_str:
        return None

    text = str(time_str).lower().strip()
    text = re.sub(r"^cập nhật\s*", "", text).strip()
    now = datetime.now()

    if "vừa xong" in text:
        return now

    match = re.search(r"(\d+)\s*giây", text)
    if match:
        return now - timedelta(seconds=int(match.group(1)))

    match = re.search(r"(\d+)\s*phút", text)
    if match:
        return now - timedelta(minutes=int(match.group(1)))

    match = re.search(r"(\d+)\s*giờ", text)
    if match:
        return now - timedelta(hours=int(match.group(1)))

    if "hôm qua" in text:
        return now - timedelta(days=1)

    match = re.search(r"(\d+)\s*ngày", text)
    if match:
        return now - timedelta(days=int(match.group(1)))

    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass

    return None


def get_topcv_listing_updated_text(job_item):
    """
    Lấy text update từ job card trên list page.
    HTML TopCV thường để trong:
    <label class="address mobile-hidden label-update" data-original-title="Cập nhật 12 phút trước">
    """
    candidates = [
        "label.label-update",
        "label[data-original-title*='Cập nhật']",
        "label[original-title*='Cập nhật']",
        "[data-original-title*='Cập nhật']",
        "[original-title*='Cập nhật']",
    ]

    for selector in candidates:
        tag = job_item.select_one(selector)
        if not tag:
            continue

        updated_text = (
            tag.get("data-original-title")
            or tag.get("original-title")
            or tag.get("title")
            or tag.get_text(" ", strip=True)
        )

        if updated_text:
            return updated_text.strip()

    return None


def get_topcv_job_url_candidates_from_card(job_item):
    """
    Lấy toàn bộ URL candidates trong 1 job card để debug.
    Không quyết định đúng/sai ở đây, chỉ gom URL tiềm năng.
    """
    candidates = []

    possible_attrs = [
        "data-redirect-to",
        "data-url",
        "data-href",
        "data-link",
        "data-target",
    ]

    # 1. Quét các attribute có thể chứa URL redirect.
    for tag in job_item.select("*"):
        for attr in possible_attrs:
            value = tag.get(attr)
            if value:
                candidates.append(value)

    # 2. Fallback sang href bình thường.
    for a in job_item.select("a[href]"):
        candidates.append(a.get("href"))

    normalized_candidates = []
    for href in candidates:
        if not href:
            continue

        href = str(href).strip()

        if not href or href.startswith("javascript:"):
            continue

        if href.startswith("//"):
            href = "https:" + href

        if href.startswith("/"):
            href = BASE_URL + href

        href = normalize_url(href)
        normalized_candidates.append(href)

    # Dedup nhưng vẫn giữ thứ tự.
    return list(dict.fromkeys(normalized_candidates))


def is_topcv_job_detail_url(url):
    """
    Nhận diện URL detail job hợp lệ trên TopCV.

    Dạng thường:
    - /viec-lam/...html

    Dạng brand:
    - /brand/<company>/tuyen-dung/...-j<job_id>.html
    """
    if not url:
        return False

    parsed = urlparse(url)
    path = parsed.path.lower()

    if "/viec-lam/" in path and path.endswith(".html"):
        return True

    if "/brand/" in path and "/tuyen-dung/" in path and path.endswith(".html"):
        return True

    return False


def get_topcv_job_url_from_card(job_item):
    """
    Lấy đúng link việc làm từ job card TopCV.

    Hỗ trợ cả:
    - link thường: /viec-lam/...html
    - link brand: /brand/.../tuyen-dung/...-jXXXX.html
    """
    candidates = []

    possible_attrs = [
        "data-redirect-to",
        "data-url",
        "data-href",
        "data-link",
        "data-target",
    ]

    for tag in job_item.select("*"):
        for attr in possible_attrs:
            value = tag.get(attr)
            if value:
                candidates.append(value)

    for a in job_item.select("a[href]"):
        candidates.append(a.get("href"))

    for href in candidates:
        if not href:
            continue

        href = str(href).strip()

        if not href or href.startswith("javascript:"):
            continue

        if href.startswith("//"):
            href = "https:" + href

        if href.startswith("/"):
            href = BASE_URL + href

        href = normalize_url(href)

        if is_topcv_job_detail_url(href):
            return href

    return None


def export_debug_card_links(soup, page_num):
    """
    Xuất toàn bộ card links của page để debug.
    Mục tiêu: xem đủ 50 card, card nào có selected_url, card nào không.
    """
    if not DEBUG_EXPORT_CARD_LINKS:
        return

    try:
        os.makedirs(CARD_LINKS_DEBUG_DIR, exist_ok=True)

        job_items = soup.select(".job-item-search-result")
        if not job_items:
            job_items = soup.select("[data-job-id], .job-item")

        rows = []
        for idx, item in enumerate(job_items, start=1):
            candidates = get_topcv_job_url_candidates_from_card(item)
            selected_url = get_topcv_job_url_from_card(item)
            updated_text = get_topcv_listing_updated_text(item)
            updated_time = parse_topcv_time(updated_text)

            rows.append({
                "card_index": idx,
                "selected_url": selected_url,
                "updated_text": updated_text,
                "updated_time": updated_time.isoformat() if isinstance(updated_time, datetime) else None,
                "candidate_urls": candidates,
                "card_text_preview": item.get_text(" ", strip=True)[:300],
            })

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(
            CARD_LINKS_DEBUG_DIR,
            f"card_links_page_{page_num}_{ts}.json"
        )

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)

        logger.info(
            f"[DEBUG CARD LINKS] Đã export {len(rows)} card links page {page_num}: {output_path}"
        )

    except Exception as e:
        logger.error(f"[DEBUG CARD LINKS] Lỗi khi export card links page {page_num}: {e}")


def extract_fresh_jobs_from_listing_page(soup, threshold_time=None):
    """
    Trả về danh sách job mới trên 1 list page.

    Early-stop rule:
    - Không dừng khi gặp 1 job cũ.
    - Duyệt hết page.
    - Nếu page có thời gian parse được nhưng không có job nào >= threshold_time -> dừng các page sau.
    """
    job_items = soup.select(".job-item-search-result")

    # Fallback nhẹ nếu TopCV đổi class.
    if not job_items:
        job_items = soup.select("[data-job-id], .job-item")

    results = []
    total_job_cards = len(job_items)
    parseable_time_count = 0
    fresh_count = 0
    old_count = 0
    no_url_count = 0
    no_time_count = 0

    for item in job_items:
        job_url = get_topcv_job_url_from_card(item)
        if not job_url:
            no_url_count += 1
            continue

        updated_text = get_topcv_listing_updated_text(item)
        updated_time = parse_topcv_time(updated_text)

        if updated_time is not None:
            parseable_time_count += 1
        else:
            no_time_count += 1

        is_fresh = True
        if threshold_time is not None:
            # Nếu đang crawl theo ngưỡng thời gian mà không parse được thời gian,
            # không coi job đó là fresh để tránh lấy nhầm dữ liệu cũ.
            is_fresh = updated_time is not None and updated_time >= threshold_time

        if is_fresh:
            fresh_count += 1
            results.append({
                "url": job_url,
                "updated_text": updated_text,
                "updated_time": updated_time,
                "crawl_time": datetime.now(),
            })
        else:
            old_count += 1

    should_stop_after_page = False
    if threshold_time is not None:
        # Dừng sớm chỉ khi page có job card và có ít nhất 1 thời gian parse được,
        # nhưng không có job nào còn nằm trong cửa sổ thời gian cần crawl.
        should_stop_after_page = total_job_cards > 0 and parseable_time_count > 0 and fresh_count == 0

    summary = {
        "total_job_cards": total_job_cards,
        "parseable_time_count": parseable_time_count,
        "fresh_count": fresh_count,
        "old_count": old_count,
        "no_url_count": no_url_count,
        "no_time_count": no_time_count,
        "should_stop_after_page": should_stop_after_page,
    }

    return results, summary


def extract_json_ld(soup):
    scripts = soup.find_all("script", type="application/ld+json")
    for sc in scripts:
        try:
            data = json.loads(sc.string)
            if isinstance(data, list):
                for d in data:
                    if d.get("@type") == "JobPosting":
                        return d
            elif data.get("@type") == "JobPosting":
                return data
        except:
            continue
    return {}


# =========================
# META
# =========================
def extract_meta(soup):
    meta = {}
    for tag in soup.find_all("meta"):
        k = tag.get("name") or tag.get("property")
        v = tag.get("content")
        if k and v:
            meta[k] = v
    return meta

# =========================
# HTML FALLBACK
# =========================
def text_or_none(el):
    return el.get_text(strip=True) if el else None


def extract_company_html(soup):
    return text_or_none(soup.select_one(".company-name, .company a"))

def extract_salary_html(soup):
    # Bước 1: Tìm qua Class quen thuộc (Đã thêm bẫy chữ "thoả" gõ sai)
    salary_html = soup.select_one(
        ".job-detail__info--section-content-value, "
        ".premium-job-description__box .title-salary, "
        ".section-salary .job-detail__info--section-content-value"
    )
    
    if salary_html:
        text = salary_html.get_text(strip=True).lower()
        if any(k in text for k in ["triệu", "tr", "usd", "thỏa", "thoả", "vnđ"]):
            return salary_html.get_text(strip=True)

    # Bước 2: Rơi xuống lớp Regex quét toàn bộ text
    raw_text = soup.get_text(" ", strip=True).lower()
    
    # KHIÊN BẢO VỆ 1: Bắt chặt chữ "Thỏa thuận" nếu nó nằm gần chữ "Lương/Thu nhập"
    if re.search(r'(mức lương|thu nhập|lương)[\s:]*(thỏa thuận|thoả thuận|cạnh tranh)', raw_text):
        return "Thỏa thuận"
        
    # TẬP HỢP TIỀN TỐ: Bắt các chữ đứng trước số tiền (có thể có hoặc không)
    # Ví dụ: từ, tới, đến, lên tới, lên đến, khoảng, trên, dưới, hơn...
    prefix_regex = r'(?:(?:từ|đến|tới|lên\s+tới|lên\s+đến|khoảng|trên|dưới|hơn)\s+)?'
    
    # KHIÊN BẢO VỆ 2: Bắt số tiền (Bắt buộc phải đứng sau chữ Lương/Thu nhập)
    # Gom toàn bộ: (Tiền tố + Số tiền + Đơn vị) vào chung Group 1 để lấy trọn vẹn
    context_match = re.search(r'(?:lương|thu nhập)[\s:]*?(' + prefix_regex + r'\d+[\.,]?\d*\s*(?:-\s*\d+[\.,]?\d*\s*)?(?:triệu|tr|vnđ|usd))', raw_text)
    if context_match:
        return context_match.group(1).title()

    # KHIÊN BẢO VỆ 3: Nếu bí quá, quét tìm số "Triệu" hoặc "USD" trơ trọi. 
    # Vẫn giữ nguyên cụm prefix_regex để lấy được chữ "Khoảng 15 Triệu" thay vì chỉ "15 Triệu"
    safe_match = re.search(r'(' + prefix_regex + r'\d+[\.,]?\d*\s*(?:-\s*\d+[\.,]?\d*\s*)?(?:triệu|tr|usd))', raw_text)
    if safe_match:
        return safe_match.group(1).title()

    return "Thỏa thuận / Không hiển thị"

def extract_level_html(soup):
    return text_or_none(soup.select_one(".job-level"))

def extract_schedule(soup):
    h_tag = soup.find(
        lambda tag: tag.name in ["h2", "h3"] 
        and "Thời gian làm việc" in tag.get_text()
    )
    if h_tag:
        # 2. Từ thẻ h3, đi ngược lên thẻ cha bao ngoài cùng của khối này
        parent_item = h_tag.find_parent("div", class_="job-description__item")
        
        if parent_item:
            # 3. Tìm thẻ div chứa nội dung chi tiết
            content_div = parent_item.find("div", class_="job-description__item--content")
            
            if content_div:
                # 4. Lấy tất cả text bên trong, các dòng cách nhau bằng dấu xuống dòng (\n)
                # strip=True giúp dọn dẹp khoảng trắng thừa ở đầu/cuối mỗi dòng
                return content_div.get_text(separator="\n", strip=True)
                
    return None

def extract_exp(soup):
    # Trỏ từ thẻ cha có class "section-experience" vào thẻ con chứa giá trị
    element = soup.select_one(".section-experience .job-detail__info--section-content-value")
    return text_or_none(element)

def extract_location_html(soup):
    sections = soup.select('.job-description__item')

    for sec in sections:
        title = sec.find('h3')
        if title and "Địa điểm làm việc" in title.get_text():

            content = sec.select_one('.job-description__item--content')
            if not content:
                return []

            locations = []

            for item in content.find_all('div'):

                # bỏ div cha (tránh gộp text)
                if item.find('div'):
                    continue

                text = item.get_text(" ", strip=True)

                if text and "địa điểm khác" not in text.lower():
                    locations.append(text)

            return list(dict.fromkeys(locations))  # dedup

    return []

def extract_dl(soup):
    dl = soup.select_one(".job-detail__info--deadline-date")
    return text_or_none(dl)

def extract_income(soup):
    sections = soup.select(".job-description__item")

    for sec in sections:
        title = sec.find("h3")
        if title and "Thu nhập" in title.get_text(strip=True):

            content = sec.select_one(".job-description__item--content")
            if not content:
                return []

            return [
                text for li in content.find_all("li")
                if (text := li.get_text(strip=True))
            ]
    return None
import re

def extract_description(soup):
    target_classes = [
        "job-description__item", 
        "box-info", 
        "job-detail-content",
        "job-description",
        "premium-job-description__box"
    ]
    
    inner_classes = [
        "job-description__item--content", 
        "content-tab",
        "premium-job-description__box--content"
    ]
    
    description_keywords = [
        "mô tả công việc", "chi tiết công việc", "nhiệm vụ", "job description"
    ]
    
    for item in soup.find_all("div", class_=target_classes):
        heading_tag = item.find(['h1', 'h2', 'h3'])
        if not heading_tag:
            continue

        heading_text = heading_tag.get_text(strip=True).lower()

        if any(keyword in heading_text for keyword in description_keywords):
            content_div = item.find("div", class_=inner_classes)
            if not content_div:
                continue

            # =========================
            # FIX QUAN TRỌNG: NUKE <br> BẰNG REGEX TRƯỚC KHI XỬ LÝ
            # =========================
            # 1. Chuyển toàn bộ khối div bị lỗi thành chuỗi HTML thô
            html_str = str(content_div)
            
            # 2. Tiêu diệt mọi biến thể của thẻ br (<br>, </br>, <br/>) và biến thành \n
            html_str = re.sub(r'</?br\s*/?>', '\n', html_str, flags=re.IGNORECASE)
            
            # 3. Nạp lại HTML "sạch" vào BeautifulSoup để xử lý tiếp
            clean_soup = BeautifulSoup(html_str, "html.parser")

            # 4. Xóa thẻ inline để tránh gãy text
            for tag in clean_soup.find_all(['strong', 'b', 'span', 'em', 'i', 'u', 'a']):
                tag.unwrap()

            # (BỎ QUA BƯỚC XỬ LÝ <br> VÌ ĐÃ LÀM Ở TRÊN RỒI)

            # 5. list → bullet
            for li in clean_soup.find_all('li'):
                li.insert_before('\n- ')

            # 6. block → xuống dòng
            for block in clean_soup.find_all(['p', 'div']):
                block.append('\n')

            # 7. LẤY TEXT
            raw_text = clean_soup.get_text()

            # =========================
            # CLEAN TEXT (Giữ nguyên code cũ của bạn)
            # =========================
            text = re.sub(r'[ \t]+', ' ', raw_text)
            text = re.sub(r'\s+([,.:])', r'\1', text)
            text = re.sub(r'"\s+', '"', text)
            text = re.sub(r'\s+"', '"', text)
            text = re.sub(r'\n\s*-\s*\n', '\n- ', text)
            text = re.sub(r'\n+', '\n', text)

            return text.strip()

    return None


def extract_job_requirements(soup):
    # 1. Danh sách các class "vỏ bọc"
    target_classes = [
        "premium-job-description__box job-detail-section requirement",
        "job-description__item job-detail-section requirement",
        "box-info job-detail-section requirement"
    ]
    
    # 2. Danh sách các class "phần ruột"
    inner_classes = [
        "premium-job-description__box--content",
        "job-description__item--content",
        "content-tab"
    ]
    
    # 3. Danh sách từ khóa quét tiêu đề (tối ưu hóa để bắt được nhiều case)
    requirement_keywords = [
        "yêu cầu ứng viên", "yêu cầu công việc", "kỹ năng", "chuyên môn", "job requirements"
    ]
    
    job_items = soup.find_all("div", class_=target_classes)
    
    for item in job_items:
        heading_tag = item.find(['h1', 'h2', 'h3'])
        
        # Luôn check NoneType trước
        if heading_tag:
            heading_text = heading_tag.get_text(strip=True).lower()
            
            # Quét kiểm tra xem có khớp từ khóa Yêu cầu không
            if any(keyword in heading_text for keyword in requirement_keywords):
                content_div = item.find("div", class_=inner_classes)
                
                if content_div:
                    # CÁCH LY DOM
                    local_soup = BeautifulSoup(str(content_div), "html.parser")
                    
                    # 1. Biến các thẻ <br> thành dấu xuống dòng trên Mini Soup
                    for br in local_soup.find_all('br'):
                        br.replace_with('\n')
                    
                    # 2. Xử lý danh sách
                    for li in local_soup.find_all('li'):
                        li.insert(0, '\n- ')
                        
                    # 3. Phân tách đoạn văn
                    for block in local_soup.find_all(['p', 'div']):
                        block.append('\n')

                    # 4. Lấy text từ Mini Soup
                    raw_text = local_soup.get_text(separator=' ')
                    
                    # 5. Dọn dẹp rác HTML sinh ra do khoảng trắng thừa
                    clean_text = re.sub(r'[ \t]+', ' ', raw_text)
                    clean_text = re.sub(r' \n |\n | \n', '\n', clean_text)
                    clean_text = re.sub(r'\n+', '\n', clean_text)
                    
                    return clean_text.strip()
                
    return None
def extract_benefits(soup):
    target_classes = [
        "premium-job-description__box job-detail-section benefit",
        "job-description__item job-detail-section benefit",
        "box-info job-detail-section benefit"
    ]
    
    inner_classes = [
        "premium-job-description__box--content",
        "job-description__item--content",
        "content-tab"
    ]
    
    benefit_keywords = [
        "quyền lợi", "quyền lợi được hưởng", "chế độ đãi ngộ", "phúc lợi", "benefit"
    ]
    
    job_items = soup.find_all("div", class_=target_classes)
    
    for item in job_items:
        heading_tag = item.find(['h1', 'h2', 'h3'])
        
        if heading_tag:
            heading_text = heading_tag.get_text(strip=True).lower()
            
            if any(keyword in heading_text for keyword in benefit_keywords):
                content_div = item.find("div", class_=inner_classes)
                
                if content_div:
                    # CÁCH LY DOM
                    local_soup = BeautifulSoup(str(content_div), "html.parser")
                    
                    # 1. Biến các thẻ <br> thành dấu xuống dòng trên Mini Soup
                    for br in local_soup.find_all('br'):
                        br.replace_with('\n')
                    
                    # 2. Xử lý danh sách
                    for li in local_soup.find_all('li'):
                        li.insert(0, '\n- ')
                        
                    # 3. Phân tách đoạn văn
                    for block in local_soup.find_all(['p', 'div']):
                        block.append('\n')

                    # 4. Lấy text từ Mini Soup
                    raw_text = local_soup.get_text(separator=' ')
                    
                    # 5. Dọn dẹp rác HTML sinh ra do khoảng trắng thừa
                    clean_text = re.sub(r'[ \t]+', ' ', raw_text)       # Gom nhiều dấu cách/tab thành 1 dấu cách
                    clean_text = re.sub(r' \n |\n | \n', '\n', clean_text) # Xóa dấu cách thừa dính sát vào dấu xuống dòng
                    clean_text = re.sub(r'\n+', '\n', clean_text)       # Gom nhiều dòng trống liên tiếp thành 1 dòng

                    return clean_text.strip()
                
    return None

def normalize(text):
    text = text.lower().strip()
    text = text.replace('đ', 'd')
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    return text

def extract_sections(soup):
    target_classes = [
        "job-description__item", 
        "box-info", #
        "box-address", #
        "premium-job-description__box", #1
        "job-description__custom-form-job" #2
    ]

    sections = {}

    requirements, benefits, description = [], [], []
    income, city, schedule = [], [], []

    # ===== STEP 1: gom section =====
    all_sections = []
    for cls in target_classes:
        all_sections.extend(soup.select(f".{cls}"))

    # ===== STEP 2: bỏ section cha (Thuật toán quét nội bộ chính xác 100%) =====
    filtered_sections = []
    for sec in all_sections:
        is_parent = False
        for cls in target_classes:
            # Lệnh select_one sẽ đâm sâu vào các thẻ con BÊN TRONG sec.
            # Nếu phát hiện sec này đang ôm một section con khác -> Nó là thẻ cha (wrapper) -> Bỏ qua.
            if sec.select_one(f".{cls}"):
                is_parent = True
                break
                
        # Nếu không chứa section con nào -> Đây chính là thẻ ruột chứa text -> Lấy ngay!
        if not is_parent:
            filtered_sections.append(sec)

    # ===== STEP 3: parse =====
    for sec in filtered_sections:
        # 1. CÁCH LY DOM
        local_soup = BeautifulSoup(str(sec), "html.parser")
        
        # 2. XÓA TIÊU ĐỀ
        title_tag = local_soup.find(['h1', 'h2', 'h3', 'h4', 'h5'])
        if not title_tag:
            title_tag = local_soup.find(class_=re.compile(r'title', re.I))
            
        if not title_tag:
            continue

        original_title = title_tag.get_text(strip=True)
        title_tag.extract() # Bứng gốc tiêu đề

        # 3. QUY TRÌNH CHUẨN HÓA THẺ (Format DOM)
        # Bật gạch đầu dòng cho thẻ <li>
        for li in local_soup.find_all('li'):
            li.insert(0, '\n- ')
            
        # Biến <br> thành dấu xuống dòng
        for br in local_soup.find_all('br'):
            br.replace_with('\n')
            
        # Thêm xuống dòng sau mỗi đoạn văn (tránh dính chữ khi dùng separator=' ')
        for block in local_soup.find_all(['p', 'div']):
            block.append('\n')

        # Unwrap các thẻ inline để không cản trở việc lấy text
        for tag in local_soup.find_all(['strong','b','span','em','i','u','a','font']):
            tag.unwrap()

        # 4. LẤY TEXT BẰNG DẤU CÁCH (Quan trọng nhất để giữ liền mạch câu)
        cleaned_text = local_soup.get_text(separator=' ')

        # 5. DỌN DẸP RÁC KHOẢNG TRẮNG VÀ XUỐNG DÒNG
        cleaned_text = re.sub(r'[ \t]+', ' ', cleaned_text)        # Gom nhiều dấu cách thành 1
        cleaned_text = re.sub(r' \n |\n | \n', '\n', cleaned_text) # Dọn dấu cách thừa dính với \n
        cleaned_text = re.sub(r'\n+', '\n', cleaned_text)          # Gom nhiều dòng trống thành 1 dòng
        cleaned_text = cleaned_text.strip()

        # 6. TRẢM TIÊU ĐỀ DÍNH CHẶT (Nếu .extract() sót)
        if cleaned_text.lower().startswith(original_title.lower()):
            cleaned_text = cleaned_text[len(original_title):].strip()
            cleaned_text = re.sub(r'^[:\-]+', '', cleaned_text).strip()

        # 7. CỨU HỘ MỤC QUYỀN LỢI (Bị nuốt chung vào thẻ khác)
        split_match = re.split(r'\n(?=Quyền lợi\s*\n)', cleaned_text, flags=re.IGNORECASE)
        if len(split_match) > 1:
            cleaned_text = split_match[0].strip() 
            benefits.append(split_match[1].replace("Quyền lợi", "").strip()) 

        sections[original_title] = cleaned_text

        # ===== mapping =====
        title_norm = normalize(original_title)

        if "yeu cau" in title_norm:
            requirements.append(cleaned_text)
        elif "quyen loi" in title_norm:
            benefits.append(cleaned_text)
        elif "mo ta" in title_norm:
            description.append(cleaned_text)
        elif "luong" in title_norm or "thu nhap" in title_norm:
            income.append(cleaned_text)
        elif "dia diem" in title_norm:
            city.append(cleaned_text)
        elif "thoi gian" in title_norm:
            schedule.append(cleaned_text)

    return sections, requirements, benefits, description, income, city, schedule

def extract_must(soup):
    """
    Trích xuất kỹ năng từ HTML TopCV dựa trên 3 cấu trúc layout cụ thể.
    """
    skills = []

    # KỊCH BẢN 1 (Theo Ảnh 1): Giao diện Premium Job
    # Đặc điểm: Nằm trong <div class="premium-job-related-tags__section--tags">
    # Thẻ chứa kỹ năng: <span class="tag-item"> hoặc <span class="tag-item expanded-tag">
    premium_container = soup.find('div', class_=re.compile(r'premium-job-related-tags__section--tags'))
    if premium_container:
        tags = premium_container.find_all('span', class_=re.compile(r'tag-item'))
        if tags:
            skills.extend([tag.get_text(strip=True) for tag in tags])
            return list(set(skills))

    # KỊCH BẢN 2 (Theo Ảnh 2): Giao diện Cột phải (Box Category)
    # Đặc điểm: Nằm trong <div class="box-category-tags">
    # Thẻ chứa kỹ năng: <span class="box-category-tag">
    box_category_tags = soup.find('div', class_='box-category-tags')
    if box_category_tags:
        tags = box_category_tags.find_all('span', class_=re.compile(r'box-category-tag'))
        if tags:
            skills.extend([tag.get_text(strip=True) for tag in tags])
            return list(set(skills))

    # KỊCH BẢN 3 (Theo Ảnh 3): Giao diện Box Skill cơ bản
    # Đặc điểm: Thẻ <h4>Kỹ năng cần có</h4>, bên dưới là <div class="item"> chứa các thẻ <span> trơn
    box_skill = soup.find('div', class_=re.compile(r'box-skill'))
    if box_skill:
        # Tìm div class="item" bên trong box-skill
        item_div = box_skill.find('div', class_='item')
        if item_div:
            # Lấy tất cả các thẻ span bên trong div.item
            tags = item_div.find_all('span')
            if tags:
                skills.extend([tag.get_text(strip=True) for tag in tags])
                return list(set(skills))

    # DỰ PHÒNG: Quét theo text "Kỹ năng cần có" nếu cấu trúc bị lệch đôi chút
    # Tìm thẻ h2, h4, hoặc div có chứa text "Kỹ năng cần có"
    titles = soup.find_all(['h2', 'h4', 'div'], string=re.compile(r'Kỹ năng cần có', re.IGNORECASE))
    for title in titles:
        # Lấy phần tử cha bao ngoài (thường là box-category hoặc tương tự)
        parent = title.parent
        if parent:
            # Tìm tất cả các thẻ span bên trong phần tử cha này
            spans = parent.find_all('span')
            for span in spans:
                text = span.get_text(strip=True)
                # Bỏ qua nếu text chính là tiêu đề
                if text and text.lower() != 'kỹ năng cần có':
                    skills.append(text)
            if skills:
                return list(set(skills))

    return []

def extract_should(soup):
    """
    Trích xuất kỹ năng từ HTML TopCV dựa trên 3 cấu trúc layout cụ thể.
    """
    skills = []

    # KỊCH BẢN 1 (Theo Ảnh 1): Giao diện Premium Job
    # Đặc điểm: Nằm trong <div class="premium-job-related-tags__section--tags">
    # Thẻ chứa kỹ năng: <span class="tag-item"> hoặc <span class="tag-item expanded-tag">
    premium_container = soup.find('div', class_=re.compile(r'premium-job-related-tags__section--tags'))
    if premium_container:
        tags = premium_container.find_all('span', class_=re.compile(r'tag-item'))
        if tags:
            skills.extend([tag.get_text(strip=True) for tag in tags])
            return list(set(skills))

    # KỊCH BẢN 2 (Theo Ảnh 2): Giao diện Cột phải (Box Category)
    # Đặc điểm: Nằm trong <div class="box-category-tags">
    # Thẻ chứa kỹ năng: <span class="box-category-tag">
    box_category_tags = soup.find('div', class_='box-category-tags')
    if box_category_tags:
        tags = box_category_tags.find_all('span', class_=re.compile(r'box-category-tag'))
        if tags:
            skills.extend([tag.get_text(strip=True) for tag in tags])
            return list(set(skills))

    # KỊCH BẢN 3 (Theo Ảnh 3): Giao diện Box Skill cơ bản
    # Đặc điểm: Thẻ <h4>Kỹ năng cần có</h4>, bên dưới là <div class="item"> chứa các thẻ <span> trơn
    box_skill = soup.find('div', class_=re.compile(r'box-skill'))
    if box_skill:
        # Tìm div class="item" bên trong box-skill
        item_div = box_skill.find('div', class_='item')
        if item_div:
            # Lấy tất cả các thẻ span bên trong div.item
            tags = item_div.find_all('span')
            if tags:
                skills.extend([tag.get_text(strip=True) for tag in tags])
                return list(set(skills))

    # DỰ PHÒNG: Quét theo text "Kỹ năng cần có" nếu cấu trúc bị lệch đôi chút
    # Tìm thẻ h2, h4, hoặc div có chứa text "Kỹ năng cần có"
    titles = soup.find_all(['h2', 'h4', 'div'], string=re.compile(r'Kỹ năng nên có', re.IGNORECASE))
    for title in titles:
        # Lấy phần tử cha bao ngoài (thường là box-category hoặc tương tự)
        parent = title.parent
        if parent:
            # Tìm tất cả các thẻ span bên trong phần tử cha này
            spans = parent.find_all('span')
            for span in spans:
                text = span.get_text(strip=True)
                # Bỏ qua nếu text chính là tiêu đề
                if text and text.lower() != 'kỹ năng cần có':
                    skills.append(text)
            if skills:
                return list(set(skills))

    return []
def extract_specializations(soup):
    for group in soup.select(".job-tags__group"):
        name = group.select_one(".job-tags__group-name")
        
        if name and "chuyên môn" in name.text.lower():
            items = group.select(".item.search-from-tag.link")
            
            result = [
                item.text.strip()
                for item in items
                if item.text.strip()
            ]
            
            return result if result else None

    return None

def extract_education(soup):
    def norm(s):
        return s.lower().strip()

    # =========================
    # 1. PREMIUM (xịn nhất)
    # =========================
    for item in soup.select(".general-information-data"):
        label = item.select_one(".general-information-data__label")
        value = item.select_one(".general-information-data__value")

        if label and "học vấn" in norm(label.get_text()):
            if value:
                return value.get_text(strip=True)

    # =========================
    # 2. BOX-GENERAL
    # =========================
    for item in soup.select(".box-general-group-info"):
        title = item.select_one(".box-general-group-info-title")
        value = item.select_one(".box-general-group-info-value")

        if title and "học vấn" in norm(title.get_text()):
            if value:
                return value.get_text(strip=True)

    # =========================
    # 3. BOX-INFO 
    # =========================
    box = soup.select_one(".box-info")
    if box:
        title = box.select_one(".title")
        if title and "thông tin" in norm(title.get_text()):
            for item in box.select(".box-item"):
                strong = item.find("strong")
                if strong and "học vấn" in norm(strong.get_text()):
                    span = item.find("span")
                    if span:
                        return span.get_text(strip=True)

    return None

def extract_custom_form_job(soup):
    result = {}

    items = soup.select(".custom-form-job__item")
    
    for item in items:
        title_el = item.select_one(".custom-form-job__item--title")
        content_el = item.select_one(".custom-form-job__item--content")
        
        if title_el:
            title = title_el.get_text(strip=True)
            content = content_el.get_text(strip=True) if content_el else ""

            if not content:  # bỏ luôn nếu rỗng
                continue

            if title in result:
                if isinstance(result[title], list):
                    result[title].append(content)
                else:
                    result[title] = [result[title], content]
            else:
                result[title] = content

    return result if result else None

def extract_company_details(soup):
    """
    Hàm gom các thông tin chi tiết của công ty thành một Dictionary.
    Nếu không tìm thấy, trả về None hoặc Dictionary rỗng.
    """
    company_info = {}
    
    # 1. Lấy Quy mô công ty
    scale_elem = soup.select_one(".company-scale .company-value")
    if scale_elem:
        company_info["scale"] = scale_elem.get_text(strip=True)
        
    # 2. Lấy Lĩnh vực hoạt động
    field_elem = soup.select_one(".company-field .company-value")
    if field_elem:
        company_info["field"] = field_elem.get_text(strip=True)
        
    # 3. Lấy Địa chỉ công ty
    address_elem = soup.select_one(".company-address .company-value")
    if address_elem:
        company_info["address"] = address_elem.get_text(strip=True)
        
    return company_info if company_info else None

def extract_flat_from_pagetext(page_text):
    """
    Hàm dùng Regex để bóc tách toàn bộ thông tin từ chuỗi pageText thô.
    Trả về các biến rời rạc (Tuple) thay vì Dictionary.
    """
    clean_text = re.sub(r'\s+', ' ', page_text)
    
    # Khởi tạo các biến rời
    salary = None
    location = None
    experience = None
    deadline = None
    description = []
    requirements = []
    benefits = []
    schedule = None


    # 3. Lấy Lương
    salary_match = re.search(r'Mức lương\s+(.*?)\s+Địa điểm', clean_text)
    if salary_match: salary = salary_match.group(1).strip()

    # 4. Lấy Địa điểm
    location_match = re.search(r'Địa điểm\s+(.*?)\s+Kinh nghiệm', clean_text)
    if location_match: location = location_match.group(1).strip()

    # 5. Lấy Kinh nghiệm
    exp_match = re.search(r'Kinh nghiệm\s+(.*?)\s+(?:Tra cứu|Xem mức)', clean_text)
    if exp_match: experience = exp_match.group(1).strip()

    # 6. Lấy Hạn nộp
    dl_match = re.search(r'Hạn nộp hồ sơ:\s+(\d{2}/\d{2}/\d{4})', clean_text)
    if dl_match: deadline = dl_match.group(1).strip()

    # 7. Lấy Mô tả công việc
    desc_match = re.search(r'Mô tả công việc\s+(.*?)\s+(?=Yêu cầu ứng viên|Trình độ, kinh nghiệm|Yêu cầu công việc)', clean_text, re.IGNORECASE)
    if desc_match: description = [desc_match.group(1).strip()]

    # 8. Lấy Yêu cầu
    req_match = re.search(r'(?:Yêu cầu ứng viên|Yêu cầu công việc)\s+(.*?)\s+(?=Quyền lợi được hưởng|Quyền lợi:)', clean_text, re.IGNORECASE)
    if req_match: requirements = [req_match.group(1).strip()]

    # 9. Lấy Quyền lợi
    ben_match = re.search(r'(?:Quyền lợi được hưởng|Quyền lợi:)\s+(.*?)\s+(?=Quyền lợi Bảo hiểm|Địa điểm làm việc|Thời gian làm việc)', clean_text, re.IGNORECASE)
    if ben_match and len(ben_match.group(1)) > 50:
        benefits = [ben_match.group(1).strip()]

    # 10. Lấy Thời gian làm việc
    schedule_match = re.search(r'Thời gian làm việc\s+(.*?)\s+(?:Cách thức ứng tuyển|Thời gian check-in|Bạn có hài lòng)', clean_text, re.IGNORECASE)
    if schedule_match:
        raw_sched = schedule_match.group(1).strip()
        schedule = re.sub(r'^Thời gian làm việc:\s*', '', raw_sched)

    # TRẢ VỀ TUPLE CHỨA CÁC BIẾN RỜI
    return salary, location, experience, deadline, description, requirements, benefits, schedule
# ===========================================================
# 5. MAIN PARSER MODULE (Dịch từ HTML -> Schema)
# ===========================================================
def parse_job_html(html, url):
    try:
        soup = BeautifulSoup(html, "html.parser")
        json_ld = extract_json_ld(soup)
        meta_tags = extract_meta(soup)

        # ===== PRIORITY: JSON-LD =====
        title = json_ld.get("title")

        company = json_ld.get("hiringOrganization", {}).get("name")

        employment_type = json_ld.get("employmentType")

        #posting_date = json_ld.get("datePosted")

        level = json_ld.get("occupationalCategory")

        openings = json_ld.get("totalJobOpenings")

        schedule = extract_schedule(soup)

        skills_needed = extract_must(soup)
        skills_should_have = extract_should(soup)
        if skills_needed is None and skills_should_have is None:
            skills_should_have = json_ld.get("skills")
            skills_needed = json_ld.get("skills")

        specialty = extract_specializations(soup)
        if not specialty:
            specialty = json_ld.get("industry")

        education = extract_education(soup)
        
        # thời gian crawl dữ liệu (ingest_ts)
        now = datetime.now(UTC)
        ingest_ts = int(now.timestamp() * 1000)
        
        # thời gian đăng job, fallback cho event_ts nếu không lấy được updated time
        vn_tz = timezone(timedelta(hours=7))
        date_str = json_ld.get("datePosted")

        if date_str:
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=vn_tz)
                posting_ts = int(dt.timestamp() * 1000)
            except ValueError:
                posting_ts = None
        else:
            posting_ts = None
        
        # thời gian hết hạn đăng ký 
        deadline = extract_dl(soup)
        valid_through = None # Mặc định là None nếu không tìm thấy
        if deadline:
            try:
                vt = datetime.strptime(deadline, "%d/%m/%Y").replace(hour=23, minute=59, second=59, tzinfo=vn_tz)
                valid_through = int(vt.timestamp() * 1000)
            except ValueError:
                pass # Bỏ qua nếu regex/text không đúng định dạng ngày
        if not valid_through:
            valid_through_raw = json_ld.get("validThrough")
            if isinstance(valid_through_raw, str):
                try:
                    valid_through = int(
                        datetime.fromisoformat(valid_through_raw).timestamp() * 1000
                    )
                except Exception as e:
                    logger.warning(
                        f"[!] Parse validThrough lỗi: {valid_through_raw} | {e}"
                    )
        # experience
        exp = json_ld.get("experienceRequirements", {}).get("monthsOfExperience")

        # ===== FALLBACK HTML =====
        if not exp:
            exp = extract_exp(soup)
        if not exp:
            exp = "Không yêu cầu"
        if not company:
            company = extract_company_html(soup)

        #if not salary_raw:
        salary_raw = extract_salary_html(soup)
        if not level:
            level = extract_level_html(soup)
        extra_inf = extract_custom_form_job(soup)
        company_details = extract_company_details(soup)
        benefits = extract_benefits(soup)
        income = extract_income(soup)
        requirements_raw = extract_job_requirements(soup)
        description = extract_description(soup)
        city = extract_location_html(soup)
        sections, requirements_raw_1, benefits_1, description_1, income_1, city_1, schedule_1 = extract_sections(soup)
        if not city:
            city = city_1
        if not benefits:
            benefits = benefits_1
        if not income:
            income = income_1
        if not requirements_raw:
            requirements_raw = requirements_raw_1
        if not description:  
            description = description_1
        if not schedule:
            schedule = schedule_1


        # Lấy text toàn trang
        page_text = soup.get_text(" ", strip=True)
        
        # Hứng toàn bộ các biến rời rạc
        ( 
            regex_salary, 
            regex_location, 
            regex_exp, 
            regex_deadline, 
            regex_desc, 
            regex_req, 
            regex_ben, 
            regex_schedule
        ) = extract_flat_from_pagetext(page_text)

        if not salary_raw:
            salary_raw = regex_salary
        if not description:
            description = regex_desc
        if not schedule:
            schedule = regex_schedule
        # =========================
        # KEYS
        # =========================
        normalized_url = normalize_url(url)
        if not title or not company:
            # Trong thực tế, bạn có thể ghi URL này ra file error_links.log để kiểm tra sau
            print(f"Skipping bad URL or blocked by Captcha: {url}")
            return None
        job_id = sha256_hash(f"{SOURCE}|{normalized_url}")

        req_str = ", ".join(requirements_raw) if requirements_raw else ""
        city_str = ", ".join(city) if isinstance(city, list) else str(city or "")

        hash_content = sha256_hash(
            normalize_text(title) + "|" +
            normalize_text(company) + "|" +
            normalize_text(city_str) + "|" +
            normalize_text(salary_raw) + "|" +
            normalize_text(req_str) + "|" +
            normalize_text(employment_type)
        )

        # =========================
        # RAW RECORD
        # =========================
        record = {
            "source": SOURCE,
            "source_url": url,
            "normalized_source_url": normalized_url,
            "crawl_version": 1,
            "ingest_ts": ingest_ts,
            "event_ts": posting_ts,
            "job_id": job_id,
            "hash_content": hash_content,

            "payload": {
                "title": title,
                "company_name": company,
                "company_details": company_details,
                "salary": salary_raw,
                "location": city,
                "monthOfExperience": exp,
                "deadline": valid_through,
                "occupationalCategory": level,
                "education": education,
                "employmentType": employment_type,
                "openings": openings,
                "description": description,
                "requirements": requirements_raw,
                "income": income, 
                "benefits": benefits,
                "extra_inf": extra_inf,
                "schedule": schedule,
                "skillsNeeded": skills_needed,
                "skillsShouldHave": skills_should_have,
                "specialty": specialty,
                "meta_tags": meta_tags,
                "json_ld": json_ld,
                "sectionsByHeading": sections,
                "pageText": soup.get_text(" ", strip=True)
            },

            # ===== RAW QUALITY FLAGS =====
            "quality_flags": {
                "has_json_ld": bool(json_ld),
                "has_page_text": bool(soup.get_text(strip=True)),
                "has_structured_company_name_conflict": bool(
                    company and extract_company_html(soup) and company != extract_company_html(soup)
                ),
                
                "has_valid_posting_date": posting_ts is not None,
                "has_valid_deadline": valid_through is not None,
                
                "has_salary_info": bool(salary_raw or income),
                "has_location_info": bool(city),
                "has_experience_info": bool(exp),
                
                "has_requirements": bool(requirements_raw),
                "has_description": bool(description),
                "has_benefits": bool(benefits),
                
                "has_skills_info": bool(skills_needed or skills_should_have),
                "has_education_info": bool(education),
                "has_specialty": bool(specialty),
                "has_schedule": bool(schedule),
                "has_employment_type": bool(employment_type),
                "has_income": bool(income),
                "has_extra_info": bool(extra_inf)
            }
        }

        return record
    except Exception as e:
        logger.error(f"Lỗi khi Parse HTML tại {url}: {e}")
        return None

# =======================================================
# 6. MASTER ORCHESTRATOR (CURL_CFFI HYBRID + CACHING)
# =======================================================
def get_total_pages(session, fallback_pages=1):
    """
    Lấy tổng số page list TopCV.

    Bản merge sửa lỗi quan trọng:
    - Không return 1 ngay khi cookie cache bị 403/401.
    - Nếu bị chặn, recover cookie ngay trong bước lấy total pages.
    - Trả về cả session vì recover_blocked_request() có thể tạo session mới.
    - Nếu vẫn không parse được total pages thì fallback về fallback_pages.
      Với speed mode, fallback_pages nên là max_pages, ví dụ 15.
    """
    url = (
        "https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257"
        "?sort=up_top&type_keyword=1&category_family=r257&saturday_status=0"
    )

    try:
        fallback_pages = int(fallback_pages or 1)
    except Exception:
        fallback_pages = 1
    fallback_pages = max(1, fallback_pages)

    logger.info(f"[*] Đang lấy tổng số trang từ: {url}")

    res = None
    try:
        check_and_trigger_global_cooldown()
        res = session.get(url, timeout=15)
    except Exception as e:
        logger.warning(f"[TOTAL PAGES] Lỗi request tổng số trang: {e}")

    if res is None or is_blocked_response(res):
        status = getattr(res, "status_code", None)
        logger.warning(
            f"[TOTAL PAGES] Bị chặn khi lấy tổng số trang "
            f"(status={status}). Thử lấy cookie mới trước khi quyết định end_page."
        )
        session, res = recover_blocked_request(
            session=session,
            url=url,
            context="TOTAL PAGES",
        )

    if res is None or is_blocked_response(res):
        status = getattr(res, "status_code", None)

        if fallback_pages is None:
            raise RuntimeError(
                "[TOTAL PAGES] Không lấy được total pages cho full batch. "
                "Dừng để tránh crawl thiếu."
            )

        logger.error(
            f"[TOTAL PAGES] Không lấy được tổng số trang sau khi recover "
            f"(status={status}). Fallback về {fallback_pages} trang."
        )
        return session, fallback_pages

    try:
        soup = BeautifulSoup(res.text, "html.parser")
        element = soup.select_one("#job-listing-paginate-text")

        if not element:
            logger.warning(
                f"[TOTAL PAGES] Không tìm thấy pagination element. "
                f"Fallback về {fallback_pages} trang."
            )
            return session, fallback_pages

        text = element.get_text(" ", strip=True)
        match = re.search(r"/\s*(\d+)", text)

        if not match:
            logger.warning(
                f"[TOTAL PAGES] Không parse được total pages từ text: {text}. "
                f"Fallback về {fallback_pages} trang."
            )
            return session, fallback_pages

        total_pages = int(match.group(1))
        logger.info(f"[OK] Tổng số trang: {total_pages}")
        return session, total_pages

    except Exception as e:
        logger.error(
            f"[TOTAL PAGES] Lỗi parse tổng số trang: {e}. "
            f"Fallback về {fallback_pages} trang."
        )
        return session, fallback_pages
def run_master_crawler(
    max_pages=5,
    list_pages_per_chunk=2,
    detail_batch_size=30,
    updated_within_minutes=None,
    threshold_time=None,
    use_checkpoint=None,
    use_processed_cache=False,
    processed_ttl_days=SPEED_PROCESSED_TTL_DAYS,
    start_page=1,
    batch_checkpoint=None,
):
    """
    Crawler chính.

    Cơ chế mới:
    - max_pages: giới hạn cứng số trang list tối đa trong 1 lần chạy.
    - updated_within_minutes / threshold_time: chỉ lấy job có updated_time >= threshold_time.
    - early stop: nếu một page không còn job nào nằm trong ngưỡng thời gian -> dừng quét list page sau.
    - use_checkpoint: mặc định True khi crawl full, False khi crawl theo ngưỡng thời gian.
    - use_processed_cache:
        + True cho speed mode để tránh bắn/crawl trùng job_id đã xử lý trong TTL.
        + False cho batch mode để vẫn crawl lại job nằm trong threshold, kể cả job cũ được cập nhật.
    - start_page:
        + batch resume truyền page bắt đầu từ batch_checkpoint.
        + batch mới/speed luôn nên là 1.
    - batch_checkpoint:
        + chỉ dùng cho batch resume/new batch để lưu tiến độ nếu server lỗi hoặc bị block.
    """
    logger.info("=== KHỞI ĐỘNG HỆ THỐNG CRAWLER HYBRID (CURL_CFFI) ===")

    if threshold_time is None and updated_within_minutes is not None:
        threshold_time = datetime.now() - timedelta(minutes=updated_within_minutes)

    if use_checkpoint is None:
        # Crawl theo thời gian luôn nên bắt đầu lại từ page 1,
        # vì job mới nhất luôn nằm ở đầu danh sách.
        use_checkpoint = threshold_time is None

    if threshold_time is not None:
        logger.info(f"[TIME FILTER] Chỉ lấy job cập nhật từ: {threshold_time.strftime('%Y-%m-%d %H:%M:%S')} trở đi")
        logger.info("[TIME FILTER] Tắt checkpoint tự động để không bỏ lỡ job mới ở page 1.")

    cookie_dict, user_agent = load_cookie_cache()

    if cookie_dict:
        logger.info("[CACHE] Đã tìm thấy vé VIP trong ổ cứng. Không cần mở Chrome!")
    else:
        logger.info("[CACHE] Không có vé VIP trong ổ cứng. Bắt đầu gọi Trạm cấp vé...")
        cookie_dict, user_agent = execute_harvester_with_breaker()
        if cookie_dict:
            save_cookie_cache(cookie_dict, user_agent)
        else:
            logger.error("Hủy khởi động Crawler do xin vé thất bại.")
            return

    session = create_topcv_session(cookie_dict, user_agent)

    if batch_checkpoint is not None:
        # Batch checkpoint chỉ dùng để resume một phiên batch lỗi trong TTL.
        # Không dùng checkpoint cũ kiểu page number cho lịch chạy định kỳ.
        start_page = int(start_page or batch_checkpoint.get("next_page", 1) or 1)
    else:
        start_page = get_last_checkpoint() + 1 if use_checkpoint else int(start_page or 1)

    start_page = max(1, start_page)

    links_queue = []
    seen_urls_in_run = set()

    # Chỉ SPEED mode dùng cache này.
    # BATCH mode phải để use_processed_cache=False để không bỏ sót job cũ được cập nhật.
    speed_processed_jobs = {}
    if use_processed_cache:
        speed_processed_jobs = load_speed_processed_jobs(ttl_days=processed_ttl_days)
        logger.info(
            f"[SPEED CACHE] Loaded {len(speed_processed_jobs)} processed job_ids "
            f"with TTL={processed_ttl_days} days"
        )
    else:
        logger.info("[SPEED CACHE] Disabled for this run")

    stats = {
        "total_farmed_links": 0,
        "new_links_to_crawl": 0,
        "successfully_saved": 0,
        "missing_data_jobs": 0,
        "failed_requests": 0,
        "pages_scanned": 0,
        "fresh_links_in_window": 0,
        "old_links_skipped_by_time": 0,
        "early_stop_pages": 0,
        "skipped_by_speed_cache": 0,
        "no_url_cards": 0,
        "no_time_cards": 0,
    }

    if max_pages is None or max_pages <= 0:
        total_pages_fallback = None
    else:
        total_pages_fallback = max_pages

    session, site_total_pages = get_total_pages(
        session,
        fallback_pages=total_pages_fallback,
    )

    # Nếu max_pages = None hoặc <= 0 thì crawl đến tổng số page thật của website.
    # Phù hợp cho batch mode.
    if max_pages is None or max_pages <= 0:
        end_page = site_total_pages
        max_pages_display = "AUTO_SITE_TOTAL"
    else:
        end_page = min(site_total_pages, start_page + max_pages - 1)
        max_pages_display = max_pages

    current_page = start_page
    stop_farming_list_pages = False

    logger.info(
        f"[CONFIG] start_page={start_page} | "
        f"end_page={end_page} | "
        f"max_pages={max_pages_display} | "
        f"site_total_pages={site_total_pages}"
    )

    # VÒNG LẶP ĐAN XEN (Vừa Farm List, Vừa Cào Detail)
    while (current_page <= end_page and not stop_farming_list_pages) or len(links_queue) > 0:

        # =====================================================
        # PHA 1: QUÉT LIST PAGE (Theo Lô / Chunk)
        # =====================================================
        if current_page <= end_page and not stop_farming_list_pages:
            chunk_start = current_page
            chunk_end = min(current_page + list_pages_per_chunk - 1, end_page)
            logger.info(f"\n=== [PHA 1] QUÉT GOM LINK TỪ TRANG {chunk_start} ĐẾN {chunk_end} ===")

            chunk_farmed = 0
            chunk_new = 0
            chunk_fresh = 0
            chunk_old = 0

            last_scanned_page_in_chunk = chunk_start - 1
            page_queue = [
                {"page": p_num, "retry_count": 0}
                for p_num in range(chunk_start, chunk_end + 1)
            ]

            while page_queue:
                page_meta = page_queue.pop(0)
                p_num = int(page_meta["page"])
                last_scanned_page_in_chunk = max(last_scanned_page_in_chunk, p_num)

                list_url = f"https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?sort=up_top&type_keyword=1&page={p_num}&category_family=r257&saturday_status=0"
                logger.info(f"  -> Đang quét trang danh sách {p_num}...")

                try:
                    check_and_trigger_global_cooldown()
                    res = session.get(list_url, timeout=15)
                except Exception as net_err:
                    requeue_list_page_or_fail(
                        page_queue=page_queue,
                        page_meta=page_meta,
                        list_url=list_url,
                        reason=f"list page network error: {net_err}",
                        stats=stats,
                    )
                    continue

                if res is None:
                    requeue_list_page_or_fail(
                        page_queue=page_queue,
                        page_meta=page_meta,
                        list_url=list_url,
                        reason="list page response is None",
                        stats=stats,
                    )
                    continue

                if is_blocked_response(res):
                    session, recovered_res = recover_blocked_request(
                        session=session,
                        url=list_url,
                        context=f"LIST PAGE {p_num}",
                    )

                    if recovered_res is not None and not is_blocked_response(recovered_res):
                        res = recovered_res
                    else:
                        status = getattr(recovered_res, "status_code", None)
                        reason = "list page blocked after cookie recovery"
                        if status:
                            reason = f"HTTP {status} - {reason}"

                        requeue_list_page_or_fail(
                            page_queue=page_queue,
                            page_meta=page_meta,
                            list_url=list_url,
                            reason=reason,
                            stats=stats,
                        )
                        continue

                try:
                    soup = BeautifulSoup(res.text, 'html.parser')

                    export_debug_card_links(soup, p_num)

                    fresh_jobs, page_summary = extract_fresh_jobs_from_listing_page(
                        soup=soup,
                        threshold_time=threshold_time,
                    )

                    logger.info(
                        f"     Page {p_num}: cards={page_summary['total_job_cards']} | "
                        f"parse_time={page_summary['parseable_time_count']} | "
                        f"fresh={page_summary['fresh_count']} | "
                        f"old/skip={page_summary['old_count']} | "
                        f"no_url={page_summary.get('no_url_count', 0)} | "
                        f"no_time={page_summary.get('no_time_count', 0)}"
                    )

                    if page_summary["total_job_cards"] == 0:
                        debug_file = f"debug_page_{p_num}.html"
                        with open(debug_file, "w", encoding="utf-8") as f:
                            f.write(res.text)

                        requeue_list_page_or_fail(
                            page_queue=page_queue,
                            page_meta=page_meta,
                            list_url=list_url,
                            reason=f"list page has 0 job cards; saved debug html to {debug_file}",
                            stats=stats,
                        )
                        continue

                    stats["pages_scanned"] += 1
                    stats["total_farmed_links"] += page_summary["total_job_cards"]
                    stats["fresh_links_in_window"] += page_summary["fresh_count"]
                    stats["old_links_skipped_by_time"] += page_summary["old_count"]
                    stats["no_url_cards"] += page_summary.get("no_url_count", 0)
                    stats["no_time_cards"] += page_summary.get("no_time_count", 0)

                    chunk_farmed += page_summary["total_job_cards"]
                    chunk_fresh += page_summary["fresh_count"]
                    chunk_old += page_summary["old_count"]

                    for job_meta in fresh_jobs:
                        href = job_meta["url"]

                        if href in seen_urls_in_run:
                            continue

                        job_id = build_job_id_from_url(href)

                        # Chỉ speed mode mới skip job_id đã xử lý trong TTL.
                        # Batch mode không dùng cache để có thể crawl lại job cũ được cập nhật.
                        if use_processed_cache and job_id in speed_processed_jobs:
                            stats["skipped_by_speed_cache"] += 1
                            continue

                        seen_urls_in_run.add(href)
                        job_meta["job_id"] = job_id
                        links_queue.append(job_meta)

                        stats["new_links_to_crawl"] += 1
                        chunk_new += 1

                    if use_checkpoint:
                        save_checkpoint(p_num)

                    if page_summary["should_stop_after_page"]:
                        stats["early_stop_pages"] += 1
                        logger.info(
                            f"[EARLY STOP] Page {p_num} không còn job nào mới hơn threshold. "
                            "Dừng quét các page sau, nhưng vẫn xử lý queue hiện tại."
                        )
                        stop_farming_list_pages = True
                        break

                    time.sleep(random.uniform(1.5, 3.0))

                except Exception as e:
                    requeue_list_page_or_fail(
                        page_queue=page_queue,
                        page_meta=page_meta,
                        list_url=list_url,
                        reason=f"list page DOM parse error: {e}",
                        stats=stats,
                    )

            logger.info(
                f"[+] HOÀN TẤT PHA 1 (Lô {chunk_start}-{last_scanned_page_in_chunk}): "
                f"cards={chunk_farmed}, fresh={chunk_fresh}, old_skip={chunk_old}, link_mới={chunk_new}."
            )
            logger.info(f"[*] Tổng đạn trong Kho chờ (Queue) hiện tại: {len(links_queue)} links.")

            current_page = last_scanned_page_in_chunk + 1 if stop_farming_list_pages else chunk_end + 1

        # =====================================================
        # PHA 2: BÓC TÁCH CHI TIẾT (Theo Lô Gối Đầu)
        # =====================================================
        if links_queue:
            current_detail_batch = [links_queue.pop(0) for _ in range(min(detail_batch_size, len(links_queue)))]
            logger.info(f"\n=== [PHA 2] BÓC TÁCH MẺ GỐI ĐẦU ({len(current_detail_batch)} LINKS) ===")
            logger.info(f"[*] Hàng đợi còn tồn đọng chờ mẻ sau: {len(links_queue)} links.")

            for idx, job_meta in enumerate(current_detail_batch):
                link = job_meta["url"]

                try:
                    logger.info(f"  -> Đang lấy [{idx+1}/{len(current_detail_batch)}]: {link}")
                    if job_meta.get("updated_text"):
                        logger.info(f"     Listing updated: {job_meta.get('updated_text')}")

                    try:
                        check_and_trigger_global_cooldown()
                        detail_res = session.get(link, timeout=15)
                    except Exception as net_err:
                        requeue_detail_or_fail(
                            links_queue=links_queue,
                            job_meta=job_meta,
                            link=link,
                            reason=f"detail network error: {net_err}",
                            stats=stats,
                        )
                        continue

                    if detail_res is None:
                        requeue_detail_or_fail(
                            links_queue=links_queue,
                            job_meta=job_meta,
                            link=link,
                            reason="detail response is None",
                            stats=stats,
                        )
                        continue

                    if is_blocked_response(detail_res):
                        session, recovered_res = recover_blocked_request(
                            session=session,
                            url=link,
                            context="DETAIL",
                        )

                        if recovered_res is not None and not is_blocked_response(recovered_res):
                            detail_res = recovered_res
                        else:
                            status = getattr(recovered_res, "status_code", None)
                            reason = "detail blocked after cookie recovery"
                            if status:
                                reason = f"HTTP {status} - {reason}"

                            requeue_detail_or_fail(
                                links_queue=links_queue,
                                job_meta=job_meta,
                                link=link,
                                reason=reason,
                                stats=stats,
                            )
                            continue

                    if detail_res.status_code == 200 and not looks_blocked_or_empty(detail_res.text):
                        record = parse_job_html(detail_res.text, link)
                        if record:
                            # Gắn thời gian update lấy từ list page vào record detail.
                            updated_time = job_meta.get("updated_time")
                            updated_ts = int(updated_time.timestamp() * 1000) if isinstance(updated_time, datetime) else None
                            if updated_ts is not None:
                                record["event_ts"] = updated_ts
                            record["listing_updated_text"] = job_meta.get("updated_text")
                            record["listing_updated_ts"] = updated_ts
                            record["listing_crawl_time"] = job_meta.get("crawl_time").isoformat() if isinstance(job_meta.get("crawl_time"), datetime) else None

                            record["payload"]["listing_updated_text"] = job_meta.get("updated_text")
                            record["payload"]["listing_updated_at"] = updated_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(updated_time, datetime) else None
                            record["quality_flags"]["has_listing_updated_time"] = updated_ts is not None

                            if log_missing_fields(record, link):
                                stats["missing_data_jobs"] += 1
                                logger.warning("      [!] Job này thiếu data quan trọng. Đã lưu log!")

                            if save_jsonl(record):
                                stats["successfully_saved"] += 1

                                # Chỉ speed mode mới mark cache sau khi ghi/emit thành công.
                                # Batch mode không mark để lần batch sau vẫn có thể crawl lại theo threshold.
                                if use_processed_cache:
                                    mark_speed_processed_job(speed_processed_jobs, record.get("job_id"))
                                    save_speed_processed_jobs(speed_processed_jobs)
                            else:
                                log_failed_link(link, "save_jsonl returned False")
                                stats["failed_requests"] += 1
                        else:
                            requeue_detail_or_fail(
                                links_queue=links_queue,
                                job_meta=job_meta,
                                link=link,
                                reason="parse_job_html returned None",
                                stats=stats,
                            )
                    else:
                        reason = f"HTTP {detail_res.status_code}"
                        requeue_detail_or_fail(
                            links_queue=links_queue,
                            job_meta=job_meta,
                            link=link,
                            reason=reason,
                            stats=stats,
                        )

                except Exception as e:
                    requeue_detail_or_fail(
                        links_queue=links_queue,
                        job_meta=job_meta,
                        link=link,
                        reason=f"detail system error: {e}",
                        stats=stats,
                    )

                time.sleep(random.uniform(1.5, 3.5))

            # Chỉ cập nhật checkpoint khi queue đã được xử lý hết.
            # Nếu còn queue mà server chết, lần resume sẽ crawl lại một phần, nhưng không mất dữ liệu.
            if batch_checkpoint is not None and len(links_queue) == 0:
                batch_checkpoint["next_page"] = current_page
                save_batch_checkpoint(batch_checkpoint)

    if batch_checkpoint is not None:
        mark_batch_checkpoint_completed(batch_checkpoint)

    # ---------------------------------------------------------
    # BÁO CÁO THỐNG KÊ
    # ---------------------------------------------------------
    logger.info("\n=======================================================")
    logger.info("BÁO CÁO THỐNG KÊ PHIÊN CRAWL ĐAN XEN + TIME FILTER")
    logger.info("=======================================================")
    logger.info(f"- Tổng số request đã thực thi         : {global_request_count}")
    logger.info(f"- Số page list đã quét                : {stats['pages_scanned']}")
    logger.info(f"- Tổng số job card ở Pha 1            : {stats['total_farmed_links']}")
    logger.info(f"- Số job nằm trong ngưỡng thời gian   : {stats['fresh_links_in_window']}")
    logger.info(f"- Số job bị bỏ qua vì cũ              : {stats['old_links_skipped_by_time']}")
    logger.info(f"- Số link MỚI cần cào ở Pha 2         : {stats['new_links_to_crawl']}")
    logger.info(f"- Số Job bóc tách THÀNH CÔNG          : {stats['successfully_saved']}")
    logger.info(f"- Số Job bị THIẾU DATA (cần check log): {stats['missing_data_jobs']}")
    logger.info(f"- Số card không lấy được URL          : {stats.get('no_url_cards', 0)}")
    logger.info(f"- Số card không parse được thời gian  : {stats.get('no_time_cards', 0)}")
    logger.info(f"- Số request bị LỖI / BLOCK           : {stats['failed_requests']}")
    logger.info(f"- Số lần dừng sớm theo thời gian      : {stats['early_stop_pages']}")
    logger.info(f"- Số job bị bỏ qua bởi speed cache    : {stats['skipped_by_speed_cache']}")
    logger.info(f"- Số job lưu thành công trong phiên này: {stats['successfully_saved']}")
    if use_processed_cache:
        logger.info(f"- Speed processed cache hiện có       : {len(speed_processed_jobs)} job_ids")
    logger.info("=======================================================\n")

if __name__ == "__main__":
    logger.info(
        "Không nên chạy trực tiếp file này trong production. "
        "Hãy dùng: python -m apps.ingestion.run_crawler --mode speed "
        "hoặc: python -m apps.ingestion.run_crawler --mode batch"
    )
