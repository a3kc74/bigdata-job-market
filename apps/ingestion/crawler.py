"""
Job posting crawler with streaming support.

Supports two modes:
  1. Single URL:    parse_job_posting(url) — crawl a single job page
  2. Streaming:     StreamingCrawler — continuously crawl job listings from
                    TopCV search pages and yield individual job data as a
                    stream, with configurable throughput.

Streaming throughput is controlled via:
  - delay_between_jobs:  seconds to wait between yielding each job (default: 2)
  - delay_between_pages: seconds to wait between fetching listing pages (default: 5)
  - max_pages:           max number of listing pages to scrape per cycle (0=unlimited)
  - loop:                whether to restart from page 1 after exhausting all pages
"""

import hashlib
import json
import logging
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Generator, Optional
from urllib.parse import urlencode, urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SOURCE_NAME = "topcv"

# ── HTTP headers ────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}


# ── Text helpers ────────────────────────────────────────────────────────────

def normalize_url(url: str) -> str:
    url = url.strip()
    if not url:
        raise ValueError("URL khong duoc de trong")

    parsed = urlparse(url)
    if not parsed.scheme:
        url = "https://" + url
    return url


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"\r", "\n", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def get_text_by_selectors(soup: BeautifulSoup, selectors: list[str]) -> str:
    for selector in selectors:
        try:
            node = soup.select_one(selector)
            if node:
                text = clean_text(node.get_text(" ", strip=True))
                if text:
                    return text
        except Exception:
            continue
    return ""


def get_all_text_from_section(section) -> str:
    if not section:
        return ""
    return clean_text(section.get_text("\n", strip=True))


def get_list_from_section(section) -> list[str]:
    if not section:
        return []
    items = []
    for li in section.select("li"):
        text = clean_text(li.get_text(" ", strip=True))
        if text:
            items.append(text)
    return items


def extract_meta_tags(soup: BeautifulSoup) -> dict:
    meta_data = {}
    for meta in soup.find_all("meta"):
        key = meta.get("name") or meta.get("property") or meta.get("itemprop")
        value = meta.get("content")
        if key and value:
            meta_data[key] = clean_text(value)
    return meta_data


def extract_jsonld(soup: BeautifulSoup) -> list:
    jsonld_blocks = []
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
            jsonld_blocks.append(parsed)
        except Exception:
            jsonld_blocks.append(raw)
    return jsonld_blocks


def extract_sections_by_headings(soup: BeautifulSoup) -> dict:
    result = {}
    headings = soup.find_all(["h1", "h2", "h3", "h4"])
    for h in headings:
        heading_text = clean_text(h.get_text(" ", strip=True))
        if not heading_text:
            continue

        collected = []
        current = h.find_next_sibling()
        while current:
            if getattr(current, "name", None) in ["h1", "h2", "h3", "h4"]:
                break
            text = clean_text(current.get_text(" ", strip=True))
            if text:
                collected.append(text)
            current = current.find_next_sibling()

        if collected:
            result[heading_text] = "\n".join(collected)
    return result


def looks_blocked_or_empty(html: str) -> bool:
    if not html or len(html) < 1000:
        return True

    lowered = html.lower()
    blocked_signals = [
        "access denied",
        "forbidden",
        "captcha",
        "cf-browser-verification",
        "attention required",
        "robot",
    ]
    if any(signal in lowered for signal in blocked_signals):
        return True

    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True).lower() if soup.title else ""
    if any(signal in title for signal in ["forbidden", "access denied", "captcha"]):
        return True

    body_text = clean_text(soup.get_text("\n", strip=True))
    if len(body_text) < 300:
        return True

    return False


# ── Stealth Playwright helper ───────────────────────────────────────────────

def _create_stealth_browser(pw):
    """
    Create a Playwright browser + context with stealth patches applied.
    Uses headed mode when DISPLAY is available (bypasses Cloudflare headless detection).
    Falls back to headless if no display is available.
    Returns (browser, context).
    """
    import os
    try:
        from playwright_stealth import stealth_sync
    except ImportError:
        stealth_sync = None

    # On Windows (nt), we always have a display context for Playwright.
    # On Linux, we check for DISPLAY or WAYLAND_DISPLAY.
    has_display = os.name == "nt" or bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))
    use_headless = not has_display

    # headless_env = os.environ.get("CRAWLER_HEADLESS", "true").strip().lower()
    # use_headless = headless_env not in ("0", "false", "no")

    browser = pw.chromium.launch(
        headless=use_headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-infobars",
            "--disable-extensions",
        ],
    )

    context = browser.new_context(
        user_agent=HEADERS["User-Agent"],
        locale="vi-VN",
        viewport={"width": 1440, "height": 900},
        extra_http_headers={
            "Accept-Language": HEADERS["Accept-Language"],
            "Referer": "https://www.google.com/",
            "Upgrade-Insecure-Requests": "1",
        },
    )

    return browser, context, stealth_sync


def _new_stealth_page(context, stealth_sync_fn):
    """Create a new page in the context with stealth patches applied."""
    page = context.new_page()
    if stealth_sync_fn:
        stealth_sync_fn(page)
    return page


# ── HTML fetching ───────────────────────────────────────────────────────────

def fetch_html_requests(url: str) -> tuple[str, str]:
    session = requests.Session()
    session.headers.update(HEADERS)

    extra_headers = {
        "Referer": "https://www.google.com/",
    }

    last_error = None
    for _ in range(2):
        try:
            response = session.get(url, headers=extra_headers, timeout=30)
            if response.status_code == 403:
                raise requests.HTTPError("403 Forbidden", response=response)
            response.raise_for_status()
            return response.text, "requests"
        except Exception as e:
            last_error = e
            time.sleep(1)

    raise last_error


def fetch_html_playwright(url: str) -> tuple[str, str]:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser, context, stealth_sync = _create_stealth_browser(p)

        try:
            page = _new_stealth_page(context, stealth_sync)

            # Visit homepage first to establish session/cookies
            page.goto("https://www.topcv.vn/", wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2000)
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(4000)

            html = page.content()
            return html, "playwright"
        finally:
            browser.close()


def fetch_html(url: str) -> tuple[str, str]:
    try:
        html, method = fetch_html_requests(url)
        if looks_blocked_or_empty(html):
            logger.info("HTML from requests looks blocked/empty, retrying with Playwright: %s", url)
            html, method = fetch_html_playwright(url)
        return html, method
    except Exception as exc:
        logger.info("requests failed for %s (%s), retrying with Playwright", url, exc)
        html, method = fetch_html_playwright(url)
        return html, method


# ── Single-page parsing ────────────────────────────────────────────────────

def parse_from_html(url: str, html: str, fetch_method: str) -> tuple[dict, str]:
    soup = BeautifulSoup(html, "html.parser")

    data = {
        "source_url": url,
        "domain": urlparse(url).netloc,
        "crawled_at": datetime.utcnow().isoformat(),
        "fetch_method": fetch_method,
    }

    data["title"] = get_text_by_selectors(soup, [
        "h1",
        ".job-title",
        ".title",
        ".job-detail__info h1",
        ".job-header-info h1",
        "[class*='job-title']",
        "[class*='title']",
    ])

    data["company_name"] = get_text_by_selectors(soup, [
        ".company-name",
        ".company-title",
        ".employer-name",
        ".job-company-name",
        "[class*='company-name']",
        "[class*='company'] a",
    ])

    data["salary"] = get_text_by_selectors(soup, [
        ".salary",
        ".job-salary",
        ".offer-salary",
        "[class*='salary']",
        "[class*='wage']",
    ])

    data["location"] = get_text_by_selectors(soup, [
        ".location",
        ".job-location",
        ".address",
        "[class*='location']",
        "[class*='address']",
    ])

    data["level"] = get_text_by_selectors(soup, [
        ".level",
        ".job-level",
        "[class*='level']",
        "[class*='position']",
        "[class*='rank']",
    ])

    data["experience"] = get_text_by_selectors(soup, [
        ".experience",
        ".job-experience",
        "[class*='experience']",
        "[class*='exp']",
    ])

    data["deadline"] = get_text_by_selectors(soup, [
        ".deadline",
        ".expired-date",
        ".job-deadline",
        "[class*='deadline']",
        "[class*='expire']",
    ])

    data["job_type"] = get_text_by_selectors(soup, [
        ".job-type",
        "[class*='job-type']",
        "[class*='working-form']",
        "[class*='type']",
    ])

    data["quantity"] = get_text_by_selectors(soup, [
        ".quantity",
        "[class*='quantity']",
        "[class*='number']",
    ])

    description_section = None
    requirement_section = None
    benefit_section = None

    description_candidates = [
        ".job-description",
        ".description",
        ".job-detail__information-detail",
        "[class*='description']",
        "[id*='description']",
    ]
    requirement_candidates = [
        ".job-requirement",
        ".requirements",
        "[class*='requirement']",
        "[id*='requirement']",
    ]
    benefit_candidates = [
        ".job-benefit",
        ".benefits",
        "[class*='benefit']",
        "[id*='benefit']",
    ]

    for selector in description_candidates:
        description_section = soup.select_one(selector)
        if description_section:
            break

    for selector in requirement_candidates:
        requirement_section = soup.select_one(selector)
        if requirement_section:
            break

    for selector in benefit_candidates:
        benefit_section = soup.select_one(selector)
        if benefit_section:
            break

    data["description"] = get_all_text_from_section(description_section)
    data["requirements"] = get_all_text_from_section(requirement_section)
    data["benefits"] = get_all_text_from_section(benefit_section)

    data["description_items"] = get_list_from_section(description_section)
    data["requirement_items"] = get_list_from_section(requirement_section)
    data["benefit_items"] = get_list_from_section(benefit_section)

    skills = []
    for node in soup.select(
        ".skill, .skills span, .job-tags a, .tag, [class*='skill'], [class*='tag']"
    ):
        text = clean_text(node.get_text(" ", strip=True))
        if text and len(text) < 50:
            skills.append(text)
    data["skills"] = sorted(set(skills))

    categories = []
    for node in soup.select(".breadcrumb a, [class*='breadcrumb'] a"):
        text = clean_text(node.get_text(" ", strip=True))
        if text:
            categories.append(text)
    data["categories"] = categories

    data["meta_tags"] = extract_meta_tags(soup)
    data["json_ld"] = extract_jsonld(soup)
    data["sections_by_heading"] = extract_sections_by_headings(soup)

    raw_text = soup.get_text("\n", strip=True)
    raw_text = clean_text(raw_text)
    data["page_text"] = raw_text

    return data, raw_text


def parse_job_posting(url: str) -> tuple[dict, str]:
    url = normalize_url(url)
    html, fetch_method = fetch_html(url)
    return parse_from_html(url, html, fetch_method)


# ── Listing page helpers (TopCV search) ─────────────────────────────────────

def _build_search_url(keyword: str = "", location: str = "", page: int = 1) -> str:
    params = {"page": page}
    if keyword:
        params["keyword"] = keyword
    if location:
        params["city"] = location

    base = "https://www.topcv.vn/tim-viec-lam-it-phan-mem-c10026"
    return f"{base}?{urlencode(params)}"


def _normalize_job_url(raw_url: str) -> str:
    raw_url = (raw_url or "").strip()
    if not raw_url:
        return ""

    if raw_url.startswith("//"):
        raw_url = "https:" + raw_url
    elif raw_url.startswith("/"):
        raw_url = "https://www.topcv.vn" + raw_url

    raw_url = raw_url.split("#", 1)[0]

    if "/viec-lam/" not in raw_url:
        return ""

    return raw_url


def fetch_listing_jobs(page: int = 1, keyword: str = "", location: str = "") -> tuple[list[dict], list[dict]]:
    """
    Crawl 1 listing page bằng Playwright, parse trực tiếp từ DOM đã render.
    Returns:
        valid_jobs: list record nhẹ từ listing
        error_jobs: list lỗi để debug
    """
    from playwright.sync_api import sync_playwright

    url = _build_search_url(keyword=keyword, location=location, page=page)

    valid_jobs: list[dict] = []
    error_jobs: list[dict] = []
    seen: set[str] = set()
    current_time = int(time.time())

    with sync_playwright() as p:
        browser, context, stealth_sync = _create_stealth_browser(p)

        page_obj = _new_stealth_page(context, stealth_sync)

        try:
            # Visit homepage first to establish cookies/session
            page_obj.goto("https://www.topcv.vn/", wait_until="domcontentloaded", timeout=60000)
            page_obj.wait_for_timeout(2000)

            # Dismiss any popups (e.g. "Tìm việc an toàn cùng TopCV")
            try:
                close_btn = page_obj.locator("button.close, .modal .close, [data-dismiss='modal'], .popup-close, .btn-close")
                if close_btn.count() > 0:
                    close_btn.first.click(timeout=3000)
                    page_obj.wait_for_timeout(500)
                    logger.debug("Dismissed a popup on homepage")
            except Exception:
                pass

            # Navigate to listing page
            page_obj.goto(url, wait_until="domcontentloaded", timeout=60000)
            page_obj.wait_for_timeout(3000)

            # Dismiss popups again on listing page
            try:
                close_btn = page_obj.locator("button.close, .modal .close, [data-dismiss='modal'], .popup-close, .btn-close")
                if close_btn.count() > 0:
                    close_btn.first.click(timeout=3000)
                    page_obj.wait_for_timeout(500)
                    logger.debug("Dismissed a popup on listing page")
            except Exception:
                pass

            # Wait for job cards to appear (the real selector from TopCV DOM)
            try:
                page_obj.wait_for_selector(".job-item-search-result", timeout=10000)
                logger.debug("Job cards loaded via .job-item-search-result")
            except Exception:
                logger.debug("Timed out waiting for .job-item-search-result, trying scroll fallback")

            # Scroll to trigger lazy loading
            for _ in range(3):
                page_obj.mouse.wheel(0, 2500)
                page_obj.wait_for_timeout(1200)

            # Extract job data directly using page.evaluate() on the DOM
            # This is more reliable than locator().evaluate_all()
            raw_jobs = page_obj.evaluate("""
                () => {
                    const cards = document.querySelectorAll('.job-item-search-result');
                    return Array.from(cards).map(card => {
                        // Title + link: h3.title > a  or  a[href*='/viec-lam/']
                        const titleLink = card.querySelector('h3.title a')
                            || card.querySelector('.title a')
                            || card.querySelector('a[href*="/viec-lam/"]');

                        // Company: a.company
                        const companyEl = card.querySelector('a.company')
                            || card.querySelector('.company')
                            || card.querySelector('[class*="company"]');

                        // Salary: label.salary
                        const salaryEl = card.querySelector('label.salary')
                            || card.querySelector('.salary')
                            || card.querySelector('[class*="salary"]');

                        // Location: label.address
                        const addressEl = card.querySelector('label.address')
                            || card.querySelector('.address')
                            || card.querySelector('[class*="address"]');

                        return {
                            title: titleLink ? (titleLink.innerText || '').trim() : '',
                            href: titleLink ? (titleLink.getAttribute('href') || '') : '',
                            company: companyEl ? (companyEl.innerText || '').trim() : '',
                            salary: salaryEl ? (salaryEl.innerText || '').trim() : '',
                            location: addressEl ? (addressEl.innerText || '').trim() : '',
                            data_job_id: card.getAttribute('data-job-id') || '',
                        };
                    });
                }
            """)

            logger.info("Found %d job cards on listing page %d", len(raw_jobs), page)

            # Fallback: if no .job-item-search-result found, try broader selectors
            if not raw_jobs:
                raw_jobs = page_obj.evaluate("""
                    () => {
                        const anchors = document.querySelectorAll('a[href*="/viec-lam/"]');
                        const seen = new Set();
                        const results = [];
                        for (const a of anchors) {
                            const href = (a.getAttribute('href') || '').trim();
                            const text = (a.innerText || '').trim();
                            if (href && text && text.length > 5 && !seen.has(href)) {
                                seen.add(href);
                                results.push({
                                    title: text,
                                    href: href,
                                    company: '',
                                    salary: '',
                                    location: '',
                                    data_job_id: '',
                                });
                            }
                        }
                        return results;
                    }
                """)
                logger.info("Fallback: found %d anchor-based jobs on page %d", len(raw_jobs), page)

            if not raw_jobs:
                body_preview = page_obj.evaluate("() => document.body.innerText.substring(0, 2000)")
                error_jobs.append({
                    "page": page,
                    "error": "Khong tim thay job card trong DOM",
                    "body_preview": body_preview,
                })
                return valid_jobs, error_jobs

            # Parse the extracted data into records
            for item in raw_jobs:
                try:
                    link = _normalize_job_url(item.get("href", ""))
                    title = clean_text(item.get("title", ""))

                    if not link or not title:
                        continue

                    if link in seen:
                        continue

                    seen.add(link)
                    job_id_raw = item.get("data_job_id") or link.rstrip("/").split("/")[-1].split(".")[0]

                    record = {
                        "job_id": f"{SOURCE_NAME}_{job_id_raw}",
                        "company_name": clean_text(item.get("company", "")) or "N/A",
                        "job_title": title,
                        "location": clean_text(item.get("location", "")) or "N/A",
                        "salary_text": clean_text(item.get("salary", "")),
                        "salary_min": 0,
                        "salary_max": 0,
                        "currency": "VND",
                        "source_url": link,
                        "ingest_ts": current_time,
                        "event_ts": current_time - random.randint(3600, 86400),
                        "source": SOURCE_NAME,
                    }

                    hash_string = f"{record['job_title']}_{record['company_name']}_{record['location']}"
                    record["hash_content"] = hashlib.md5(hash_string.encode("utf-8")).hexdigest()
                    valid_jobs.append(record)

                except Exception as exc:
                    error_jobs.append({
                        "page": page,
                        "error": str(exc),
                        "raw": str(item)[:300],
                    })

        finally:
            browser.close()

    logger.info("Listing page %d: valid_jobs=%d, error_jobs=%d", page, len(valid_jobs), len(error_jobs))
    return valid_jobs, error_jobs


# ── Streaming Crawler ───────────────────────────────────────────────────────

@dataclass
class StreamingCrawlerConfig:
    keyword: str = ""
    location: str = ""
    delay_between_jobs: float = 2.0
    delay_between_pages: float = 5.0
    max_pages: int = 0
    loop: bool = True
    max_events: int = 0


class StreamingCrawler:
    def __init__(self, config: Optional[StreamingCrawlerConfig] = None):
        self.config = config or StreamingCrawlerConfig()
        self._seen_urls: set[str] = set()
        self._total_yielded: int = 0

    @property
    def total_yielded(self) -> int:
        return self._total_yielded

    def stream(self) -> Generator[dict, None, None]:
        cfg = self.config
        cycle = 0

        while True:
            cycle += 1
            page = 0
            has_more = True

            logger.info(
                "Streaming cycle %d started (keyword=%r, location=%r)",
                cycle, cfg.keyword, cfg.location,
            )

            while has_more:
                page += 1

                if cfg.max_pages > 0 and page > cfg.max_pages:
                    logger.info("Reached max_pages=%d, ending cycle %d", cfg.max_pages, cycle)
                    break

                listing_url = _build_search_url(cfg.keyword, cfg.location, page)
                logger.info("Fetching listing page %d: %s", page, listing_url)

                try:
                    jobs_on_page, error_jobs = fetch_listing_jobs(
                        page=page,
                        keyword=cfg.keyword,
                        location=cfg.location,
                    )
                except Exception as exc:
                    logger.warning("Failed to fetch listing page %d: %s", page, exc)
                    break

                logger.info(
                    "Parsed listing page %d: valid_jobs=%d, error_jobs=%d",
                    page,
                    len(jobs_on_page),
                    len(error_jobs),
                )

                if error_jobs:
                    logger.debug("Sample listing errors: %s", error_jobs[:3])

                if not jobs_on_page:
                    logger.info("No jobs found on page %d, ending cycle", page)
                    has_more = False
                    break

                for listing_job in jobs_on_page:
                    link = listing_job["source_url"]

                    if link in self._seen_urls:
                        logger.debug("Skipping already-seen URL: %s", link)
                        continue

                    try:
                        job_data, _ = parse_job_posting(link)

                        if not job_data.get("title"):
                            job_data["title"] = listing_job.get("job_title", "")
                        if not job_data.get("company_name"):
                            job_data["company_name"] = listing_job.get("company_name", "")
                        if not job_data.get("location"):
                            job_data["location"] = listing_job.get("location", "")

                        job_data["job_id"] = listing_job.get("job_id")
                        job_data["ingest_ts"] = listing_job.get("ingest_ts")
                        job_data["event_ts"] = listing_job.get("event_ts")
                        job_data["source"] = listing_job.get("source")
                        job_data["hash_content"] = listing_job.get("hash_content")

                        self._seen_urls.add(link)
                        self._total_yielded += 1

                        logger.info(
                            "[%d] Crawled: %s — %s",
                            self._total_yielded,
                            job_data.get("title", "?"),
                            job_data.get("company_name", "?"),
                        )

                        yield job_data

                        if cfg.max_events > 0 and self._total_yielded >= cfg.max_events:
                            logger.info("Reached max_events=%d, stopping.", cfg.max_events)
                            return

                        if cfg.delay_between_jobs > 0:
                            time.sleep(cfg.delay_between_jobs)

                    except Exception as exc:
                        logger.warning("Failed to crawl %s: %s", link, exc)
                        continue

                if cfg.delay_between_pages > 0:
                    time.sleep(cfg.delay_between_pages)

            if not cfg.loop:
                logger.info("Loop disabled, stopping after cycle %d", cycle)
                break

            logger.info(
                "Cycle %d complete. Total yielded: %d. Restarting...",
                cycle, self._total_yielded,
            )
            self._seen_urls.clear()


# ── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="Job Posting Crawler")
    sub = parser.add_subparsers(dest="mode", help="Crawl mode")

    single = sub.add_parser("single", help="Crawl a single job posting URL")
    single.add_argument("url", help="Job posting URL")

    stream = sub.add_parser("stream", help="Stream job postings continuously")
    stream.add_argument("--keyword", default="", help="Search keyword (default: all jobs)")
    stream.add_argument("--location", default="", help="City filter")
    stream.add_argument(
        "--delay-jobs",
        type=float,
        default=2.0,
        help="Seconds between jobs (throughput control, default: 2.0)",
    )
    stream.add_argument(
        "--delay-pages",
        type=float,
        default=5.0,
        help="Seconds between listing pages (default: 5.0)",
    )
    stream.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Max listing pages per cycle (0=unlimited, default: 0)",
    )
    stream.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after N events (0=unlimited, default: 0)",
    )
    stream.add_argument(
        "--no-loop",
        action="store_true",
        help="Don't restart after exhausting pages",
    )

    args = parser.parse_args()

    if args.mode == "single":
        data, raw_text = parse_job_posting(args.url)

        json_file = "job_posting.json"
        raw_file = "job_posting_raw.txt"

        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        with open(raw_file, "w", encoding="utf-8") as f:
            f.write(raw_text)

        print(f"Da luu JSON vao: {json_file}")
        print(f"Da luu raw text vao: {raw_file}")
        print(f"URL da crawl: {data['source_url']}")
        print(f"Fetch method: {data['fetch_method']}")

    elif args.mode == "stream":
        config = StreamingCrawlerConfig(
            keyword=args.keyword,
            location=args.location,
            delay_between_jobs=args.delay_jobs,
            delay_between_pages=args.delay_pages,
            max_pages=args.max_pages,
            max_events=args.max_events,
            loop=not args.no_loop,
        )
        crawler = StreamingCrawler(config)

        print(
            f"Starting streaming crawler (delay={config.delay_between_jobs}s, "
            f"max_pages={config.max_pages}, loop={config.loop})"
        )
        print("Press Ctrl+C to stop.\n")

        try:
            for job_data in crawler.stream():
                print(json.dumps({
                    "title": job_data.get("title", ""),
                    "company_name": job_data.get("company_name", ""),
                    "source_url": job_data.get("source_url", ""),
                    "salary": job_data.get("salary", ""),
                    "location": job_data.get("location", ""),
                    "skills": job_data.get("skills", []),
                }, ensure_ascii=False))
        except KeyboardInterrupt:
            print(f"\nStopped. Total events: {crawler.total_yielded}")

    else:
        url = input("Nhap link can crawl: ").strip()
        data, raw_text = parse_job_posting(url)

        json_file = "job_posting.json"
        raw_file = "job_posting_raw.txt"

        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        with open(raw_file, "w", encoding="utf-8") as f:
            f.write(raw_text)

        print(f"Da luu JSON vao: {json_file}")
        print(f"Da luu raw text vao: {raw_file}")
        print(f"URL da crawl: {data['source_url']}")
        print(f"Fetch method: {data['fetch_method']}")