import json
import re
import time
from datetime import datetime
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


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


def fetch_html_requests(url: str) -> tuple[str, str]:
    session = requests.Session()
    session.headers.update(HEADERS)

    # Một số site phản hồi tốt hơn nếu có referer/search engine
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
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=HEADERS["User-Agent"],
            locale="vi-VN",
            viewport={"width": 1440, "height": 900},
        )
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(4000)
        html = page.content()
        browser.close()
        return html, "playwright"


def fetch_html(url: str) -> tuple[str, str]:
    try:
        html, method = fetch_html_requests(url)
        if looks_blocked_or_empty(html):
            html, method = fetch_html_playwright(url)
        return html, method
    except Exception:
        html, method = fetch_html_playwright(url)
        return html, method


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


if __name__ == "__main__":
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