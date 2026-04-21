import cloudscraper
from bs4 import BeautifulSoup
import json
import hashlib
import re
from urllib.parse import urlparse, urlunparse
from datetime import datetime, UTC


SOURCE = "topcv"


# =========================
# FETCH
# =========================
def get_soup(url):
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    res = scraper.get(url)

    if res.status_code != 200:
        print("Lỗi request:", res.status_code)
        return None

    return BeautifulSoup(res.text, "html.parser")


# =========================
# NORMALIZE
# =========================
def normalize_url(url):
    p = urlparse(url)
    return urlunparse((p.scheme.lower(), p.netloc.lower(), p.path, "", "", ""))


def normalize_text(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip().lower())


def sha256_hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# =========================
# JSON-LD
# =========================
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
    return text_or_none(soup.select_one(".salary, .job-detail__salary"))


def extract_level_html(soup):
    return text_or_none(soup.select_one(".job-level"))

def extract_schedule(soup):
    h3_tag = soup.find(lambda tag: tag.name == "h3" and "Thời gian làm việc" in tag.text)

    if h3_tag:
        # 2. Từ thẻ h3, đi ngược lên thẻ cha bao ngoài cùng của khối này
        parent_item = h3_tag.find_parent("div", class_="job-description__item")
        
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

                # ❗ bỏ div cha (tránh gộp text)
                if item.find('div'):
                    continue

                text = item.get_text(" ", strip=True)

                if text and "địa điểm khác" not in text.lower():
                    locations.append(text)

            return list(dict.fromkeys(locations))  # dedup

    return []



def extract_job_requirements(soup):
    # Dùng CSS Selector để tìm TẤT CẢ các thẻ <li> nằm bên trong khối có class "requirement"
    li_tags = soup.select(".requirement li")
    
    # Dùng List Comprehension để rút chữ ra và bỏ qua các thẻ rỗng
    return [li.get_text(strip=True) for li in li_tags if li.get_text(strip=True)]

def extract_sections(soup):
    sections = {}
    for sec in soup.select(".job-description__item"):
        title = sec.find("h3")
        content = sec.get_text(" ", strip=True)
        if title:
            sections[title.get_text(strip=True)] = content
    return sections

def extract_ul_list(html_content):
    # 1. Parse đoạn HTML (nếu bạn đang truyền vào một chuỗi string)
    # Lưu ý: Nếu bạn đã có sẵn đối tượng soup của thẻ <ul> này từ bước trước,
    # bạn có thể bỏ qua dòng BeautifulSoup này.
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 2. Tìm tất cả các thẻ <li>
    li_tags = soup.find_all('li')
    
    # 3. Duyệt qua từng thẻ <li>, lấy chữ và làm sạch khoảng trắng
    result_items = []
    for li in li_tags:
        text = li.get_text(strip=True)
        if text: # Chỉ lấy nếu có nội dung, tránh thẻ <li> rỗng
            result_items.append(text)
            
    return result_items

# =========================
# MAIN PARSER
# =========================
def parse_job(url):

    soup = get_soup(url)
    if not soup:
        return None

    json_ld = extract_json_ld(soup)
    meta_tags = extract_meta(soup)

    # ===== PRIORITY: JSON-LD =====
    title = json_ld.get("title")

    company = json_ld.get("hiringOrganization", {}).get("name")

    employment_type = json_ld.get("employmentType")

    posting_date = json_ld.get("datePosted")

    valid_through = json_ld.get("validThrough")

    level = json_ld.get("occupationalCategory")

    openings = json_ld.get("totalJobOpenings")

    schedule = extract_schedule(soup)
    benefits = extract_ul_list(json_ld.get("jobBenefits"))

    # salary
    salary_raw = None
    salary_obj = json_ld.get("baseSalary", {})
    if salary_obj:
        salary_raw = str(salary_obj)

    # location
    #addr = json_ld.get("jobLocation", {}).get("address", {})
    #city = addr.get("addressRegion")

    # experience
    exp = json_ld.get("experienceRequirements", {}).get("monthsOfExperience")

    # skills
    skills = json_ld.get("skills")

    # ===== FALLBACK HTML =====
    if not exp:
        exp = extract_exp(soup)
    if not company:
        company = extract_company_html(soup)

    if not salary_raw:
        salary_raw = extract_salary_html(soup)

    #if not city:
    city = extract_location_html(soup)

    if not level:
        level = extract_level_html(soup)

    requirements_raw = extract_job_requirements(soup)

    # =========================
    # KEYS
    # =========================
    normalized_url = normalize_url(url)

    job_id = sha256_hash(f"{SOURCE}|{normalized_url}")

    hash_content = sha256_hash(
        normalize_text(title) + "|" +
        normalize_text(company) + "|" +
        #normalize_text(city) + "|" +
        normalize_text(salary_raw) + "|" 
        #normalize_text(requirements_raw)
    )

    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    # =========================
    # RAW RECORD
    # =========================
    record = {
        "source": SOURCE,
        "source_url": url,
        "normalized_source_url": normalized_url,
        "crawl_version": 1,
        "ingest_ts": now,
        "event_ts": now,

        "job_id": job_id,
        "hash_content": hash_content,

        "payload": {
            "domain": "www.topcv.vn",
            "fetch_method": "cloudscraper",

            "title": title,
            "company_name": company,
            "salary": salary_raw,
            "location": city,
            "experience": exp,
            "deadline": valid_through,

            "description": soup.get_text(" ", strip=True),
            "requirements": requirements_raw,
            "benefits": benefits,
            "schedule": schedule,
            "skills": skills,
            "categories": json_ld.get("industry"),
            "meta_tags": meta_tags,
            "json_ld": json_ld,
            "sections_by_heading": extract_sections(soup),
            "page_text": soup.get_text(" ", strip=True)
        },

        # ===== RAW QUALITY FLAGS =====
        "quality_flags": {
            "has_json_ld": bool(json_ld),
            "has_page_text": bool(soup.get_text(strip=True)),
            "has_structured_salary": bool(salary_obj),
            "has_structured_company_name_conflict":
                bool(company and extract_company_html(soup) and company != extract_company_html(soup)),
            "has_valid_posting_date": bool(posting_date),
            "has_valid_deadline": bool(valid_through)
        }
    }
    ex = extract_exp(soup)
    if not ex:
        print("null")
    else: print(ex)
    return record


# =========================
# SAVE JSONL
# =========================
def save_jsonl(record, file="raw_jobs.jsonl"):
    with open(file, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def save_json(data, file="data1.json"):
    with open(file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# =========================
# RUN
# =========================
def main():
    url = "https://www.topcv.vn/viec-lam/giao-vien-tieng-anh-giao-tiep-online/2068487.html?ta_source=JobSearchList_LinkDetail&u_sr_id=T3cprHopOa1qJyGx2FWqb6lrbg5klo5bNV0gEvDA_1776741199"

    rec = parse_job(url)

    if rec:
        save_jsonl(rec)
        save_json(rec)



if __name__ == "__main__":
    main()