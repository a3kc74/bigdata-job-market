import cloudscraper
from bs4 import BeautifulSoup
import json
import hashlib
import re
import time
import random
import unicodedata
from urllib.parse import urlparse, urlunparse
from datetime import datetime, UTC, timezone, timedelta

# ===========================================================
# 1. CRAWL LINKS
# ===========================================================
BASE_URL = "https://www.topcv.vn"
def get_job_links(scraper, page=1):
    url = f"https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?category_family=r257&page={page}"
    print(f"[*] Đang quét trang danh sách: {url}")
    
    try:
        res = scraper.get(url, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
    except Exception as e:
        print(f"[!] Lỗi khi lấy danh sách trang {page}: {e}")
        return []

    job_links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Lọc: Chỉ lấy những link trỏ tới chi tiết việc làm
        if "/viec-lam/" in href:
            # Bổ sung domain nếu link dạng rút gọn
            if href.startswith("/"):
                href = BASE_URL + href
            # Cắt bỏ đuôi theo dõi tracking (?ta_source=...) để link sạch 100%
            href = href.split("?")[0] 
            job_links.add(href)
            
    return list(job_links)

# ===========================================================
# 2. CRAWL DETAIL
# ===========================================================
SOURCE = "topcv"
# =========================
# FETCH
# =========================
"""
def get_soup(url):
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    res = scraper.get(url)

    if res.status_code != 200:
        print("Lỗi request:", res.status_code)
        return None

    return BeautifulSoup(res.text, "html.parser")
"""
def get_soup(scraper, url):
    """
    Hàm fetch dùng chung phiên (session) và bắt lỗi mạng an toàn.
    """
    try:
        res = scraper.get(url, timeout=15)
        if res.status_code != 200:
            print(f"[!] HTTP {res.status_code} tại URL: {url}")
            return None
        return BeautifulSoup(res.text, "html.parser")
    except Exception as e:
        print(f"[!] Lỗi kết nối {url}: {e}")
        return None
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
    # Tránh trường hợp "thời gian làm việc thỏa thuận"
    if re.search(r'(mức lương|thu nhập|lương)[\s:]*(thỏa thuận|thoả thuận|cạnh tranh)', raw_text):
        return "Thỏa thuận"
        
    # KHIÊN BẢO VỆ 2: Bắt số tiền (Bắt buộc phải đứng sau chữ Lương/Thu nhập để tránh phụ cấp ăn trưa 40.000 VNĐ)
    context_match = re.search(r'(?:lương|thu nhập)[\s:]*?((?:lên tới\s*)?\d+[\.,]?\d*\s*(?:-\s*\d+[\.,]?\d*\s*)?(triệu|tr|vnđ|usd))', raw_text)
    if context_match:
        return context_match.group(1).title()

    # KHIÊN BẢO VỆ 3: Nếu bí quá, quét tìm số "Triệu" hoặc "USD" trơ trọi. 
    # Tuyệt đối không quét chữ "VNĐ" trơ trọi để tránh tiền gửi xe, tiền ăn trưa nhảm.
    safe_match = re.search(r'((?:lên tới\s*)?\d+[\.,]?\d*\s*(?:-\s*\d+[\.,]?\d*\s*)?(triệu|tr|usd))', raw_text)
    if safe_match:
        return safe_match.group(1).title()

    return "Thỏa thuận / Không hiển thị"
"""
def extract_salary_html(soup):
    sec = soup.select_one("div.section-salary")
    if not sec:
        return None
    val = sec.find("div", class_="job-detail__info--section-content-value")
    return val.get_text(strip=True) if val else None
"""
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

def extract_job_requirements(soup):
    # Dùng CSS Selector để tìm TẤT CẢ các thẻ <li> nằm bên trong khối có class "requirement"
    li_tags = soup.select(".requirement li")
    
    # Dùng List Comprehension để rút chữ ra và bỏ qua các thẻ rỗng
    return [li.get_text(strip=True) for li in li_tags if li.get_text(strip=True)]

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
def extract_description(soup):
    container = soup.find(
        "div",
        class_=lambda c: c and (
            "job-description__item--content" in c
            or "premium-job-description__box--content" in c
            or "content-tab" in c
        )
    )

    if not container:
        return None

    # ===== CASE 1: có list =====
    li_tags = container.find_all("li")
    if li_tags:
        results = []
        for li in li_tags:
            text = li.get_text(" ", strip=True)
            if text:
                results.append(text)
        return results if results else None

    # ===== CASE 2: không có list → lấy paragraph/text =====
    texts = []

    for tag in container.find_all(["p", "div", "span"]):
        text = tag.get_text(" ", strip=True)
        if text:
            texts.append(text)

    # fallback nếu không có p/div
    if not texts:
        text = container.get_text(" ", strip=True)
        return text if text else None

    return "\n".join(texts)
def extract_sections(soup):
    sections = {}
    for sec in soup.select(".job-description__item"):
        title = sec.find("h3")
        content = sec.get_text(" ", strip=True)
        if title:
            sections[title.get_text(strip=True)] = content
    return sections

def extract_benefits(html_content):
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

def extract_must(soup):
    for box in soup.find_all("div", class_="box-category"):
        title = box.find("div", class_="box-title")

        if title and "kỹ năng cần có" in title.text.lower():
            skills = [
                tag.text.strip()
                for tag in box.find_all("span", class_="box-category-tag")
                if tag.text.strip()
            ]
            return skills if skills else None
    return None

def extract_should(soup):
    for box in soup.find_all("div", class_="box-category"):
        title = box.find("div", class_="box-title")

        if title and "kỹ năng nên có" in title.text.lower():
            skills = [
                tag.text.strip()
                for tag in box.find_all("span", class_="box-category-tag")
                if tag.text.strip()
            ]
            return skills if skills else None

    return None

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
    for group in soup.select(".box-general-group"):
        title = group.select_one(".box-general-group-info-title")
        
        if title and "học vấn" in title.text.lower():
            value = group.select_one(".box-general-group-info-value")
            
            if value:
                text = value.text.strip()
                return text if text else None

    return None

def normalize(text):
    text = text.lower().strip()
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    return text

def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip()

def extract_to_variables(soup):
    # init biến
    requirements = []
    benefits = []
    description = []
    income = []

    for sec in soup.select(".job-description__item"):
        title_tag = sec.find("h3")
        if not title_tag:
            continue

        title = normalize(title_tag.get_text(strip=True))

        content = sec.get_text(" ", strip=True)
        content = content.replace(title_tag.get_text(strip=True), "")
        content = clean_text(content)

        if not content:
            continue

        # mapping trực tiếp
        if any(k in title for k in ["yeu cau ung vien", "requirement"]):
            requirements.append(content)

        elif any(k in title for k in ["quyen loi", "benefit"]):
            benefits.append(content)

        elif any(k in title for k in ["mo ta cong viec", "responsibility", "description"]):
            description.append(content)

        elif any(k in title for k in ["thu nhap", "salary", "luong"]):
            income.append(content)

    return requirements, benefits, description, income

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
# =========================
# MAIN PARSER
# =========================
def parse_job(scraper, url):

    soup = get_soup(scraper, url)
    if not soup:
        return None

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
    
    # thời gian post job (event_ts)
    vn_tz = timezone(timedelta(hours=7))
    date_str = json_ld.get("datePosted")
    if date_str:
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=vn_tz)
            event_ts = int(dt.timestamp() * 1000)
        except ValueError:
            event_ts = None # Lỗi format thì null
    else:
        event_ts = None # Không có ngày đăng thì null
    
    # thời gian hết hạn đăng ký 
    deadline = extract_dl(soup)
    valid_through = None # Mặc định là None nếu không tìm thấy
    if deadline:
        try:
            vt = datetime.strptime(deadline, "%d/%m/%Y").replace(hour=23, minute=59, second=59, tzinfo=vn_tz)
            valid_through = int(vt.timestamp() * 1000)
        except ValueError:
            pass # Bỏ qua nếu regex/text không đúng định dạng ngày


    # location
    """
    addr = json_ld.get("jobLocation", {}).get("address", {})
    city = addr.get("addressRegion")
    """
    # experience
    exp = json_ld.get("experienceRequirements", {}).get("monthsOfExperience")

    # ===== FALLBACK HTML =====
    if not exp:
        exp = extract_exp(soup)
    if not company:
        company = extract_company_html(soup)

    #if not salary_raw:
    salary_raw = extract_salary_html(soup)

    #if not city:
    city = extract_location_html(soup)

    if not level:
        level = extract_level_html(soup)

    requirements_raw, benefits, description, income = extract_to_variables(soup)
    
    if not benefits:
        benefits = extract_benefits(json_ld.get("jobBenefits"))
    if not income:
        income = extract_income(soup)
    if not requirements_raw:
        requirements_raw = extract_job_requirements(soup)
    if not description:  
        description = extract_description(soup)
    extra_inf = extract_custom_form_job(soup)
    company_details = extract_company_details(soup)


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

    # thời gian hết hạn đăng ký 
    deadline = extract_dl(soup)
    if not deadline:
        deadline = regex_deadline
    valid_through = None # Mặc định là None nếu không tìm thấy
    if deadline:
        try:
            vt = datetime.strptime(deadline, "%d/%m/%Y").replace(hour=23, minute=59, second=59, tzinfo=vn_tz)
            valid_through = int(vt.timestamp() * 1000)
        except ValueError:
            pass # Bỏ qua nếu regex/text không đúng định dạng ngày
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
        "event_ts": event_ts,
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
            "sectionsByHeading": extract_sections(soup),
            "pageText": soup.get_text(" ", strip=True)
        },

        # ===== RAW QUALITY FLAGS =====
        "quality_flags": {
            "has_json_ld": bool(json_ld),
            "has_page_text": bool(soup.get_text(strip=True)),
            "has_structured_company_name_conflict": bool(
                company and extract_company_html(soup) and company != extract_company_html(soup)
            ),
            
            "has_valid_posting_date": event_ts is not None,
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
    url = "https://www.topcv.vn/viec-lam/qa-tester-game-web-thu-nhap-up-to-30m-nhan-viec-ngay/2125006.html?ta_source=JobSearchList_LinkDetail&u_sr_id=QFf6VTYpflndZ6fgB5yIWy6588anjONYspO4092O_1777343898"
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    rec = parse_job(scraper, url)

    if rec:
        save_jsonl(rec)
        save_json(rec)

# =======================================================
# PHẦN 4: HỆ THỐNG ĐIỀU PHỐI (THE BATCH MANAGER)
# =======================================================
def save_jsonl(record, file_path="raw_jobs.jsonl"):
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def run_batch_crawler(start_page=1, end_page=3):
    """
    Luồng chạy chính của chế độ Batch.
    """
    # 1. Khởi tạo 1 Scraper duy nhất cho toàn bộ quá trình
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    
    total_jobs_saved = 0
    
    # 2. Vòng lặp cào qua các trang danh sách
    for page in range(start_page, end_page + 1):
        links = get_job_links(scraper, page)
        print(f"[-] Tìm thấy {len(links)} links ở trang {page}. Bắt đầu bóc tách...")
        
        # 3. Vòng lặp cào chi tiết từng bài đăng
        for idx, link in enumerate(links):
            print(f"   + Cào chi tiết {idx+1}/{len(links)}: {link}")
            
            record = parse_job(scraper, link)
            
            if record:
                save_jsonl(record, "raw_jobs_batch.jsonl")
                total_jobs_saved += 1
            
            # NGỦ ĐỂ VƯỢT RÀO: Nghỉ 1-3 giây giữa mỗi link chi tiết
            time.sleep(random.uniform(1.5, 3.5))
            
        # Nghỉ dài hơn một chút khi chuyển sang trang danh sách tiếp theo
        print(f"[zZz] Xong trang {page}, nghỉ ngơi 5 giây...")
        time.sleep(5)
        
    print(f"\n[OK] Batch Crawler hoàn tất! Tổng cộng đã lưu: {total_jobs_saved} jobs.")

if __name__ == "__main__":
    main()
    #run_batch_crawler(1, 2)