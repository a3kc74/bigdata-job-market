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
import logging
import sys

# Cấu hình Logger định dạng chuẩn cho Docker
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | CRAWLER | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout) # Ép log đẩy thẳng ra Docker console
    ]
)
logger = logging.getLogger(__name__)

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
# =========================
# MAIN PARSER
# =========================
def parse_job(scraper, url):

    soup = get_soup(scraper, url)
    if not soup:
        return None
    with open("debug.html", "w", encoding="utf-8") as f:
        f.write(soup.prettify())
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
    if not valid_through:
        valid_through = int(datetime.fromisoformat(json_ld.get("validThrough")).timestamp() * 1000)

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
"""
def save_jsonl(record, file="raw_jobs.jsonl"):
    with open(file, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def save_json(data, file="data1.json"):
    with open(file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
"""
# =========================
# RUN
# =========================
def save_json(data, file="data1.json"):
    with open(file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
def main():
    url = "https://www.topcv.vn/brand/fptis/tuyen-dung/frontend-developer-angular-reactjs-j2125875.html?ta_source=SuggestSimilarJob_LinkDetail&jr_i=job-es-v1%3A%3A1777810062389-63b9e3%3A%3A5a5e162c44b14d76912a6a4a1f2226d6%3A%3A5%3A%3A0.9500"
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

import os
from datetime import datetime

import os
from datetime import datetime

def log_missing_fields(record, url, log_file="missing_jobs.log"):
    """
    Hàm kiểm tra lỗi dựa trên quality_flags đã được tạo sẵn trong record.
    """
    if not record or "quality_flags" not in record:
        return False
        
    flags = record["quality_flags"]
    
    # 1. Từ điển ánh xạ: Tên trường -> Tên cờ tương ứng trong quality_flags
    critical_flags = {
        "salary": "has_salary_info",
        "location": "has_location_info",
        "experience": "has_experience_info",
        "requirements": "has_requirements",
        "description": "has_description",
        "benefits": "has_benefits",
        "schedule": "has_schedule",
        "deadline": "has_valid_deadline"
    }
    
    missing_fields = []
    
    # 2. Duyệt qua các cờ quan trọng, nếu cờ mang giá trị False => Thiếu data
    for field_name, flag_name in critical_flags.items():
        if flags.get(flag_name) is False:
            missing_fields.append(field_name)
            
    # 3. Ghi Log nếu có trường bị thiếu
    if missing_fields:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] THIẾU {missing_fields} | URL: {url}\n"
        
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_message)
            
        return True # Báo hiệu Job bị thiếu data
    
    return False # Job hoàn hảo

def run_batch_crawler(start_page=1, end_page=3):
    """
    Luồng chạy chính của chế độ Batch.
    """
    # 1. Khởi tạo 1 Scraper duy nhất cho toàn bộ quá trình
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    
    total_jobs_saved = 0
    jobs_with_missing_data = 0  
    # 2. Vòng lặp cào qua các trang danh sách
    for page in range(start_page, end_page + 1):
        links = get_job_links(scraper, page)
        print(f"[-] Tìm thấy {len(links)} links ở trang {page}. Bắt đầu bóc tách...")
        
        # 3. Vòng lặp cào chi tiết từng bài đăng
        for idx, link in enumerate(links):
            print(f"   + Cào chi tiết {idx+1}/{len(links)}: {link}")
            
            record = parse_job(scraper, link)
            
            if record:
                # ----------------------------------------------------
                # VŨ KHÍ MỚI: QUÉT LỖI TRƯỚC KHI LƯU
                # ----------------------------------------------------
                is_missing = log_missing_fields(record, link)
                if is_missing:
                    jobs_with_missing_data += 1
                    print(f"      [!] Cảnh báo: Job này thiếu data. Đã lưu vào log!")
                save_jsonl(record, "raw_jobs_batch.jsonl")
                total_jobs_saved += 1
            
            # NGỦ ĐỂ VƯỢT RÀO: Nghỉ 1-3 giây giữa mỗi link chi tiết
            time.sleep(random.uniform(1.5, 3.5))
            
        # Nghỉ dài hơn một chút khi chuyển sang trang danh sách tiếp theo
        print(f"[zZz] Xong trang {page}, nghỉ ngơi 5 giây...")
        time.sleep(5)
        
    # Thay cho các câu print cũ của bạn:
    logger.info("Batch Crawler hoàn tất!")
    logger.info(f"Tổng số Job đã lưu: {total_jobs_saved}")
    
    if jobs_with_missing_data > 0:
        # Dùng level WARNING để đánh dấu có sự cố (Docker/Kibana sẽ bôi màu vàng/đỏ)
        logger.warning(f"Có {jobs_with_missing_data} Job bị thiếu dữ liệu. Vui lòng check missing_jobs.log!")

if __name__ == "__main__":
    #main()
    run_batch_crawler(7, 8)