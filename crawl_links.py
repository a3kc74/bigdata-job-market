import cloudscraper
from bs4 import BeautifulSoup

scraper = cloudscraper.create_scraper()

def get_job_links(page=1):
    url = f"https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?category_family=r257&page={page}"
    res = scraper.get(url)
    
    soup = BeautifulSoup(res.text, "html.parser")
    
    job_links = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        
        if "/viec-lam/" in href:
            if href.startswith("/"):
                href = BASE_URL + href
            job_links.add(href)
    return list(job_links)

import time
import random

def parse_job_detail(url):
    try:
        res = scraper.get(url, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")

        # fallback selector (đừng tin 1 cái)
        title = (
            soup.find("h1")
        )

        company = (
            soup.find("h2")
        )

        # ví dụ text anchor
        deadline = None
        label = soup.find(string=lambda x: x and "Hạn nộp" in x)
        if label:
            parent = label.find_parent()
            if parent:
                deadline = parent.get_text(strip=True)

        return {
            "url": url,
            "title": title.get_text(strip=True) if title else None,
            "company": company.get_text(strip=True) if company else None,
            "deadline": deadline
        }

    except Exception as e:
        print(f"[!] Lỗi {url}: {e}")
        return None
    
def crawl_demo():
    links = get_job_links(page=1)
    
    print(f"Lấy được {len(links)} job links")

    results = []

    for link in links[:5]:  # demo lấy 5 job thôi
        job = parse_job_detail(link)
        if job:
            results.append(job)

        time.sleep(random.uniform(1, 2))  # tránh bị block

    return results

if __name__ == "__main__":
    data = crawl_demo()
    
    for job in data:
        print(job)