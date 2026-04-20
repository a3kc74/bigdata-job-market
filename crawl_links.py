import cloudscraper
from bs4 import BeautifulSoup

BASE_URL = "https://www.topcv.vn"

scraper = cloudscraper.create_scraper()

def get_job_links(page=1):
    url = f"{BASE_URL}/tim-viec-lam-it-phan-mem-c10026?page={page}"
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