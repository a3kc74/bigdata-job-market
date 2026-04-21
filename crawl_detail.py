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