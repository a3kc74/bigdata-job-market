import cloudscraper
from bs4 import BeautifulSoup
import json
import time
import random


# ======================
# JSON-LD EXTRACT
# ======================
def extract_json_ld(soup):
    scripts = soup.find_all("script", type="application/ld+json")
    
    for tag in scripts:
        try:
            data = json.loads(tag.string)

            # có thể là list
            if isinstance(data, list):
                for item in data:
                    if item.get("@type") == "JobPosting":
                        return item
            else:
                if data.get("@type") == "JobPosting":
                    return data
        except:
            continue

    return None


# ======================
# PARSE JOB FROM JSON-LD
# ======================
def parse_job_jsonld(data, url):
    job = {}

    job["source"] = "topcv"
    job["source_url"] = url

    job["title"] = data.get("title")

    # company
    job["company_name"] = (
        data.get("hiringOrganization", {}).get("name")
    )

    # location
    job["location"] = (
        data.get("jobLocation", {})
        .get("address", {})
        .get("addressLocality")
    )

    # salary (để raw string cho đơn giản)
    if "baseSalary" in data:
        job["salary"] = str(data["baseSalary"])
    else:
        job["salary"] = None

    job["employment_type"] = data.get("employmentType")
    job["date_posted"] = data.get("datePosted")
    job["valid_through"] = data.get("validThrough")

    # description (HTML text)
    job["description"] = data.get("description")

    # optional
    job["job_id_source"] = data.get("identifier", {}).get("value")

    return job


# ======================
# FALLBACK HTML (nếu thiếu)
# ======================
def fallback_html(job, soup):
    if not job.get("company_name"):
        el = soup.select_one(".company-name")
        if el:
            job["company_name"] = el.get_text(strip=True)

    if not job.get("location"):
        el = soup.select_one(".job-detail__info--address")
        if el:
            job["location"] = el.get_text(strip=True)

    if not job.get("salary"):
        el = soup.select_one(".salary")
        if el:
            job["salary"] = el.get_text(strip=True)

    return job


# ======================
# DETAIL CRAWL
# ======================
def crawl_detail(scraper, url):
    try:
        res = scraper.get(url)
        if res.status_code != 200:
            return None

        soup = BeautifulSoup(res.text, "html.parser")

        json_ld = extract_json_ld(soup)
        if not json_ld:
            print("⚠️ Không có JSON-LD:", url)
            return None

        job = parse_job_jsonld(json_ld, url)
        job = fallback_html(job, soup)

        return job

    except Exception as e:
        print("Lỗi detail:", e)
        return None


# ======================
# LIST CRAWL
# ======================
def crawl_topcv(limit=50):
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )

    results = []
    page = 1

    while len(results) < limit:
        url = f"https://www.topcv.vn/tim-viec-lam-moi-nhat?page={page}"
        print(f"Scan page {page}")

        try:
            res = scraper.get(url)
            if res.status_code != 200:
                break

            soup = BeautifulSoup(res.text, "html.parser")

            jobs = soup.select(".job-item-2") or soup.select(".job-item-search-result")
            if not jobs:
                break

            for item in jobs:
                if len(results) >= limit:
                    break

                a = item.select_one(".title a") or item.select_one(".job-title a")
                if not a:
                    continue

                link = a.get("href")
                print(" ->", link)

                job = crawl_detail(scraper, link)
                if job:
                    results.append(job)

                time.sleep(random.uniform(1, 2))

            page += 1

        except Exception as e:
            print("Lỗi page:", e)
            break

    return results


# ======================
# SAVE JSON
# ======================
def save_json(data, filename="jobs.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ======================
# MAIN
# ======================
if __name__ == "__main__":
    jobs = crawl_topcv(30)

    print("Total:", len(jobs))

    save_json(jobs)
    print("Saved to jobs.json")