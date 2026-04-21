import cloudscraper
from bs4 import BeautifulSoup
import time
import csv
import random


def crawl_job_detail(scraper, link):
    try:
        res = scraper.get(link)
        if res.status_code != 200:
            return None

        soup = BeautifulSoup(res.text, 'html.parser')

        data = {}

        # job_id
        data['job_id'] = link.split('-')[-1]

        # title
        title = soup.select_one('h1')
        data['title'] = title.get_text(strip=True) if title else ""

        # salary (STRING)
        salary_elem = soup.select_one('.job-detail__info--salary')
        data['salary'] = salary_elem.get_text(strip=True) if salary_elem else "Thỏa thuận"

        # location
        loc = soup.select_one('.job-detail__info--address')
        data['location'] = loc.get_text(strip=True) if loc else ""

        # experience
        exp = soup.select_one('.job-detail__info--exp')
        data['experience'] = exp.get_text(strip=True) if exp else ""

        # deadline
        deadline = soup.select_one('.job-detail__info--deadline')
        data['application_deadline'] = deadline.get_text(strip=True) if deadline else ""

        # description
        desc = soup.select_one('.job-description')
        data['job_description'] = desc.get_text("\n", strip=True) if desc else ""

        # requirements
        req = soup.select_one('.job-requirements')
        data['requirements'] = req.get_text("\n", strip=True) if req else ""

        # benefits
        benefit = soup.select_one('.job-benefits')
        data['benefits'] = benefit.get_text("\n", strip=True) if benefit else ""

        # working hours (TopCV thường không rõ → để trống)
        data['working_hours'] = ""

        # expertise (skills list)
        skills = soup.select('.job-tags a')
        data['expertise'] = [s.get_text(strip=True) for s in skills]

        return data

    except Exception as e:
        print(f"Lỗi detail: {e}")
        return None


def crawl_topcv(total_jobs_needed=50):
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )

    all_jobs = []
    page = 1

    while len(all_jobs) < total_jobs_needed:
        url = f"https://www.topcv.vn/tim-viec-lam-moi-nhat?page={page}"
        print(f"Đang quét list page {page}...")

        try:
            res = scraper.get(url)
            if res.status_code != 200:
                break

            soup = BeautifulSoup(res.text, 'html.parser')
            job_items = soup.select('.job-item-2') or soup.select('.job-item-search-result')

            if not job_items:
                print("Không tìm thấy job, dừng.")
                break

            for item in job_items:
                if len(all_jobs) >= total_jobs_needed:
                    break

                title_elem = item.select_one('.title a') or item.select_one('.job-title a')
                if not title_elem:
                    continue

                link = title_elem.get('href')
                print(f" -> Crawl detail: {link}")

                job_data = crawl_job_detail(scraper, link)

                if job_data:
                    all_jobs.append(job_data)

                time.sleep(random.uniform(1, 2))  # tránh bị block

            page += 1

        except Exception as e:
            print(f"Lỗi page {page}: {e}")
            break

    return all_jobs


def save_csv(data, filename="topcv_jobs.csv"):
    if not data:
        return

    keys = data[0].keys()

    with open(filename, "w", newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    jobs = crawl_topcv(20)

    print(f"\nĐã crawl được {len(jobs)} jobs")

    save_csv(jobs)
    print("Đã lưu file CSV")