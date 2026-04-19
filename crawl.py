import cloudscraper
from bs4 import BeautifulSoup
import time
import json # Sửa: Dùng JSON thay vì CSV
import hashlib

def crawl_topcv_list(total_jobs_needed=10):
    scraper = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True})
    all_jobs = []
    page = 1
    
    while len(all_jobs) < total_jobs_needed:
        url = f"https://www.topcv.vn/tim-viec-lam-moi-nhat?page={page}"
        print(f"Đang quét trang {page}...")
        
        try:
            response = scraper.get(url)
            if response.status_code != 200: break
            soup = BeautifulSoup(response.text, 'html.parser')
            job_items = soup.select('.job-item-2') or soup.select('.job-item-search-result')

            if not job_items: break

            current_time = int(time.time())

            for item in job_items:
                if len(all_jobs) >= total_jobs_needed: break
                
                title_elem = item.select_one('.title a') or item.select_one('.job-title a')
                company_elem = item.select_one('.company a') or item.select_one('.company-name')
                
                title = title_elem.get_text(strip=True) if title_elem else "N/A"
                link = title_elem.get('href', '') if title_elem else ""
                company = company_elem.get_text(strip=True) if company_elem else "N/A"
                
                # Trích xuất ID từ link (Ví dụ giả lập)
                job_id_raw = link.split('/')[-1].split('.')[0] if link else f"unknown_{current_time}"

                # NẶN VÀO KHUÔN 12 TRƯỜNG CỦA TV1
                record = {
                    "job_id": f"topcv_{job_id_raw}",
                    "company_name": company,
                    "job_title": title,
                    "location": "N/A", # Tạm thời điền N/A nếu code hiện tại chưa cào được
                    "salary_min": 0, 
                    "salary_max": 0,
                    "currency": "VND",
                    "source_url": link,
                    "ingest_ts": current_time,
                    "event_ts": current_time - 86400, # Giả lập đăng hôm qua
                    "source": "topcv"
                }
                
                # Sinh Metadata: Hash content
                hash_string = f"{record['job_title']}_{record['company_name']}"
                record["hash_content"] = hashlib.md5(hash_string.encode('utf-8')).hexdigest()
                
                all_jobs.append(record)
            
            page += 1
            time.sleep(2) 

        except Exception as e:
            print(f"Lỗi: {e}")
            break

    return all_jobs

# Chạy và Lưu chuẩn JSON
jobs = crawl_topcv_list(10) # Test thử 10 job trước
if jobs:
    with open('topcv_jobs.json', 'w', encoding='utf-8') as f:
        json.dump(jobs, f, ensure_ascii=False, indent=4)
    print("Thành công! Đã lưu file topcv_jobs.json chuẩn Schema dự án.")