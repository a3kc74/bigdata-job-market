import cloudscraper
from bs4 import BeautifulSoup
import time
import csv

def crawl_topcv_list(total_jobs_needed=100):
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    
    all_jobs = []
    page = 1
    
    print(f"Bắt đầu crawl {total_jobs_needed} công việc...")

    while len(all_jobs) < total_jobs_needed:
        # URL tìm kiếm việc làm chung
        url = f"https://www.topcv.vn/tim-viec-lam-moi-nhat?page={page}"
        print(f"Đang quét trang {page}...")
        
        try:
            response = scraper.get(url)
            if response.status_code != 200:
                print(f"Lỗi truy cập trang {page} (Status: {response.status_code})")
                break
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Selector mới: Tìm tất cả các box chứa job
            # Thường nằm trong class .job-item-2 hoặc .job-item-search-result
            job_items = soup.select('.job-item-2') or soup.select('.job-item-search-result')

            if not job_items:
                # Debug: Nếu không tìm thấy, in ra một đoạn HTML để kiểm tra (tùy chọn)
                print(f"Không tìm thấy job-item ở trang {page}. Dừng lại.")
                break

            for item in job_items:
                if len(all_jobs) >= total_jobs_needed:
                    break
                
                # Lấy tiêu đề và link
                title_elem = item.select_one('.title a') or item.select_one('.job-title a')
                # Lấy tên công ty
                company_elem = item.select_one('.company a') or item.select_one('.company-name')
                # Lấy mức lương
                salary_elem = item.select_one('.label-item .text-main') or item.select_one('.salary')

                if title_elem:
                    title = title_elem.get_text(strip=True)
                    link = title_elem.get('href', '')
                    company = company_elem.get_text(strip=True) if company_elem else "N/A"
                    salary = salary_elem.get_text(strip=True) if salary_elem else "Thỏa thuận"

                    all_jobs.append({
                        "STT": len(all_jobs) + 1,
                        "Tiêu đề": title,
                        "Công ty": company,
                        "Lương": salary,
                        "Link": link
                    })
            
            print(f"-> Đã lấy được {len(all_jobs)} công việc...")
            page += 1
            time.sleep(2) 

        except Exception as e:
            print(f"Lỗi tại trang {page}: {e}")
            break

    return all_jobs

# Chạy script
jobs = crawl_topcv_list(100)

if jobs:
    print(f"\n--- THÀNH CÔNG: LẤY ĐƯỢC {len(jobs)} CÔNG VIỆC ---")
    
    # Lưu file CSV
    keys = jobs[0].keys()
    with open('topcv_jobs.csv', 'w', newline='', encoding='utf-8-sig') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(jobs)
    print("Dữ liệu đã được lưu vào file: topcv_jobs.csv")
else:
    print("\nKhông lấy được dữ liệu nào. Vui lòng kiểm tra lại kết nối hoặc URL.")



import csv

keys = jobs[0].keys()
with open('topcv_jobs.csv', 'w', newline='', encoding='utf-8-sig') as output_file:
    dict_writer = csv.DictWriter(output_file, fieldnames=keys)
    dict_writer.writeheader()
    dict_writer.writerows(jobs)
print("Dữ liệu đã được lưu vào file topcv_jobs.csv")