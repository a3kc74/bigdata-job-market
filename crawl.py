import cloudscraper
from bs4 import BeautifulSoup
import time
import json
import hashlib
import random
from kafka import KafkaProducer

# ==========================================
# CẤU HÌNH CƠ BẢN
# ==========================================
SOURCE_NAME = "topcv"
KAFKA_TOPIC = "jobs_raw"
KAFKA_SERVER = 'localhost:9092'
BATCH_OUTPUT_FILE = "topcv_historical_data.json"
ERROR_OUTPUT_FILE = "topcv_error_records.json"

# ==========================================
# CHIẾN LƯỢC 2 & 3: VƯỢT RÀO & CHUẨN HÓA SCHEMA
# ==========================================
def fetch_and_parse_jobs(page=1):
    """Cào dữ liệu 1 trang, ép vào Schema và lọc lỗi"""
    scraper = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True})
    url = f"https://www.topcv.vn/tim-viec-lam-it-phan-mem-c10026?page={page}"
    
    valid_jobs = []
    error_jobs = []
    
    try:
        # CHIẾN LƯỢC 4: BẢO VỆ LUỒNG CODE (Timeout & Status Code)
        response = scraper.get(url, timeout=15)
        if response.status_code != 200:
            print(f"[!] Lỗi truy cập trang {page} (Mã lỗi: {response.status_code})")
            return valid_jobs, error_jobs
            
        soup = BeautifulSoup(response.text, 'html.parser')
        job_items = soup.select('.job-item-2') or soup.select('.job-item-search-result')
        
        current_time = int(time.time())
        
        for item in job_items:
            try:
                # 1. Bóc tách HTML
                title_elem = item.select_one('.title a') or item.select_one('.job-title a')
                company_elem = item.select_one('.company a') or item.select_one('.company-name')
                location_elem = item.select_one('.address')
                
                title = title_elem.get_text(strip=True) if title_elem else None
                link = title_elem.get('href', '') if title_elem else None
                company = company_elem.get_text(strip=True) if company_elem else "N/A"
                location = location_elem.get_text(strip=True) if location_elem else "N/A"
                
                # CHIẾN LƯỢC 4: PHÂN LUỒNG RÁC (Kiểm tra Data Quality)
                if not title or not link:
                    error_jobs.append({"page": page, "error": "Thiếu Title hoặc Link", "raw_html": str(item)[:100]})
                    continue # Bỏ qua record này, đi tiếp

                job_id_raw = link.split('/')[-1].split('.')[0]

                # 2. CHUẨN HÓA SCHEMA 12 TRƯỜNG
                record = {
                    "job_id": f"{SOURCE_NAME}_{job_id_raw}",
                    "company_name": company,
                    "job_title": title,
                    "location": location,
                    "salary_min": 0, # Mặc định chờ TV3 parse sâu hơn
                    "salary_max": 0,
                    "currency": "VND",
                    "source_url": link,
                    
                    # 3. TỰ SINH METADATA
                    "ingest_ts": current_time,
                    "event_ts": current_time - random.randint(3600, 86400), # Giả lập time
                    "source": SOURCE_NAME
                }
                
                # Tạo hash_content chống trùng lặp
                hash_string = f"{record['job_title']}_{record['company_name']}_{record['location']}"
                record["hash_content"] = hashlib.md5(hash_string.encode('utf-8')).hexdigest()
                
                valid_jobs.append(record)
                
            except Exception as e:
                error_jobs.append({"page": page, "error": str(e)})
                continue
                
        return valid_jobs, error_jobs

    except Exception as e:
        print(f"[!] Lỗi kết nối mạng tại trang {page}: {e}")
        return valid_jobs, error_jobs

# ==========================================
# CHIẾN LƯỢC 1: TÁCH LUỒNG (BATCH & STREAM)
# ==========================================

def run_historical_batch(total_pages=5):
    """Luồng cào sâu (Deep Crawl) để lấy dữ liệu lịch sử"""
    print(f"\n🚀 BẮT ĐẦU LUỒNG BATCH: Cào {total_pages} trang lịch sử...")
    all_valid_jobs = []
    all_error_jobs = []
    
    for page in range(1, total_pages + 1):
        print(f"Đang quét trang {page}...")
        valid, errors = fetch_and_parse_jobs(page)
        all_valid_jobs.extend(valid)
        all_error_jobs.extend(errors)
        
        # Mưa dầm thấm lâu: Nghỉ ngơi tránh block IP
        time.sleep(random.uniform(2.5, 5.0)) 
        
    # Lưu file dữ liệu chuẩn
    if all_valid_jobs:
        with open(BATCH_OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_valid_jobs, f, ensure_ascii=False, indent=4)
        print(f"✅ Đã lưu {len(all_valid_jobs)} jobs chuẩn vào {BATCH_OUTPUT_FILE}")
        
    # Lưu file rác/lỗi (Dead-letter)
    if all_error_jobs:
        with open(ERROR_OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_error_jobs, f, ensure_ascii=False, indent=4)
        print(f"⚠️ Phát hiện {len(all_error_jobs)} records lỗi. Đã lưu vào {ERROR_OUTPUT_FILE}")

def run_realtime_stream():
    """Luồng cào nông (Shallow Crawl) bot trực chiến Kafka"""
    print(f"\n📡 BẮT ĐẦU LUỒNG STREAM: Bot trực chiến lắng nghe việc làm mới...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"[!] Lỗi kết nối Kafka ({KAFKA_SERVER}): {e}. Vui lòng bật Docker Compose!")
        return

    seen_hashes = set()
    
    while True:
        try:
            print(f"\n[{time.strftime('%H:%M:%S')}] Đang quét việc làm IT mới nhất ở Trang 1...")
            valid_jobs, _ = fetch_and_parse_jobs(page=1)
            
            new_jobs_count = 0
            for job in valid_jobs:
                # Khử trùng lặp tại cổng bằng hash_content
                if job['hash_content'] not in seen_hashes:
                    producer.send(KAFKA_TOPIC, job)
                    seen_hashes.add(job['hash_content'])
                    new_jobs_count += 1
                    print(f"  [Kafka ->] Đã bắn: {job['job_title']} ({job['company_name']})")
                    
            producer.flush()
            print(f"✅ Đã cập nhật {new_jobs_count} việc làm mới. Đang ngủ 5 phút...")
            
            # Quét lại sau mỗi 5 phút
            time.sleep(300) 
            
        except KeyboardInterrupt:
            print("\n🛑 Đã dừng luồng Stream.")
            break
        except Exception as e:
            print(f"Lỗi luồng Stream: {e}. Sẽ thử lại sau 60s...")
            time.sleep(60)

# ==========================================
# ĐIỀU KHIỂN
# ==========================================
if __name__ == "__main__":
    # Thay đổi biến này thành 'stream' khi bạn muốn test luồng Kafka
    MODE = 'batch' 
    
    if MODE == 'batch':
        # Cào thử 3 trang lịch sử
        run_historical_batch(total_pages=3)
    elif MODE == 'stream':
        # Chạy bot liên tục
        run_realtime_stream()