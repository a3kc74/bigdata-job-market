# THIẾT KẾ KIẾN TRÚC CRAWLER (DATA SOURCES)
**Người phụ trách:** Thành viên 2 (TV2) - Data Sources Owner
**Mục tiêu:** Xây dựng hệ thống thu thập dữ liệu tuyển dụng (TopCV) ổn định, bền bỉ, chống Anti-Bot hiệu quả và tuân thủ tuyệt đối Data Contract để cung cấp đầu vào cho hệ thống phân tích Big Data (Kiến trúc Lambda).

---

## 1. KIẾN TRÚC VÀ CHIẾN LƯỢC TÁCH LUỒNG (ROUTING STRATEGY)
Crawler được thiết kế theo 2 chế độ hoạt động song song để đáp ứng Kiến trúc Lambda. Mỗi luồng có một chiến lược tối ưu Request riêng biệt:

### 1.1. Luồng Batch (Historical Loader - Cào Sâu)
* **Mục đích:** Thu thập khối lượng lớn dữ liệu lịch sử hoặc quét định kỳ để cập nhật thay đổi (VD: mức lương thay đổi).
* **Tần suất chạy:** Chạy thủ công lần đầu (Đổ móng) hoặc định kỳ hàng tuần/tháng qua CronJob.
* **Vùng quét:** Rộng (Từ trang 1 đến trang 50 hoặc vét cạn danh mục).
* **Chiến lược tiết kiệm Request (Stateful Crawling):**
    * **Early Exit (Cắt đuôi sớm):** Quét qua các trang danh sách. Nếu phát hiện *toàn bộ 50 link trên 1 trang* đều đã tồn tại trong lịch sử cào -> Lập tức kích hoạt lệnh `break` dừng vòng lặp Pagination, bỏ qua các trang còn lại (vì chắc chắn là đồ cũ).
    * **Cập nhật tin (Update Handling):** Chấp nhận tốn thêm 1 request chui vào Detail Page để cào lại những job cũ nếu thời gian cào (last_scraped) đã quá 7 ngày. Việc lọc trùng lặp bản ghi sẽ được giao phó cho mã băm `hash_content` ở lớp Spark ETL phía sau.
* **Tích hợp (Đích đến):** Gom mẻ (Batching) xuất ra file `.jsonl` và đẩy thẳng vào vùng Raw của **HDFS** cho TV3.

### 1.2. Luồng Speed (Stream Producer - Cào Nông)
* **Mục đích:** Bắt các tin tuyển dụng mới nhất theo thời gian thực (Real-time).
* **Tần suất chạy:** Liên tục ngầm định mỗi 5 - 15 phút. 
* **Vùng quét:** Nông (Chỉ quét từ trang 1 đến trang 3).
* **Chiến lược tiết kiệm Request (Bỏ qua triệt để):**
    * Dùng màng lọc `seen_links` trên RAM (hoặc Redis). Quét 150 links ở 3 trang đầu, **loại bỏ 100%** các link đã từng thấy.
    * KHÔNG quan tâm đến việc nhà tuyển dụng "Làm mới" hay cập nhật tin cũ. Nhiệm vụ của Speed là chỉ bắt tin MỚI TOANH. Điều này giúp mỗi chu kỳ 5 phút, Bot chỉ tốn 1-2 requests để tải đúng các tin vừa xuất hiện, giữ cho IP an toàn tuyệt đối.
* **Tích hợp (Đích đến):** Đóng gói thành từng chuỗi JSON và dùng Kafka Producer bắn thẳng vào **Kafka** (topic: `jobs_raw`) cho TV4.

---

## 2. CHIẾN THUẬT CHỐNG ANTI-BOT (STEALTH ARCHITECTURE)
Để vượt qua WAF (Web Application Firewall) như Cloudflare cấp độ Enterprise mà không cần dùng trình duyệt cồng kềnh (Selenium/Playwright):

* **Network / TLS Layer:** Sử dụng thư viện `curl_cffi` (impersonate="chrome117") thay cho `requests` hay `cloudscraper` để giả lập hoàn hảo JA3/TLS Fingerprint của trình duyệt thật.
* **Behavior Pattern (Hành vi con người):**
    * *Homepage Warmup:* Luôn GET trang chủ trước để lấy Initial Cookies (Session ID, CSRF Token, CF Clearance) trước khi chui vào cào deep link.
    * *Micro-batching:* Cào 38-43 requests thì chủ động ngắt ống mạng (`session.close()`), nghỉ giải lao (cooldown) 2.5 - 4 phút rồi tạo session mới toanh. Điều này reset Rate-Limit của WAF.
    * *Delay ngẫu nhiên:* Nghỉ 2.0s - 4.0s giữa các requests detail.
* **IP Reputation:** Tích hợp **Residential Proxy** (Proxy dân cư) hoặc Rotating Proxy. Đồng nhất 1 Proxy IP cho 1 Session.

---

## 3. CHIẾN LƯỢC BÓC TÁCH & CHUẨN HÓA (RESILIENT PARSING)
Cấu trúc HTML của TopCV thường xuyên thay đổi (A/B Testing, đổi template). Crawler áp dụng kỹ thuật Parsing phòng thủ nhiều lớp:

* **Ưu tiên JSON-LD:** Lọc tìm thẻ `<script type="application/ld+json">` để lấy cấu trúc dữ liệu ngầm sạch sẽ nhất.
* **HTML Fallback & Heuristic:** * Khi JSON-LD thiếu dữ liệu, dùng BeautifulSoup quét vét đáy. 
    * *Không hardcode CSS class:* Dùng `any()` kết hợp Keyword (VD: `["địa điểm làm việc", "khu vực"]`) để rà quét text của thẻ tiêu đề (`h2`, `h3`, `div`), sau đó lấy cụm nội dung liền kề.
    * Làm sạch rác DOM toàn cục (xử lý lỗi dính chữ bằng `separator=' '`).
* **Mã băm Khử trùng lặp (`hash_content`):** Băm (`hashlib.md5`) các trường nội dung cốt lõi (Title, Description, Skills, Salary, Location, Company). Không đưa URL hay thời gian cào vào mã băm. Đây là "chìa khóa" để Spark của TV3/TV4 vứt bỏ các tin trùng lặp nội dung.
* **Tuân thủ Data Contract:** Chuẩn hóa đầu ra thành 12 trường dạng Flat JSON theo quy định của TV1.

---

## 4. QUẢN LÝ TRẠNG THÁI VÀ BỘ ĐỆM (CACHE & STATE MANAGEMENT)
Crawler bắt buộc tự duy trì bộ nhớ trạng thái để đảm bảo "dòng chảy 1 chiều" (Không chọc ngược xuống Database để check trùng lặp):

* **In-Memory & Storage:** Sử dụng biến `seen_links` trên RAM để check trùng lặp siêu tốc $O(1)$. Lưu dự phòng trạng thái xuống file `processed_links.txt` (Hoặc **Redis** khi cần chạy đa tiến trình/Multi-pod).
* **Eviction Strategy (Lãng quên):** Chỉ lưu URL kèm theo Timestamp. Thiết lập TTL (Time-To-Live) là **60 ngày**. Quá 60 ngày hệ thống sẽ tự động xóa link khỏi Cache để giải phóng RAM, vì bản thân tin tuyển dụng trên web cũng sẽ tự ẩn sau 30-60 ngày.

---

## 5. TRIỂN KHAI & VẬN HÀNH (DEPLOYMENT & LOGGING)
* **Đóng gói:** Dockerize toàn bộ source code bằng `Dockerfile`.
* **Logging chuẩn Cloud-Native:** Loại bỏ lệnh `print()`. Sử dụng thư viện `logging` của Python đẩy thẳng ra `sys.stdout` với định dạng chuẩn `%asctime | %levelname | CRAWLER | %message`. Kibana/Grafana của TV5 sẽ tự động hứng log.
* **Lên lịch Kubernetes:** Phối hợp cùng TV5 đẩy lên Minikube:
    * *Luồng Speed:* Triển khai dạng Deployment (1 Pod chạy 24/7).
    * *Luồng Batch:* Triển khai dạng CronJob (Thức dậy vào cuối tuần, cào xong rồi tự tắt).
