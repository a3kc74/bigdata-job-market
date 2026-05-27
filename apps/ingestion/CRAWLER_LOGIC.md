# Giải thích logic crawler

File này giải thích crawler theo cách dễ đọc hơn: crawler chạy để làm gì, chạy
theo các bước nào, các biến cấu hình quan trọng có nghĩa gì, và vì sao speed
mode với batch mode lại khác nhau.

## 1. Crawler trong project này dùng để làm gì?

Crawler có nhiệm vụ lấy job từ TopCV, parse các thông tin cần thiết, rồi ghi ra
JSONL.

Từ JSONL đó:

- Batch pipeline sẽ nạp vào HDFS rồi chạy ETL Raw -> Bronze -> Silver -> Gold
- Speed pipeline sẽ có producer tail file JSONL và đẩy dần từng record vào
  Kafka topic `jobs_raw`

Nói ngắn gọn:

- Crawler không đi thẳng vào Elasticsearch
- Crawler cũng không đi thẳng vào Gold
- Crawler là nguồn phát sinh dữ liệu ban đầu

## 2. Những file nào chịu trách nhiệm chính?

### `run_crawler.py`

Đây là file CLI wrapper.

Nó quyết định:

- Chạy ở chế độ `speed`
- Hay chạy ở chế độ `batch`

### `topcv_crawler.py`

Đây là nơi chứa logic crawl thật:

- Quét list page
- Chọn link job cần lấy
- Vào detail page
- Parse HTML
- Ghi JSONL
- Retry khi lỗi
- Cooldown khi bị block hoặc request quá dày

### `run_crawler_to_hdfs.py`

File này là wrapper phục vụ luồng batch/HDFS.

## 3. Speed mode là gì?

Speed mode là chế độ crawl nhẹ, ưu tiên dữ liệu mới.

Lệnh chạy:

```bash
python -m apps.ingestion.run_crawler --mode speed
```

Ý tưởng của speed mode:

- Chỉ quét một số page đầu
- Chỉ lấy các job vừa mới update gần đây
- Tránh crawl lặp lại cùng một job liên tục

Mục tiêu:

- Lấy dữ liệu mới nhanh
- Đẩy sớm sang Kafka
- Để speed layer có dữ liệu gần realtime hơn batch

## 4. Giải thích 4 biến cấu hình speed quan trọng

Trong manifest speed hiện tại có các biến:

- `SPEED_CRAWL_MAX_PAGES=15`
- `SPEED_UPDATED_WITHIN_MINUTES=30`
- `SPEED_LIST_PAGES_PER_CHUNK=5`
- `SPEED_DETAIL_BATCH_SIZE=40`

### `SPEED_CRAWL_MAX_PAGES=15`

Nghĩa là:

- Trong một lần chạy speed, crawler chỉ quét tối đa 15 page list đầu tiên

Vì sao?

- Job mới thường nằm ở các page đầu
- Speed mode không cần quét quá sâu như batch
- Giới hạn page giúp giảm thời gian crawl và giảm nguy cơ bị block

Hiểu đơn giản:

- Đây là giới hạn độ sâu của speed crawl

### `SPEED_UPDATED_WITHIN_MINUTES=30`

Nghĩa là:

- Speed chỉ giữ các job có thời gian update nằm trong 30 phút gần nhất

Ví dụ:

- Crawler chạy lúc `10:30`
- Threshold sẽ là `10:00`
- Các job update trước `10:00` sẽ bị bỏ qua

Vì sao?

- Speed chỉ quan tâm dữ liệu mới
- Không muốn bắn lại quá nhiều job cũ

Hiểu đơn giản:

- Đây là cửa sổ “job mới đến mức nào thì còn đáng lấy”

### `SPEED_LIST_PAGES_PER_CHUNK=5`

Nghĩa là:

- Crawler không quét 15 page một lèo rồi mới xử lý detail
- Nó chia thành từng lô 5 page

Ví dụ:

- Lô 1: page 1 -> 5
- Lô 2: page 6 -> 10
- Lô 3: page 11 -> 15

Sau khi gom link từ một lô, crawler sẽ chuyển sang xử lý detail trước, rồi mới
quay lại quét tiếp lô sau.

Vì sao?

- Giúp dữ liệu mới được xử lý sớm hơn
- Không để queue link phình quá to
- Giảm cảm giác “phải đợi lâu mới có record đầu tiên”

Hiểu đơn giản:

- Chunk là kích thước lô quét list page

### `SPEED_DETAIL_BATCH_SIZE=40`

Nghĩa là:

- Mỗi lần crawler lấy detail tối đa 40 link từ queue ra để xử lý

Nếu queue đang có 120 link:

- Đợt 1 xử lý 40
- Đợt 2 xử lý 40
- Đợt 3 xử lý 40

Vì sao?

- Tránh ôm toàn bộ queue quá lớn vào một đợt xử lý
- Giúp crawler chạy theo nhịp đều hơn
- Dễ chèn retry/cooldown hơn

Hiểu đơn giản:

- Đây là kích thước mẻ xử lý detail

## 5. Batch mode là gì?

Batch mode là chế độ crawl sâu hơn, ưu tiên độ đầy đủ hơn là độ tươi.

Lệnh chạy:

```bash
python -m apps.ingestion.run_crawler --mode batch
```

Khác với speed mode, batch không chỉ nhìn 30 phút gần nhất mà thường nhìn theo
một cửa sổ ngày.

Ví dụ:

- Crawl các job được update trong 7 ngày gần nhất

Đó là ý của câu:

- “Crawl theo cửa sổ ngày thay vì cửa sổ thời gian ngắn”

Giải thích dễ hiểu hơn:

- Speed mode hỏi: “job nào mới trong khoảng vài chục phút gần đây?”
- Batch mode hỏi: “job nào còn nằm trong vùng dữ liệu của vài ngày gần đây?”

Vì vậy:

- Speed = nhẹ, nhanh, gần realtime
- Batch = sâu hơn, đầy đủ hơn, phục vụ data lake / serving chuẩn

## 6. Tại sao batch và speed không dùng cùng một cơ chế bỏ trùng?

Vì mục tiêu khác nhau.

### Speed mode

Speed mode không muốn phát đi phát lại cùng một job quá nhiều lần giữa các lần
chạy gần nhau.

Do đó speed mode dùng:

- `speed processed cache`

### Batch mode

Batch mode không dùng cache này vì batch phải được phép crawl lại job cũ nếu
job đó vừa được chỉnh sửa hoặc update lại.

Nếu batch cũng bỏ qua job chỉ vì “đã thấy rồi”, batch sẽ bỏ lỡ thay đổi dữ liệu.

## 7. Speed processed cache là gì?

Đây là cache lưu các `job_id` mà speed mode đã xử lý gần đây.

Mục đích:

- nếu cùng một job vẫn còn xuất hiện trên các page đầu của TopCV
- thì speed mode không cần crawl lại và bắn lại nó liên tục

Cache này dùng file:

- `runtime/crawler/speed_processed_jobs_29d.json`

Và có TTL mặc định:

- `29 ngày`

### Cơ chế hoạt động

Khi speed mode chạy:

1. crawler load cache các `job_id` đã xử lý
2. khi quét list page:
   - nếu `job_id` chưa có trong cache -> crawl detail như bình thường
   - nếu `job_id` đã có trong cache nhưng `listing_updated_time` trên card **mới hơn** lần đã xử lý trước -> vẫn crawl lại
   - chỉ bỏ qua khi `job_id` đã có trong cache và `listing_updated_time` **không mới hơn**
3. khi xử lý detail thành công và ghi JSONL xong, crawler mới mark `job_id` đó
   vào cache

Điểm quan trọng:

- chỉ job xử lý thành công mới được mark cache
- job lỗi chưa được coi là hoàn thành

Hiểu đơn giản:

- speed processed cache là bộ nhớ “job này gần đây tao đã làm rồi”
- nhưng không còn bỏ qua mù quáng chỉ theo `job_id`
- nó còn so thêm thời gian `listing_updated_time` trên list page để không bỏ sót job đã được update nội dung

## 8. Crawler chạy theo những bước nào?

Logic chính của `topcv_crawler.run_master_crawler(...)` có thể hiểu thành 2 pha
xen kẽ nhau.

### Pha 1: quét list page để gom link

Crawler sẽ:

1. vào page list job
2. đọc các job card trên page đó
3. lấy URL job
4. kiểm tra thời gian update
5. chỉ giữ job còn nằm trong cửa sổ thời gian
6. đưa link đó vào queue

Nếu một page không còn job nào mới hơn threshold:

- crawler có thể dừng quét các page sau

Ý nghĩa:

- không cần quét sâu vô ích

### Pha 2: vào detail page để lấy dữ liệu thật

Với từng link trong queue:

1. request detail page
2. parse title, company, salary, location, skills, ...
3. bổ sung metadata thời gian lấy từ list page
4. ghi record ra JSONL
5. nếu là speed mode và lưu thành công thì mark cache

## 9. Vì sao dữ liệu speed có thể chảy sang Kafka trước khi crawler chạy xong?

Vì crawler chỉ có nhiệm vụ ghi JSONL.

Song song với crawler, có một producer khác:

- `apps/producer/crawler_jsonl_producer.py`

Producer này:

1. watch file `jobs_speed_*.jsonl`
2. mỗi 2 giây kiểm tra có dòng mới không
3. nếu có thì publish dòng mới đó vào Kafka topic `jobs_raw`

Nghĩa là:

- crawler đang chạy đến đâu
- JSONL sinh ra đến đâu
- producer đẩy Kafka đến đó

Không cần chờ crawler hoàn tất toàn bộ run.

## 10. Vì sao crawler không chạy nhanh hết mức?

Vì nếu bắn request quá dày, rất dễ bị block.

Crawler hiện chủ động chậm lại ở vài điểm.

### Sleep giữa các list page

Sau mỗi list page, crawler nghỉ khoảng:

- 1.5 đến 3.0 giây

### Sleep giữa các detail page

Sau mỗi detail page, crawler nghỉ khoảng:

- 1.5 đến 3.5 giây

### Cooldown sau mỗi 40 request

Sau mỗi 40 request, crawler nghỉ thêm khoảng:

- 20 đến 30 giây

### Recover khi bị block

Nếu bị TopCV block:

1. crawler chờ
2. thử harvest cookie mới
3. retry bằng session hiện tại
4. nếu vẫn fail thì tạo session mới và retry tiếp

Vì vậy tốc độ crawl thực tế không cố định.

## 11. Thời gian ước tính để crawl 1 job là bao nhiêu?

Trong điều kiện tương đối bình thường:

- 1 job detail thường rơi vào khoảng 2.5 đến 5 giây

Con số này gồm:

- thời gian request
- thời gian parse HTML
- sleep giữa các detail page

Nếu gặp cooldown hoặc block recovery:

- tốc độ sẽ chậm hơn đáng kể

## 12. Vì sao trigger của speed stream không nên dựa vào tổng thời gian crawl?

Vì speed stream không chờ cả crawler run xong mới có dữ liệu.

Dữ liệu được sinh dần như sau:

- crawler ghi JSONL dần dần
- producer đẩy Kafka dần dần
- Spark stream đọc Kafka theo micro-batch

Nên trigger phù hợp phải dựa vào:

- producer poll bao lâu một lần
- record mới xuất hiện nhanh cỡ nào
- bạn muốn ES tươi cỡ nào

Chứ không nên hỏi:

- “một phiên crawl tổng cộng mất bao nhiêu phút?”

## 13. Nên nhìn runtime ở đâu nếu muốn debug?

Bạn nên nhìn các chỗ sau:

- log của `apps.ingestion.run_crawler`
- file JSONL trong `CRAWLER_LOCAL_OUTPUT_DIR`
- Kafka topic `jobs_raw`
- log của Spark driver trong `apps/stream_etl/stream_main.py`

## 14. Cách đo tốc độ crawl

Đã có script hỗ trợ:

- `scripts/measure_topcv_crawler_speed.py`

Script này sẽ:

1. chạy một speed crawl thật
2. đo tổng thời gian chạy
3. tìm các file JSONL vừa sinh ra
4. đếm số job lưu được
5. tính:
   - jobs/second
   - jobs/minute
   - seconds/job

Lý do nên dùng script thật thay vì unit test:

- tốc độ crawl phụ thuộc mạng
- phụ thuộc tốc độ response của TopCV lúc đó
- phụ thuộc có bị block hay không
- phụ thuộc `max_pages`, `chunk`, `detail_batch_size`

Nói ngắn gọn:

- đây là bài toán integration/runtime
- không phải bài toán unit test thuần
