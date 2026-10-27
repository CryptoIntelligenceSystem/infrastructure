# Crypto Intelligence System — Rà soát hiện trạng và lộ trình triển khai

> Ngày rà soát: 2026-10-27. Phạm vi: 4 repo `data-pipeline`, `infrastructure`, `backend-api`, `frontend-dashboard` tại commit `main` mới nhất (`data-pipeline@a7a0df4`, `infrastructure@47ba79a`, `backend-api@de43ca9`, `frontend-dashboard@30f4a4e`).
> Mọi trích dẫn `file:dòng` tính theo nhánh `main` của repo tương ứng. Mục ghi "chưa xác minh" là suy luận từ code, chưa chạy thật.

## 1. Tóm tắt điều hành

- Nhóm đang ở **tuần 2–3** theo lịch (repo tạo ngày 2026-10-19). Code `data-pipeline` + `infrastructure` đã phủ phần lớn tuần 1–4 và có bản nháp tuần 5–6, nhưng **chưa từng chạy end-to-end** và chưa có bằng chứng kiểm chứng.
- `backend-api` và `frontend-dashboard` **trống hoàn toàn** (chỉ có `README.md` 0 byte) → tuần 7 chưa bắt đầu.
- Collector chất lượng khá (22 test pass, validate, dedupe, retry/backoff, chỉ lấy nến đã đóng), nhưng dùng **REST polling Binance Futures** (`/fapi/v1/klines`) thay vì Spot `/api/v3/klines` + WebSocket như kế hoạch.
- **Không có thành phần nào ghi vào `pipeline.market_data` / `pipeline.news_data`** trong ClickHouse → bảng đã tạo nhưng luôn rỗng; API sau này sẽ không có dữ liệu để đọc.
- **Không có Spark Structured Streaming**; chỉ có 1 job batch, và job này ghi `append` vào bảng `MergeTree` → chạy lại là nhân đôi dữ liệu.
- Lỗi mất dữ liệu: collector ghi file local **trước** khi gửi Kafka; nếu Kafka lỗi, bản ghi bị đánh dấu "đã có" và **không bao giờ được gửi lại** (`data-pipeline/collector/main.py:48-55`).
- Hợp đồng dữ liệu chỉ tồn tại ngầm trong `collector/models.py`; `raw-sink`, Spark và ClickHouse mỗi nơi tự giả định tên trường → cần chốt schema dùng chung **trước tiên**.
- PR đang mở ở `data-pipeline` (nhánh `fix/data-pipeline`) đã **bị thay thế** bởi commit `a7a0df4` trên `main` và đang xung đột → đề xuất đóng.
- Ưu tiên tiếp theo: (M0) chốt hợp đồng + quy trình → (M1–M3 song song) ingestion tin cậy, raw zone MinIO, Spark → ClickHouse → (M4–M5) API + Dashboard → (M6) kiểm thử E2E, đo độ trễ, demo.

## 2. Bảng hiện trạng

Ký hiệu: ✅ Xong · 🟡 Dở · ⬜ Chưa có · 🔴 Có vấn đề

| Tuần | Tính năng | Repo | Trạng thái | Ghi chú |
|---|---|---|---|---|
| 1 | Phạm vi BTCUSDT/ETHUSDT 1m | data-pipeline | ✅ | `.env.example:2-3` |
| 1 | Sơ đồ kiến trúc | — | ⬜ | Không có tài liệu kiến trúc ở repo nào |
| 1 | Định dạng JSON chung (schema) | data-pipeline | 🔴 | Chỉ có dataclass `collector/models.py:17-56`, không có JSON Schema dùng chung |
| 2 | Collector nến lịch sử REST (phân trang) | data-pipeline | ✅ | `collectors.py:138-170`, nhưng dùng Futures `/fapi/v1/klines` |
| 2 | Collector nến realtime (WebSocket kline 1m) | data-pipeline | ⬜ | Thay bằng polling REST mỗi 60 s (`main.py:110-115`) |
| 2 | Chỉ nhận nến đã đóng | data-pipeline | ✅ | `collectors.py:102-119` (lọc `close_time < now`) |
| 2 | Lấp khoảng trống sau mất kết nối | data-pipeline | 🟡 | Ngầm định nhờ `limit=100` (≤100 phút); không có gap-detection |
| 2 | Collector tin tức CoinDesk | data-pipeline | 🟡 | Chỉ RSS, không API key, không nguồn dự phòng |
| 2 | Ghi JSON mẫu theo ngày | data-pipeline | ✅ | `output.py:54-91` (`data/samples/<type>/yyyy/mm/dd.jsonl`) |
| 2 | Validate + dedupe | data-pipeline | ✅ | `validation.py`, `output.py:12-31` |
| 3 | Kafka (Docker Compose, KRaft) | infrastructure | ✅ | `docker-compose.yml:2-41` |
| 3 | Producer đẩy vào topic | data-pipeline | 🔴 | Có, nhưng lỗi mất dữ liệu khi Kafka down (xem C1) |
| 3 | Consumer kiểm tra message | infrastructure | 🟡 | `raw-sink` có, chưa có công cụ/kịch bản kiểm tra |
| 3 | Collector chạy trong Docker | data-pipeline | ⬜ | Không có `Dockerfile`, không có service trong compose |
| 4 | MinIO + raw sink theo ngày | infrastructure | 🟡 | `raw-sink/consumer.py`; mỗi bản ghi 1 object, key chứa `:` |
| 4 | Metadata (nguồn, thời điểm thu thập, schema version) | infrastructure | 🟡 | Có trong payload; thiếu metadata Kafka (topic/partition/offset, `ingested_at`) |
| 4 | Lập luận MinIO thay HDFS | — | ⬜ | Chưa có tài liệu |
| 5 | Bảng ClickHouse | infrastructure | 🟡 | `clickhouse/init/001_schema.sql`; `market_data` đúng ReplacingMergeTree |
| 5 | Nạp nến chuẩn hóa vào ClickHouse | — | ⬜ | **Không có ai ghi vào `market_data`/`news_data`** |
| 5 | Gộp 5 phút / MA20 | infrastructure | 🔴 | `batch_market_analysis.py`, ghi trùng khi chạy lại, nhãn `interval` sai |
| 6 | Spark Structured Streaming (volume bất thường) | — | ⬜ | Không có |
| 6 | Spark batch đọc MinIO qua `s3a://` | infrastructure | 🟡 | Có code, chưa chạy thử; nguy cơ lỗi key chứa `:` (H3) |
| 6 | News Impact (tùy chọn) | — | ⬜ | |
| 7 | API (nến, tin, phân tích) | backend-api | ⬜ | Repo trống |
| 7 | Dashboard | frontend-dashboard | ⬜ | Repo trống |
| 8 | E2E, kiểm thử lỗi, đo độ trễ, tài liệu | — | ⬜ | |
| — | Unit test | data-pipeline | ✅ | 22 test pass (`pytest -q`, 0.32 s) |
| — | Unit test | infrastructure | ⬜ | Không có test cho raw-sink / Spark job |
| — | CI | tất cả | ⬜ | Không có `.github/workflows` |

### 2.1 Hiện trạng từng repo

**data-pipeline** — Python 3.11 (`.venv` cục bộ), dependency `requests`, `python-dotenv`, `kafka-python` (đang cài 2.3.2), `pytest` (`requirements.txt:1-4`). Chạy: `python -m collector --source all [--once] [--send-kafka] [--start … --end …]` (README.md:5-35). Không có Dockerfile/Makefile/lint config.
- Git: 4 commit trên `main`; người đóng góp: tutruong272005 (3), NgHuan (1), hungdi272005-cmd (1 merge). Nhánh remote: `feat/Collector_Handle_data` (đã merge qua PR #1), `fix/data-pipeline` (PR đang mở, xem 2.2).
- Commit `a7a0df4` (NgHuan, 2026-10-26) được **đẩy thẳng lên `main`** không qua PR — trái quy trình nhóm; cần bật branch protection.

**infrastructure** — Docker Compose (Kafka 3.9.1 KRaft, MinIO Chainguard `latest`, ClickHouse 24.8, Spark 3.5.9 master+worker), `raw-sink` Python 3.13 (`boto3`, `kafka-python`), 1 job PySpark. `docker compose --env-file .env.example config -q` **hợp lệ**; `py_compile` 2 file Python **OK**.
- Git: 6 commit; tutruong272005 (5), hungdi272005-cmd (1 merge PR #1). Nhánh `feat/infrastructure` đã merge, còn tồn tại.
- File `.pyc` bị commit: `raw-sink/__pycache__/consumer.cpython-313.pyc`, `spark/jobs/__pycache__/batch_market_analysis.cpython-313.pyc`; `.gitignore:1-3` thiếu `__pycache__/`.

**backend-api**, **frontend-dashboard** — mỗi repo 1 commit "Initial commit", chỉ có `README.md` rỗng. Chưa chọn ngôn ngữ/framework.

### 2.2 Issue và PR đang mở

- **Không xem được trực tiếp**: máy chưa cài `gh` và repo là private (GitHub API trả 404 khi không xác thực). Số hiệu và nội dung issue **chưa xác minh** — cần một thành viên cài `gh` (`winget install GitHub.cli`, `gh auth login`) để bổ sung.
- **PR từ nhánh `fix/data-pipeline`** (commit `9b43c50`, tutruong272005, 2026-10-24, "fix: skip unclosed candles and normalize RSS timestamps"), suy ra từ nhánh remote:
  - Thay đổi: lọc nến chưa đóng bằng `row[6] < now_ms`, chuẩn hóa ngày RSS `-0000` về UTC, thêm 2 test.
  - Đánh giá: hướng sửa đúng, test pass trên nhánh (5/5). Tuy nhiên **cả hai thay đổi đã có trên `main`** dưới dạng tổng quát hơn (`collectors.py:102-119` `_closed_klines`, `collectors.py:173-178` `_parse_pub_date`, `now_ms` inject được để test ổn định), và nhánh **xung đột** với `main` (`git merge-tree` báo conflict). Test trong PR phụ thuộc đồng hồ thật nên có thể flaky.
  - Đề xuất: **đóng PR**, ghi chú "superseded by a7a0df4", ghi nhận đóng góp; nếu muốn giữ test RSS `-0000` thì kiểm tra `tests/test_collectors.py` trên `main` đã có tương đương chưa.

## 3. Hợp đồng dữ liệu — so sánh giữa các thành phần

Nguồn "thật" duy nhất hiện nay là `data-pipeline/collector/models.py:17-56`. Các nơi khác tự giả định:

| Trường / khía cạnh | Collector (`models.py`) | raw-sink (`consumer.py`) | Spark batch | ClickHouse (`001_schema.sql`) | Ghi chú |
|---|---|---|---|---|---|
| Thời gian | ISO-8601 string `…T08:00:00.000Z` (`models.py:6-10`) | parse ISO (`consumer.py:60`) | `to_timestamp` (`batch…py:45`) | `DateTime64(3,'UTC')` | Kế hoạch ghi "UTC, mili giây" — cần chốt **string ISO** hay **epoch ms (Int64)**. Nạp ISO có `Z` vào ClickHouse cần `date_time_input_format='best_effort'` (chưa xác minh với loader sẽ viết). |
| `source` | `binance_futures` / `coindesk` | không dùng | không dùng | market `String`, news `LowCardinality(String)` | Kiểu không đồng nhất; kế hoạch nói Spot (`binance`). |
| `trade_count` | `int` | — | `long` | `UInt64` | OK, nên ghi rõ "≥0". |
| `interval` | `"1m"` | dùng trong key | giữ nguyên `"1m"` cho nến **5 phút** (`batch…py:58,69`) | `analysis_results.interval` | **Lệch nghĩa**: kết quả 5m được gắn nhãn `1m`. |
| `symbols` (news) | list | — | — | `Array(String)` | OK |
| `sentiment` | `None` | — | — | `Nullable(String)` | OK nhưng chưa ai điền. |
| `schema_version` | `"1.0"` | không kiểm tra | không đọc | `LowCardinality(String)` | Không ai kiểm tra version khi đọc. |
| Envelope / metadata ingest | không có | không thêm | — | không có `ingested_at` | Thiếu dữ liệu để đo độ trễ tuần 8. |
| Khóa Kafka | `symbol` (market) / `article_id` (news) (`kafka.py:8-9`) | không dùng | — | — | Đúng yêu cầu. |
| Topic | `market_data`, `news_data` (`config.py:24-25`) | hard-code (`consumer.py:52,77-78`) | — | — | Hard-code 2 nơi. |

Kết luận: **chưa có hợp đồng dùng chung**; lệch về đơn vị thời gian (chưa chốt), nhãn `interval` của kết quả phân tích, kiểu `source`, và thiếu trường ingest để đo độ trễ.

## 4. Vấn đề phát hiện

### Nghiêm trọng

| # | Vấn đề | Vị trí | Đề xuất sửa |
|---|---|---|---|
| C1 | Bản ghi được ghi vào file local rồi mới gửi Kafka; nếu `publish` lỗi chỉ log, lần chạy sau dedupe theo file nên **không bao giờ gửi lại** → mất dữ liệu ở Kafka/MinIO/ClickHouse. | `data-pipeline/collector/main.py:46-55`, `output.py:54-91` | Gửi Kafka trước, chỉ ghi "đã gửi" khi future thành công; hoặc tách 2 trạng thái (file mẫu vs. checkpoint gửi). Dedupe cuối cùng giao cho ClickHouse `ReplacingMergeTree`. |
| C2 | Không có thành phần nào nạp dữ liệu vào `pipeline.market_data` / `pipeline.news_data` → API/Dashboard không có dữ liệu. | `infrastructure/clickhouse/init/001_schema.sql:3-41` (bảng tạo nhưng không có writer) | Viết Spark Structured Streaming Kafka → ClickHouse (M3). Không dùng ClickHouse Kafka Engine để tránh bỏ qua Spark. |
| C3 | Không có Spark Structured Streaming (yêu cầu cốt lõi tuần 6 của môn Big Data). | `infrastructure/spark/jobs/` chỉ có `batch_market_analysis.py` | Thêm `stream_market.py` có `checkpointLocation` trên `s3a://`, `withWatermark`, `foreachBatch` ghi ClickHouse. |
| C4 | Không có hợp đồng dữ liệu dùng chung; mỗi thành phần giả định tên trường riêng (bảng mục 3). | `data-pipeline/collector/models.py:17-56`, `infrastructure/raw-sink/consumer.py:51-61`, `spark/jobs/batch_market_analysis.py:42-53` | Tạo `contracts/` chứa JSON Schema (`market_kline.v1.json`, `news_article.v1.json`) + mẫu; CI validate mẫu và output collector (M0). |

### Cao

| # | Vấn đề | Vị trí | Đề xuất sửa |
|---|---|---|---|
| H1 | Bảng `analysis_results` dùng `MergeTree` và job `append` toàn bộ lịch sử mỗi lần chạy → **nhân đôi** kết quả. | `001_schema.sql:43-55`, `batch_market_analysis.py:117-127` | Đổi sang `ReplacingMergeTree(created_at) ORDER BY (result_type, symbol, interval, event_time)`; truy vấn với `FINAL`. |
| H2 | Không có WebSocket kline; realtime bằng polling REST 60 s ⇒ trễ tới ~60–120 s và tốn request. Không có reconnect cho WS (vì chưa có WS). | `data-pipeline/collector/main.py:107-115` | Thêm `ws_collector.py` (stream `<symbol>@kline_1m`, chỉ phát khi `k.x == true`, reconnect với backoff có jitter, ping/pong; khi reconnect gọi REST backfill từ `last_open_time + 1`). |
| H3 | Object key MinIO chứa `:` (vd `market/2026/10/22/BTCUSDT_1m_2026-10-22T08:00:00.000Z.json`). Hadoop `Path` thường lỗi `URISyntaxException: Relative path in absolute URI` với tên file có `:` → Spark `s3a://` có thể không đọc được (**chưa xác minh bằng chạy thật**). | `infrastructure/raw-sink/consumer.py:51-61` | Dùng epoch ms hoặc `%Y%m%dT%H%M%SZ` trong tên object. |
| H4 | Mỗi message = 1 object JSON nhỏ (~2.880 object/ngày/symbol); Spark đọc đệ quy hàng nghìn file nhỏ rất chậm (small-files problem). | `consumer.py:86-95`, `batch…py:37-41` | Gom theo lô (vd mỗi 1 phút hoặc 500 bản ghi) thành file NDJSON `market/yyyy/mm/dd/part-<offset>.jsonl`; hoặc để Spark streaming ghi raw. |
| H5 | `raw-sink` auto-commit offset và không bắt lỗi: message lỗi (thiếu `open_time`, JSON hỏng) làm process crash lặp lại; crash sau khi offset đã commit có thể mất bản ghi. | `consumer.py:76-95` | `enable_auto_commit=False`, commit sau `put_object`; `try/except` + topic DLQ (`market_data_dlq`). |
| H6 | Dùng Binance **Futures** (`fapi.binance.com/fapi/v1/klines`) trong khi kế hoạch là Spot `/api/v3/klines`. Giá/volume Futures khác Spot. | `data-pipeline/collector/collectors.py:129`, `.env.example:1`, `config.py:15` | Nhóm quyết định (mục 6). Nếu chọn Spot: base `https://api.binance.com`, path `/api/v3/klines`, limit tối đa 1000; đổi `source` thành `binance_spot`. |
| H7 | Mount `./spark/conf` vào `/opt/spark/conf/custom` — Spark không đọc thư mục này, nên `spark-defaults.conf` **không có hiệu lực** (job hiện tự set S3A trong code nên vẫn chạy). | `infrastructure/docker-compose.yml:120,147` | Mount vào `/opt/spark/conf/spark-defaults.conf` hoặc set `SPARK_CONF_DIR`. |
| H8 | Không có job/kịch bản nào ghi tin tức vào ClickHouse; nguồn tin chỉ RSS (≈ 25 bài mới nhất), không lấy được lịch sử, không có dự phòng. | `collectors.py:202-239`, `main.py:75-85` | Chốt nguồn: CoinDesk Data API (cần API key) hoặc giữ RSS + thêm 1 RSS dự phòng (vd Cointelegraph). Chạy news poll mỗi 5 phút. |

### Trung bình

| # | Vấn đề | Vị trí | Đề xuất sửa |
|---|---|---|---|
| M1 | Không phát hiện khoảng trống: sau khi mất kết nối > `BINANCE_LIMIT` phút (100) dữ liệu bị thiếu vĩnh viễn trừ khi chạy backfill tay. | `main.py:58-72`, `.env.example:4` | Lưu `last_open_time` theo symbol; khi chạy lại gọi `collect_klines_range(last+1m, now)`. |
| M2 | Tạo `KafkaProducer` mới cho mỗi lượt poll; chưa bật idempotence (`enable_idempotence` mặc định `False` trong kafka-python 2.3.2, đã kiểm tra `DEFAULT_CONFIG`). | `data-pipeline/collector/kafka.py:24-28` | Tạo producer một lần; `enable_idempotence=True`, `acks="all"`, `linger_ms=50`, `compression_type="gzip"`. |
| M3 | Nến 5 phút được tính cả cửa sổ chưa đủ 5 nến 1m (cửa sổ cuối đang dở) và MA20 cho 19 điểm đầu tính trên < 20 nến. | `batch_market_analysis.py:57-89` | Lọc `count(*) = 5`; chỉ phát MA20 khi `count over window = 20`. |
| M4 | Mật khẩu mặc định hard-code làm giá trị fallback trong code (giá trị mẫu, không phải secret thật). | `batch_market_analysis.py:17-18,122-123`, `consumer.py:22-23` | Bỏ fallback, fail-fast khi thiếu biến môi trường. |
| M5 | Image MinIO dùng tag `latest`, không cố định phiên bản → khó tái lập. | `docker-compose.yml:44` | Pin digest hoặc dùng `minio/minio:RELEASE.<date>`. |
| M6 | Collector chưa đóng gói Docker; trạng thái dedupe nằm ở file local `data/samples` → container mất trạng thái khi recreate. | `data-pipeline` (không có `Dockerfile`), `output.py:54-91` | Thêm `Dockerfile` + service `collector` trong compose, mount volume `collector_data`. |
| M7 | Thiếu cột `ingested_at` để đo độ trễ thu thập → truy vấn (yêu cầu tuần 8). | `001_schema.sql:3-41` | Thêm `ingested_at DateTime64(3,'UTC') DEFAULT now64(3)`. |
| M8 | `Settings` đọc biến môi trường lúc import (giá trị mặc định của class attribute) → khó test/override. | `data-pipeline/collector/config.py:13-26` | Dùng `field(default_factory=…)` hoặc `Settings.from_env()`. |
| M9 | `main` không được bảo vệ; commit `a7a0df4` đẩy thẳng lên `main`. | lịch sử git `data-pipeline` | Bật branch protection: bắt buộc PR + 1 review + CI xanh. |

### Thấp

| # | Vấn đề | Vị trí | Đề xuất sửa |
|---|---|---|---|
| L1 | File `.pyc` bị commit; `.gitignore` thiếu `__pycache__/`. | `infrastructure/.gitignore:1-3`, `raw-sink/__pycache__/`, `spark/jobs/__pycache__/` | `git rm -r --cached **/__pycache__`, thêm vào `.gitignore`. |
| L2 | README `backend-api`, `frontend-dashboard` rỗng. | `README.md` (0 byte) | Ghi stack đã chọn + cách chạy. |
| L3 | Không có lint/format (ruff/black) và CI ở cả 4 repo. | — | Xem mục 5.4. |
| L4 | `source` kiểu `String` ở `market_data` nhưng `LowCardinality(String)` ở `news_data`. | `001_schema.sql:6,28` | Thống nhất `LowCardinality(String)`. |
| L5 | Topic name hard-code trong raw-sink. | `consumer.py:52,77-78` | Đọc từ env như collector. |
| L6 | Kafka healthcheck dùng `localhost:9092` (listener EXTERNAL) — chạy được nhưng nên dùng listener nội bộ. | `docker-compose.yml:24` | Dùng `kafka:29092`. |

### Các điểm kỹ thuật đã kiểm tra — tóm tắt

- **Binance**: chỉ nến đã đóng ✅ (`collectors.py:114`, theo `close_time` vì REST không có `k.x`); UTC ms ✅ (`models.py:6-10`); giá/volume parse `float` ✅ (`collectors.py:91-97`), validate hữu hạn/dương ✅; backfill REST có phân trang ✅ nhưng là Futures, limit 1000 (Futures cho tới 1500) (`config.py:20`); retry 3 lần, backoff `2^(n-1)` s, tôn trọng `Retry-After` với 418/429 ✅ (`collectors.py:45-73`); **không** có WebSocket/reconnect ⬜; **không** tự lấp khoảng trống ⬜.
- **CoinDesk**: RSS `https://www.coindesk.com/arc/outboundfeeds/rss/` (`.env.example:6`); không API key; rate-limit chỉ qua retry 429 chung; không có dự phòng.
- **Kafka**: KRaft ✅ (`docker-compose.yml:10-11`); key = `symbol` ✅; `acks="all"` ✅, idempotence ⬜; 3 partition, RF 1 (`docker-compose.yml:38-39`).
- **MinIO/HDFS**: lưu raw ✅, partition `market/yyyy/mm/dd/` ✅; Spark `s3a://` có code nhưng chưa chạy, nguy cơ H3/H4; chưa có lập luận MinIO thay HDFS.
- **Spark**: batch có nhưng ghi trùng (H1); streaming chưa có (C3). ClickHouse không đọc Kafka trực tiếp (không có Kafka Engine) — tốt, nhưng hiện cũng không có gì nạp ClickHouse (C2).
- **ClickHouse**: `market_data` `ReplacingMergeTree(collected_at) ORDER BY (symbol, interval, open_time)` ✅ đúng khuyến nghị; `news_data` `ReplacingMergeTree(collected_at) ORDER BY (source, article_id)` ✅; `analysis_results` `MergeTree` 🔴 (H1).
- **API/Dashboard**: chưa có gì.
- **Hạ tầng**: port host 9092, 9000, 9001, 8123, 9003, 7077, 8080, 8081 — **không trùng**; API sau này **tránh 8080** (Spark UI), dùng 8000; frontend 5173. `.env.example` có ở 2 repo ✅. Không tìm thấy secret thật bị commit (đã quét tên file `.env`/`.pem`/`.key` và mẫu khóa AWS/GitHub/OpenAI trong toàn bộ lịch sử 2 repo có code).
- **RAM ước tính** (chưa đo thực tế): Kafka ~1–1.2 GB (heap mặc định 1 GB), MinIO ~0.3–0.5 GB, ClickHouse ~0.5–1.5 GB, Spark master ~0.5–1 GB, worker daemon ~0.5–1 GB + executor 2 GB (`SPARK_WORKER_MEMORY`), driver mỗi job ~1 GB, raw-sink/collector/API ~0.3 GB, frontend dev ~0.3 GB ⇒ **~7–9 GB** khi chạy đủ stack + 1 streaming job. Máy 16 GB chạy được nếu giới hạn WSL2 (`.wslconfig` `memory=10GB`); máy 8 GB cần profile rút gọn.
- **Chất lượng**: test chỉ ở collector; logging có cấu trúc cơ bản; xử lý lỗi tốt ở collector, yếu ở raw-sink; trùng lặp code: logic parse ISO/khóa bản ghi lặp ở `output.py:42-51`, `main.py:39-43`, `consumer.py:51-61` — sẽ giảm khi có module contract dùng chung.

## 5. Lộ trình triển khai

Vai trò: **A** Ingestion · **B** Data Eng (Spark) · **C** Backend & Storage (ClickHouse, MinIO, API) · **D** Frontend & Integration. Ước lượng: S ≤ 0,5 ngày, M 1–2 ngày, L 3–5 ngày. Ký hiệu ⫽ = làm song song được.

### M0 — Chốt hợp đồng và quy trình (tuần 3, 2–3 ngày) — **làm đầu tiên**

Mục tiêu: một nguồn sự thật cho schema; mọi repo có quy trình PR + CI.

| ID | Task | Repo / file | Ai | Phụ thuộc | Công |
|---|---|---|---|---|---|
| M0.1 | Viết JSON Schema `market_kline.v1.json`, `news_article.v1.json`, `analysis_result.v1.json` + 3 file mẫu; chốt: thời gian ISO-8601 UTC ms `Z` (hoặc epoch ms), `source` enum, thêm `ingested_at` | `infrastructure/contracts/` | A + C | — | M |
| M0.2 | Sơ đồ kiến trúc + quyết định MinIO vs HDFS (ADR ngắn) | `infrastructure/docs/architecture.md`, `docs/adr/001-object-storage.md` | B | — | S |
| M0.3 | Đặc tả API (OpenAPI) cho `/candles`, `/news`, `/indicators`, `/signals`, `/health` | `backend-api/openapi.yaml` | C + D | M0.1 | M |
| M0.4 | Bật branch protection `main` 4 repo; đóng PR `fix/data-pipeline` (superseded); xóa nhánh đã merge | GitHub settings | A | — | S |
| M0.5 | `.gitignore` thêm `__pycache__/`, gỡ `.pyc` | `infrastructure/.gitignore` | C | — | S |

Tiêu chí hoàn thành:
```bash
# schema hợp lệ và mẫu khớp schema
pip install check-jsonschema && check-jsonschema --schemafile contracts/market_kline.v1.json contracts/examples/market_kline.json
# output collector khớp schema
check-jsonschema --schemafile ../infrastructure/contracts/market_kline.v1.json data/samples/market/2026/10/27.jsonl   # (chuyển jsonl→json array hoặc dùng script validate)
git ls-files | grep -c __pycache__   # = 0
```

### M1 — Ingestion tin cậy (tuần 3–4) — chủ yếu A ⫽ M2, M3, M4-skeleton

| ID | Task | Repo / file | Ai | Phụ thuộc | Công |
|---|---|---|---|---|---|
| M1.1 | Sửa C1: gửi Kafka thành công rồi mới ghi checkpoint | `data-pipeline/collector/main.py`, `output.py` | A | — | S |
| M1.2 | Producer dùng lại + `enable_idempotence=True` | `collector/kafka.py` | A | — | S |
| M1.3 | Quyết định Spot/Futures (H6) và sửa base URL/path/source | `collectors.py`, `config.py`, `.env.example` | A | Câu hỏi Q1 | S |
| M1.4 | WebSocket kline 1m, chỉ phát `k.x == true`, reconnect backoff + jitter | `collector/ws_collector.py` (mới) | A | M1.2 | M |
| M1.5 | Gap-fill khi khởi động/reconnect: REST `[last_open+1m, now)` | `collector/main.py`, `collectors.py` | A | M1.4 | M |
| M1.6 | Validate theo JSON Schema dùng chung | `collector/validation.py` | A | M0.1 | S |
| M1.7 | `Dockerfile` + service `collector` (profile `ingest`) | `data-pipeline/Dockerfile`, `infrastructure/docker-compose.yml` | A + C | — | S |
| M1.8 | Nguồn tin: chốt CoinDesk API key hoặc RSS + RSS dự phòng | `collectors.py` | A | Câu hỏi Q3 | M |

Tiêu chí:
```bash
pytest -q                                   # có test mới cho C1, WS k.x, gap-fill
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
  --topic market_data --from-beginning --max-messages 5 --property print.key=true   # key = BTCUSDT/ETHUSDT
# kịch bản lỗi: docker compose stop kafka; chờ 3 phút; docker compose start kafka → không mất nến (kiểm bằng truy vấn gap ở M3)
```

### M2 — Raw zone MinIO (tuần 4) — C ⫽ M1, M3

| ID | Task | Repo / file | Ai | Phụ thuộc | Công |
|---|---|---|---|---|---|
| M2.1 | raw-sink: commit thủ công sau `put_object`, try/except + DLQ topic | `infrastructure/raw-sink/consumer.py` | C | — | M |
| M2.2 | Envelope metadata: `{topic, partition, offset, ingested_at, schema_version, payload}` | `consumer.py` | C | M0.1 | S |
| M2.3 | Ghi theo lô NDJSON, tên object không chứa `:` (H3, H4) | `consumer.py` | C | — | M |
| M2.4 | Sửa mount `spark-defaults.conf` (H7), pin image MinIO (M5) | `docker-compose.yml` | C | — | S |
| M2.5 | Test unit `record_key`/batching | `infrastructure/raw-sink/tests/` | C | — | S |

Tiêu chí:
```bash
docker compose run --rm --entrypoint sh minio-mc -c "mc ls -r local/raw-data/market/$(date -u +%Y/%m/%d)/ | head"   # (thêm service mc hoặc dùng console :9001)
docker compose exec spark-worker /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/jobs/smoke_read_raw.py         # in ra count > 0 khi đọc s3a://raw-data/market/
```

### M3 — Spark → ClickHouse (tuần 5–6) — B (+C cho schema) ⫽ M1, M2

| ID | Task | Repo / file | Ai | Phụ thuộc | Công |
|---|---|---|---|---|---|
| M3.1 | Schema: thêm `ingested_at`, `analysis_results` → `ReplacingMergeTree`, thêm bảng `candles_5m` / `volume_anomalies` | `clickhouse/init/002_*.sql` | C | M0.1 | S |
| M3.2 | **Streaming** Kafka → parse theo schema → `withWatermark("open_ts","2 minutes")` → `foreachBatch` ghi `market_data`, checkpoint `s3a://checkpoints/market` | `infrastructure/spark/jobs/stream_market.py` | B | M0.1, M3.1 | L |
| M3.3 | Streaming phát hiện volume bất thường (z-score / > k·MA20 volume, cửa sổ trượt) → `volume_anomalies` | `spark/jobs/stream_market.py` hoặc `stream_anomaly.py` | B | M3.2 | M |
| M3.4 | Batch: 5m OHLCV + MA20 đúng (lọc đủ nến, `interval='5m'`), idempotent | `spark/jobs/batch_market_analysis.py` | B | M3.1 | M |
| M3.5 | Streaming news → `news_data` | `spark/jobs/stream_news.py` | B | M3.2 | S |
| M3.6 | Script `make submit-stream` / `scripts/submit.ps1` | `infrastructure/scripts/` | B | — | S |

Tiêu chí (ClickHouse, cổng 8123):
```sql
SELECT symbol, count() AS n, uniqExact(open_time) AS u FROM pipeline.market_data FINAL GROUP BY symbol;  -- n = u
-- không có khoảng trống > 1 phút trong 24h gần nhất
SELECT symbol, open_time, prev FROM (
  SELECT symbol, open_time, lagInFrame(open_time) OVER (PARTITION BY symbol ORDER BY open_time) AS prev
  FROM pipeline.market_data FINAL WHERE open_time > now() - INTERVAL 1 DAY)
WHERE prev > toDateTime64(0,3) AND dateDiff('second', prev, open_time) > 60;   -- 0 dòng
-- chạy batch 2 lần, số dòng không đổi
SELECT count() FROM pipeline.analysis_results FINAL WHERE result_type = '5m_ma20';
```

### M4 — API (tuần 6–7) — C ⫽ M5

| ID | Task | Repo / file | Ai | Phụ thuộc | Công |
|---|---|---|---|---|---|
| M4.1 | Khung FastAPI (đề xuất) + `clickhouse-connect`, `/health`, Dockerfile, `.env.example`, port 8000 | `backend-api/app/` | C | M0.3 | M |
| M4.2 | `GET /candles?symbol=&interval=1m|5m&from=&to=&limit=` | `app/routes/candles.py` | C | M4.1 | M |
| M4.3 | `GET /news?symbol=&limit=`, `GET /indicators?symbol=&type=ma20`, `GET /signals?symbol=` (volume anomaly), `GET /status` (thời điểm cập nhật gần nhất, lag) | `app/routes/` | C | M4.1 | M |
| M4.4 | Test với ClickHouse thật (docker) hoặc fixture | `backend-api/tests/` | C | M4.2 | S |

Tiêu chí:
```bash
curl "http://localhost:8000/candles?symbol=BTCUSDT&interval=1m&limit=5"   # 5 phần tử, khớp OpenAPI
curl "http://localhost:8000/status"                                       # last_open_time trong 2 phút gần nhất
```

### M5 — Dashboard tối thiểu (tuần 6–7) — D ⫽ M3, M4 (dùng mock theo OpenAPI)

| ID | Task | Repo / file | Ai | Phụ thuộc | Công |
|---|---|---|---|---|---|
| M5.1 | Khung Vite + React + TS; client sinh từ `openapi.yaml`; mock server (Prism) | `frontend-dashboard/` | D | M0.3 | M |
| M5.2 | Biểu đồ nến (lightweight-charts) + MA20, chọn symbol/interval | `src/components/CandleChart.tsx` | D | M5.1 | M |
| M5.3 | Danh sách tin, panel tín hiệu volume bất thường, "cập nhật lúc …" | `src/components/` | D | M5.1 | M |
| M5.4 | Chuyển từ mock sang API thật qua `VITE_API_URL` | `.env.example` | D | M4.3 | S |

Tiêu chí: `npm run build` thành công; mở `http://localhost:5173`, tab Network chỉ gọi `VITE_API_URL` (không mock) và thấy nến cập nhật trong ≤ 2 phút.

### M6 — E2E, kiểm thử lỗi, đo lường, demo (tuần 8) — cả nhóm

| ID | Task | Ai | Công |
|---|---|---|---|
| M6.1 | `docker compose --profile full up` chạy toàn bộ; script `scripts/e2e_check.sh` gọi các truy vấn ở M1–M4 | D | M |
| M6.2 | Kịch bản lỗi: tắt mạng collector 5 phút, restart Kafka, gửi lại bản ghi trùng, gửi bản ghi thiếu trường → kết quả kỳ vọng ghi trong `docs/failure-tests.md` | A + B | M |
| M6.3 | Đo độ trễ: `SELECT quantiles(0.5,0.95)(dateDiff('millisecond', close_time, ingested_at)) FROM pipeline.market_data WHERE open_time > now() - INTERVAL 1 HOUR` | C | S |
| M6.4 | Thống kê số bản ghi theo nguồn/ngày, MinIO vs ClickHouse | C | S |
| M6.5 | Tài liệu, slide, kịch bản demo | cả nhóm | M |

### 5.1 Sơ đồ song song

```
Tuần 3   M0 (A+C schema, B ADR, C+D OpenAPI) ───────────┐
Tuần 4   A: M1 ingestion    C: M2 raw-sink    B: M3.1-3.2 streaming (dùng data/samples)   D: M5.1 khung + mock
Tuần 5   A: M1.4-1.5 WS/gap  C: M4.1-4.2 API   B: M3.3-3.4 anomaly + batch                 D: M5.2 biểu đồ
Tuần 6   A: M1.8 news + M3.5 hỗ trợ B         C: M4.3 API        B: hoàn thiện M3      D: M5.3
Tuần 7   tích hợp: D M5.4 nối API thật; A+B chạy dài 24h; C tối ưu truy vấn
Tuần 8   M6 cả nhóm
```
Điểm khóa duy nhất là **M0.1 + M0.3** (≤ 3 ngày). Sau đó B dùng file `data/samples/*.jsonl` đẩy vào Kafka để phát triển streaming mà không chờ WebSocket; D dùng mock từ OpenAPI; C dùng fixture CSV nạp ClickHouse.

### 5.2 Rủi ro và giảm thiểu

| Rủi ro | Mức | Giảm thiểu |
|---|---|---|
| Nguồn tin: RSS ít bài, không có lịch sử; API key CoinDesk có giới hạn/đổi điều khoản | Cao | Chốt sớm (Q3); RSS + 1 RSS dự phòng; cache kết quả; News Impact để tùy chọn |
| Tài nguyên máy (~7–9 GB) | Cao | Compose profiles: `core` (Kafka, MinIO, ClickHouse), `spark`, `ingest`, `app`; Spark executor 1 GB khi dev; giới hạn `.wslconfig`; 1 máy mạnh làm "máy demo" |
| Binance chặn theo vùng/IP hoặc rate-limit 429/418 | Trung bình | Đã có backoff; dùng WebSocket giảm request; có data mẫu để demo offline |
| Tích hợp giữa repo lệch schema | Cao | Contract trong `infrastructure/contracts`, CI validate ở mỗi repo; tăng `schema_version` khi đổi |
| Spark connector (hadoop-aws, clickhouse-jdbc) lệch phiên bản, tải Maven chậm | Trung bình | Bake jar vào image Spark; ghi rõ phiên bản; thử sớm ở M2 (smoke read) |
| Giảng viên yêu cầu HDFS | Trung bình | ADR M0.2; phương án dự phòng: thêm `namenode`/`datanode` (bde2020/hadoop 3.x) và đổi `s3a://` → `hdfs://` — Spark code chỉ đổi đường dẫn |
| Chưa từng chạy E2E | Cao | Chạy "thin slice" (BTCUSDT → Kafka → MinIO → ClickHouse → `curl`) ngay cuối tuần 4 |

### 5.3 Chiến lược nhánh và PR

- Trunk-based: `main` được bảo vệ; nhánh ngắn `feat/…`, `fix/…`, `docs/…`, `chore/…`, `test/…`, sống ≤ 3 ngày.
- Một nhánh = một task ID ở trên (vd `feat/m1-4-ws-kline`); thay đổi nhiều repo dùng cùng tên nhánh và liên kết chéo PR.
- PR ≤ ~400 dòng, Conventional Commits, mẫu PR: Mục đích / Thay đổi chính / Cách kiểm thử / Ảnh hưởng. 1 reviewer bắt buộc, ưu tiên người ở vai trò kế tiếp trong luồng dữ liệu (A→B, B→C, C→D, D→A).
- Rebase lên `main` trước khi merge; `git push --force-with-lease`; squash merge.
- Đồng bộ ngắn 15 phút, 2 lần/tuần; theo dõi bằng GitHub Projects với các task ID trên.

### 5.4 CI tối thiểu (GitHub Actions)

- `data-pipeline`: `ruff check .` + `pytest -q` + validate `contracts/examples` (Python 3.11).
- `infrastructure`: `docker compose --env-file .env.example config -q`; `ruff check raw-sink spark/jobs`; `pytest raw-sink/tests`; `check-jsonschema` cho `contracts/`.
- `backend-api`: `ruff` + `pytest` (ClickHouse service container cho test tích hợp).
- `frontend-dashboard`: `npm ci && npm run lint && npm run build`.
- Tùy chọn tuần 7: workflow nightly chạy `docker compose --profile core up` + `scripts/e2e_check.sh` với dữ liệu mẫu.

## 6. Câu hỏi cần nhóm quyết định

1. **Q1 — Binance Spot hay Futures?** Code đang dùng Futures (`fapi`), kế hoạch ghi Spot `/api/v3/klines`. Đề xuất: Spot (đúng kế hoạch, dữ liệu phổ biến hơn).
2. **Q2 — Định dạng thời gian trong hợp đồng**: giữ ISO-8601 string (`…000Z`, hiện tại) hay epoch ms `Int64`? Đề xuất: giữ ISO cho payload, thêm `open_time_ms` nếu Spark/ClickHouse cần.
3. **Q3 — Nguồn tin tức**: xin API key CoinDesk Data API hay giữ RSS + RSS dự phòng? Ai chịu trách nhiệm key và nơi lưu (`.env`, không commit)?
4. **Q4 — MinIO hay HDFS** cho phần lưu trữ thô, có cần hỏi giảng viên? Nếu bắt buộc Hadoop: chạy HDFS song song hay thay hẳn?
5. **Q5 — Đường nạp ClickHouse**: xác nhận dùng Spark Structured Streaming `foreachBatch` (khuyến nghị cho môn học) thay vì ClickHouse Kafka Engine.
6. **Q6 — Raw zone**: raw-sink Python riêng (hiện tại) hay để Spark streaming ghi raw lên MinIO luôn (bớt 1 service)?
7. **Q7 — Stack backend/frontend**: FastAPI + React/Vite + lightweight-charts có phù hợp kỹ năng nhóm không?
8. **Q8 — Hợp đồng đặt ở đâu**: `infrastructure/contracts/` (đề xuất) hay tạo repo `contracts` riêng / git submodule?
9. **Q9 — Máy demo**: máy nào chạy đủ stack (RAM ≥ 16 GB)? Có cần profile rút gọn cho máy 8 GB?
10. **Q10 — Issue đang mở ở `data-pipeline`**: chưa xem được do thiếu `gh`/quyền; cần người có quyền đối chiếu issue với các task ở mục 5.
