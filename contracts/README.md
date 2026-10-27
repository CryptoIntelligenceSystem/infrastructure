# Hợp đồng dữ liệu chung (contracts)

Nguồn sự thật duy nhất cho định dạng dữ liệu trao đổi giữa các repo `data-pipeline`, `infrastructure`, `backend-api`, `frontend-dashboard`. Repo khác **không tự định nghĩa lại** các schema này; nếu cần dùng, chép bản sao bằng script đồng bộ và có test phát hiện bản sao bị lệch (so với `v1/SHA256SUMS`).

Phiên bản hiện tại: **v1** (`v1/VERSION`).

## 1. Kafka

| Topic | Message key | Value | Producer | Consumer |
|---|---|---|---|---|
| `market_data` | `symbol` (UTF-8, vd `BTCUSDT`) | [`v1/market_kline.schema.json`](v1/market_kline.schema.json) | data-pipeline collector | Spark streaming |
| `news_data` | `article_id` | [`v1/news_article.schema.json`](v1/news_article.schema.json) | data-pipeline collector | Spark streaming |

- Value là JSON UTF-8. Mỗi message market là **một nến 1 phút đã đóng**.
- Khóa duy nhất: market `(source, symbol, interval, open_time)`; news `(source, article_id)`. Consumer phải chịu được message lặp (at-least-once).

## 2. Thời gian

- Mọi trường thời gian là chuỗi **ISO-8601 UTC, đúng 3 chữ số mili giây, hậu tố `Z`**: `2026-10-27T08:00:00.000Z`.
- Spark: đặt `spark.sql.session.timeZone=UTC` và parse bằng format tường minh `yyyy-MM-dd'T'HH:mm:ss.SSSX`.
- ClickHouse: lưu `DateTime64(3, 'UTC')`.
- `open_time`/`close_time` là thời điểm của nến (Binance), `collected_at` là lúc collector nhận dữ liệu.

## 3. Quy tắc nghiệp vụ ngoài JSON Schema

JSON Schema không diễn tả được các ràng buộc sau; consumer (Spark validate) phải kiểm tra thêm:

- `low ≤ open ≤ high`, `low ≤ close ≤ high`
- `close_time > open_time` (với `1m`: `close_time = open_time + 59.999 s`)
- giá và volume là số hữu hạn (không NaN/Infinity)

Bản ghi vi phạm không bị bỏ lặng lẽ: Spark ghi vào bảng dead-letter `pipeline.market_data_rejects`.

## 4. Vùng dữ liệu thô (MinIO, truy cập qua `s3a://`)

| Dữ liệu | Đường dẫn | Phân vùng theo |
|---|---|---|
| Market thô | `s3a://raw-data/market/year=YYYY/month=M/day=D/` | `open_time` (UTC) |
| News thô | `s3a://raw-data/news/year=YYYY/month=M/day=D/` | `published_at` (UTC) |
| Checkpoint streaming | `s3a://checkpoints/<job>/` | mỗi job một thư mục |

- Phân vùng kiểu Hive, giá trị số nguyên không đệm số 0 (vd `year=2026/month=1/day=5`) để Spark tự nhận cột phân vùng và lọc theo ngày.
- Mỗi dòng là một [`v1/raw_record.schema.json`](v1/raw_record.schema.json): metadata Kafka (`topic`, `partition`, `offset`, `kafka_timestamp`, `key`), `value` là nguyên văn message, và `ingested_at`.
- Message không parse được thời gian để phân vùng vẫn được lưu, vào `year=0/month=0/day=0/`.

## 5. ClickHouse

Schema bảng nằm ở [`../clickhouse/`](../clickhouse/) (database `pipeline`). Quy ước:

- Bảng dữ liệu đã xử lý dùng `ReplacingMergeTree` với `ORDER BY` là khóa duy nhất, để chạy lại/khởi động lại không sinh bản trùng; truy vấn đọc dùng `FINAL`.
- Bảng nến 1 phút: `market_data` `ORDER BY (symbol, interval, open_time)`.
- Bảng tín hiệu: `signals` `ORDER BY (symbol, signal_type, open_time)`.

## 6. Quy tắc thay đổi

- Thêm trường optional, nới ràng buộc → tăng minor trong `v1/VERSION`, giữ `schema_version: "1.0"` trong message.
- Đổi tên/xóa trường, đổi kiểu, siết ràng buộc → tạo `v2/`, chạy song song tới khi mọi consumer chuyển.
- Mọi thay đổi file trong `v1/` phải cập nhật `v1/SHA256SUMS` (test sẽ fail nếu quên) và có ví dụ hợp lệ/không hợp lệ trong `v1/examples/`.
- PR sửa `contracts/` cần review của người phụ trách producer và consumer liên quan.

## 7. Kiểm tra

```bash
pip install -r contracts/requirements-dev.txt
pytest -q contracts/tests
python contracts/tests/test_contracts.py --update-sums   # sau khi sửa schema
```
