# infrastructure — Lộ trình triển khai

> Tham chiếu: [PROJECT_REVIEW.md](PROJECT_REVIEW.md) (hợp đồng chung v1, lịch tích hợp, câu hỏi mở). Lịch: tuần 1 = 19–25/10/2026.
> Repo này **chứa hợp đồng chung** (`contracts/v1/`) — mọi repo khác tham chiếu tới đây.
> Trích dẫn `file:dòng` theo `main@47ba79a`.

## 1. Mục tiêu cuối (khi demo)

- `contracts/v1/` là nguồn sự thật cho message Kafka, raw envelope, OpenAPI, biến môi trường/port.
- `docker compose --profile full up -d` dựng toàn bộ stack (Kafka, MinIO, ClickHouse, Spark, raw-sink, collector, API, FE) trên máy 16 GB.
- raw-sink ghi NDJSON theo lô vào `s3a://raw-data/{market,news}/yyyy/mm/dd/`, at-least-once, có DLQ.
- Spark Structured Streaming nạp `market_data`, `news_data`, `signals` (volume bất thường) vào ClickHouse có checkpoint + watermark; Spark batch đọc MinIO tính `candles_5m`, `indicators` (MA20), chạy lại không trùng.
- `scripts/e2e_check.sh` kiểm tra toàn tuyến; `docs/` có kiến trúc, ADR MinIO/HDFS, báo cáo E2E (độ trễ, số bản ghi, kiểm thử lỗi).

## 2. Hiện trạng

- Compose: Kafka 3.9.1 KRaft (3 partition/topic), MinIO Chainguard `latest`, ClickHouse 24.8, Spark 3.5.9 master + worker, raw-sink Python 3.13. `docker compose --env-file .env.example config -q` hợp lệ.
- ClickHouse: `market_data`, `news_data` đúng `ReplacingMergeTree`, nhưng **không có writer**; `analysis_results` là `MergeTree` → trùng khi chạy lại.
- Spark: 1 job batch (5m + MA20) chưa chạy thử; **không có streaming**.
- raw-sink: 1 object/bản ghi, tên có `:`, auto-commit, không bắt lỗi.
- Không test, không CI, `.pyc` bị commit. Issue liên quan: `data-pipeline#2` (chưa làm Spark).

## 3. Kiến trúc và cấu trúc thư mục đề xuất (chỉ phần thay đổi)

```
contracts/                       # MỚI — hợp đồng chung v1 (PROJECT_REVIEW mục 3)
  README.md, v1/{*.schema.json, openapi.yaml, env-and-ports.md, examples/, VERSION}
clickhouse/
  init/001_schema.sql            # giữ
  init/002_v1_tables.sql         # MỚI — ingested_at, candles_5m, indicators, signals
  fixtures/*.jsonl               # MỚI — dữ liệu mẫu cho API/FE test
  scripts/load_fixtures.sh       # MỚI
raw-sink/
  consumer.py → raw_sink/{app.py, batching.py, keys.py}   # tách để test được
  tests/                         # MỚI
spark/
  Dockerfile                     # bake jar hadoop-aws, aws-sdk-bundle, spark-sql-kafka, clickhouse-jdbc
  jobs/common/{schemas.py, clickhouse.py, s3.py}           # MỚI — dùng chung giữa các job
  jobs/stream_market.py, stream_news.py, stream_anomaly.py # MỚI
  jobs/batch_market_analysis.py  # sửa: ghi candles_5m + indicators, idempotent
  tests/                         # MỚI — test hàm thuần bằng pyspark local
scripts/                         # MỚI — submit.sh/ps1, e2e_check.sh, failure_tests.md
docs/                            # architecture.md, adr/001-object-storage.md, E2E_REPORT.md
.github/workflows/ci.yml         # MỚI
```

Lý do: hợp đồng đặt cạnh hạ tầng vì schema ClickHouse và topic đã ở đây; tách raw-sink/Spark thành module nhỏ để unit test; bake jar vào image để không phụ thuộc Maven lúc demo; compose profiles để máy yếu chỉ chạy phần cần thiết.

## 4. Milestone

### INF-M0 — Hợp đồng v1 và vệ sinh repo
- **Tuần**: 2 · **Phụ trách**: C (chủ trì), D (OpenAPI, CI), cả nhóm review
- **Phụ thuộc**: không. **Chặn**: mọi milestone khác của cả 4 repo.
- **Mock**: không cần.

| Task | File | Mô tả | Công | Nhánh |
|---|---|---|---|---|
| INF-M0.1 | `contracts/v1/market_kline.schema.json`, `news_article.schema.json`, `raw_envelope.schema.json`, `examples/` | JSON Schema 2020-12 theo PROJECT_REVIEW 3.3–3.5; mỗi schema 1 ví dụ hợp lệ + 1 lỗi | M | `docs/contracts-v1` |
| INF-M0.2 | `contracts/v1/openapi.yaml` | OpenAPI 3.1 theo PROJECT_REVIEW 3.7, có ví dụ response | M | `docs/contracts-v1-openapi` |
| INF-M0.3 | `contracts/v1/env-and-ports.md`, `.env.example` | Bảng biến/port 3.8; thêm `MINIO_RAW_BUCKET`, `MINIO_CHECKPOINT_BUCKET` | S | `docs/contracts-v1` |
| INF-M0.4 | `.gitignore`, `raw-sink/__pycache__`, `spark/jobs/__pycache__` | Thêm `__pycache__/`, `git rm --cached` các `.pyc` | S | `chore/gitignore-pycache` |
| INF-M0.5 | `.github/workflows/ci.yml` | `compose config`, `check-jsonschema` cho ví dụ, `ruff` | S | `chore/ci-infrastructure` |

**Hoàn thành khi**:
```bash
pip install check-jsonschema
check-jsonschema --schemafile contracts/v1/market_kline.schema.json contracts/v1/examples/market_kline.valid.json   # pass
! check-jsonschema --schemafile contracts/v1/market_kline.schema.json contracts/v1/examples/market_kline.invalid.json  # fail như mong đợi
npx @redocly/cli lint contracts/v1/openapi.yaml
git ls-files | grep -c __pycache__        # 0
```

### INF-M1 — Compose ổn định và profiles
- **Tuần**: 3 · **Phụ trách**: D
- **Phụ thuộc**: INF-M0.3 (tên biến). Sang repo khác: service `collector` cần `data-pipeline/Dockerfile` (DP-M4) — trước đó để service ở trạng thái build từ `../data-pipeline` và chỉ bật trong profile `ingest`.
- **Mock**: chưa có API/FE image → profile `app` để trống, thêm sau ở INF-M6.

| Task | File | Mô tả | Công | Nhánh |
|---|---|---|---|---|
| INF-M1.1 | `docker-compose.yml` | Profiles `core` (kafka, kafka-init, minio, mc-init, clickhouse), `spark`, `ingest` (raw-sink, collector), `app` (api, fe), `full` | M | `chore/compose-profiles` |
| INF-M1.2 | `docker-compose.yml:120,147` | Sửa mount `spark-defaults.conf` vào `/opt/spark/conf/` (H7) | S | `fix/spark-conf-mount` |
| INF-M1.3 | `docker-compose.yml:44` | Pin image MinIO; thêm service `mc-init` tạo bucket `raw-data`, `checkpoints` | S | `chore/compose-profiles` |
| INF-M1.4 | `docker-compose.yml:38-40` | `kafka-init` tạo thêm `market_data_dlq`, `news_data_dlq`, đặt `retention.ms=604800000` | S | `chore/compose-profiles` |
| INF-M1.5 | `docker-compose.yml` | Healthcheck ClickHouse (`/ping`), MinIO (`/minio/health/live`); `mem_limit` mỗi service; healthcheck Kafka dùng `kafka:29092` (L6) | S | `chore/compose-profiles` |
| INF-M1.6 | `README.md` | Hướng dẫn profiles, ngân sách RAM, `.wslconfig` | S | `docs/compose-profiles` |

**Hoàn thành khi**:
```bash
docker compose --env-file .env.example config -q
docker compose --profile core up -d && docker compose ps     # tất cả healthy
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list   # có 4 topic
docker compose run --rm mc ls local/                            # raw-data/, checkpoints/
docker stats --no-stream                                        # tổng < 4 GB với profile core
```

### INF-M2 — Raw zone MinIO tin cậy
- **Tuần**: 3–5 · **Phụ trách**: A
- **Phụ thuộc**: INF-M0.1 (`raw_envelope`), INF-M1.3 (bucket). Sang repo khác: không bắt buộc — dùng công cụ replay DP-M0.3.
- **Mock**: `data-pipeline/tools/replay_samples.py` đẩy `tests/fixtures/*.jsonl` vào Kafka.

| Task | File | Mô tả | Công | Nhánh |
|---|---|---|---|---|
| INF-M2.1 | `raw-sink/raw_sink/keys.py` | Tên object `part-<partition>-<firstOffset>.jsonl`, không chứa `:` (H3) | S | `feat/raw-sink-batching` |
| INF-M2.2 | `raw-sink/raw_sink/batching.py` | Gom lô theo 500 bản ghi hoặc 60 s/partition/ngày; bọc envelope (H4, M2.2 cũ) | M | `feat/raw-sink-batching` |
| INF-M2.3 | `raw-sink/raw_sink/app.py` | `enable_auto_commit=False`, commit sau `put_object`; message lỗi → DLQ + log (H5) | M | `fix/raw-sink-commit-dlq` |
| INF-M2.4 | `raw-sink/tests/` | Test key, batching, envelope khớp schema, DLQ | S | `test/raw-sink` |
| INF-M2.5 | `docs/adr/001-object-storage.md` | Lập luận MinIO (S3A) thay HDFS + phương án chuyển HDFS (B viết) | S | `docs/adr-object-storage` |

**Hoàn thành khi**:
```bash
pytest raw-sink/tests -q
python ../data-pipeline/tools/replay_samples.py --topic market_data --file ../data-pipeline/tests/fixtures/market_sample.jsonl
docker compose run --rm mc cat local/raw-data/market/2026/10/27/part-0-0.jsonl | head -1 | check-jsonschema --schemafile contracts/v1/raw_envelope.schema.json -
# gửi 1 message JSON hỏng → xuất hiện trong market_data_dlq, raw-sink vẫn chạy
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic market_data_dlq --from-beginning --max-messages 1
```

### INF-M3 — Schema ClickHouse v1 và fixture
- **Tuần**: 4 (đầu tuần) · **Phụ trách**: C
- **Phụ thuộc**: INF-M0. **Chặn**: INF-M4, INF-M5, API-M1.
- **Mock**: tự tạo fixture từ file mẫu collector.

| Task | File | Mô tả | Công | Nhánh |
|---|---|---|---|---|
| INF-M3.1 | `clickhouse/init/002_v1_tables.sql` | `ALTER ... ADD COLUMN ingested_at` cho `market_data`, `news_data` (M7); tạo `candles_5m`, `indicators`, `signals` (ReplacingMergeTree, PROJECT_REVIEW 3.6); thống nhất `source LowCardinality` (L4) | M | `feat/clickhouse-schema-v1` |
| INF-M3.2 | `clickhouse/fixtures/*.jsonl`, `clickhouse/scripts/load_fixtures.sh` | 2 ngày nến 1m BTC/ETH, 50 tin, 5m, MA20, 10 signal; nạp bằng `INSERT ... FORMAT JSONEachRow` | M | `feat/clickhouse-fixtures` |
| INF-M3.3 | `clickhouse/queries/*.sql` | Truy vấn kiểm tra: trùng, khoảng trống, độ trễ (dùng lại ở INF-M6) | S | `feat/clickhouse-fixtures` |

**Hoàn thành khi**:
```bash
docker compose --profile core down -v && docker compose --profile core up -d
curl -s "http://localhost:8123/?query=SHOW%20TABLES%20FROM%20pipeline" -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD"  # có candles_5m, indicators, signals
./clickhouse/scripts/load_fixtures.sh && ./clickhouse/scripts/load_fixtures.sh   # nạp 2 lần
# SELECT count() FROM pipeline.market_data FINAL  == số dòng fixture (không nhân đôi)
```

### INF-M4 — Spark Structured Streaming → ClickHouse
- **Tuần**: 4–5 · **Phụ trách**: B (A hỗ trợ `stream_news`)
- **Phụ thuộc**: INF-M0.1, INF-M3.1, INF-M1.3 (bucket checkpoints). Sang repo khác: dữ liệu Kafka từ DP — trước DP-M2 dùng replay DP-M0.3.
- **Mock**: replay `tests/fixtures/market_sample.jsonl` lặp lại với `open_time` dịch theo thời gian hiện tại (`replay_samples.py --shift-to-now --rate 10/s`).
- **Đóng issue**: `data-pipeline#2` (phần streaming).

| Task | File | Mô tả | Công | Nhánh |
|---|---|---|---|---|
| INF-M4.1 | `spark/Dockerfile` | Bake jar `hadoop-aws:3.3.4`, `aws-java-sdk-bundle`, `spark-sql-kafka-0-10_2.12:3.5.x`, `clickhouse-jdbc` (bản `all`) | S | `chore/spark-bake-jars` |
| INF-M4.2 | `spark/jobs/common/{schemas,clickhouse,s3}.py` | `StructType` sinh từ `contracts/v1`, hàm ghi ClickHouse qua JDBC, cấu hình S3A (bỏ mật khẩu fallback — M4) | M | `feat/spark-stream-market` |
| INF-M4.3 | `spark/jobs/stream_market.py` | `readStream` Kafka → `from_json` → lọc schema lỗi ra DLQ → `withWatermark("open_ts","2 minutes")` → `dropDuplicates(symbol,interval,open_time)` → `foreachBatch` ghi `market_data`; checkpoint `s3a://checkpoints/stream_market` | L | `feat/spark-stream-market` |
| INF-M4.4 | `spark/jobs/stream_news.py` | Tương tự cho `news_data` | S | `feat/spark-stream-news` |
| INF-M4.5 | `scripts/submit.sh`, `scripts/submit.ps1` | Submit job với cấu hình chuẩn (executor 1 GB khi dev) | S | `chore/spark-submit-scripts` |
| INF-M4.6 | `spark/tests/test_parsing.py` | Test parse/validate bằng `SparkSession` local | S | `test/spark-parsing` |

**Hoàn thành khi**:
```bash
./scripts/submit.sh stream_market.py &   # chạy nền
python ../data-pipeline/tools/replay_samples.py --topic market_data --file ../data-pipeline/tests/fixtures/market_sample.jsonl
# trong ≤ 60 s:
# SELECT symbol, count(), uniqExact(open_time) FROM pipeline.market_data FINAL GROUP BY symbol   -- count = uniqExact > 0
# replay lần 2 → count không đổi; dừng job, chạy lại → tiếp tục từ checkpoint, không trùng
docker compose run --rm mc ls local/checkpoints/stream_market/offsets/
```

### INF-M5 — Spark phân tích (streaming anomaly + batch 5m/MA20)
- **Tuần**: 5–6 · **Phụ trách**: B
- **Phụ thuộc**: INF-M4 (đường ghi ClickHouse), INF-M2 (dữ liệu raw MinIO cho batch), INF-M3 (bảng). **Chặn**: API-M3.
- **Mock**: batch đọc file NDJSON mẫu đặt sẵn ở `s3a://raw-data/` bằng `mc cp` nếu raw-sink chưa xong.
- **Đóng issue**: `data-pipeline#2` (phần batch).

| Task | File | Mô tả | Công | Nhánh |
|---|---|---|---|---|
| INF-M5.1 | `spark/jobs/stream_anomaly.py` | Cửa sổ trượt 20 phút/1 phút theo symbol; `volume > mean + 3·stddev` → `signals` (`signal_type='volume_anomaly'`) | M | `feat/spark-volume-anomaly` |
| INF-M5.2 | `spark/jobs/batch_market_analysis.py` | Đọc envelope NDJSON; 5m chỉ khi đủ 5 nến (M3); MA20 chỉ khi đủ 20 điểm; ghi `candles_5m`, `indicators` (`interval='5m'`) → idempotent (H1) | M | `fix/spark-batch-idempotent` |
| INF-M5.3 | `spark/jobs/batch_market_analysis.py` | Tham số `--date yyyy-mm-dd` để chỉ xử lý 1 ngày (tránh quét toàn bộ) | S | `fix/spark-batch-idempotent` |
| INF-M5.4 | (tùy chọn) `spark/jobs/batch_news_impact.py` | Biến động giá ±30 phút quanh `published_at` | M | `feat/spark-news-impact` |

**Hoàn thành khi**:
```sql
-- chạy batch 2 lần cho cùng --date, kết quả không đổi
SELECT count() FROM pipeline.candles_5m FINAL WHERE toDate(open_time) = '2026-11-20';
SELECT count() FROM pipeline.indicators FINAL WHERE name = 'ma20';
-- không có nến 5m thiếu
SELECT count() FROM pipeline.candles_5m FINAL WHERE candle_count < 5;          -- 0
-- replay file có 1 nến volume x10 → xuất hiện tín hiệu
SELECT * FROM pipeline.signals FINAL WHERE signal_type = 'volume_anomaly' ORDER BY event_time DESC LIMIT 5;
```

### INF-M6 — E2E, kiểm thử lỗi, đo lường, demo
- **Tuần**: 7–8 · **Phụ trách**: D (chủ trì), cả nhóm
- **Phụ thuộc**: DP-M4 (image collector), API-M4 (image API), FE-M4 (image FE), INF-M5.

| Task | File | Mô tả | Công | Nhánh |
|---|---|---|---|---|
| INF-M6.1 | `docker-compose.yml` | Profile `app`: `api` (build `../backend-api`), `frontend` (build `../frontend-dashboard`, port 8088) | S | `feat/compose-app-profile` |
| INF-M6.2 | `scripts/e2e_check.sh` | Chạy chuỗi kiểm tra IP1–IP4, thoát ≠ 0 nếu lỗi | M | `test/e2e-check` |
| INF-M6.3 | `scripts/failure_tests.md` + script | Ngắt mạng collector 5 phút, restart Kafka, restart Spark (checkpoint), gửi trùng, gửi thiếu trường → kết quả kỳ vọng | M | `test/failure-scenarios` |
| INF-M6.4 | `clickhouse/queries/latency.sql`, `counts.sql` | p50/p95 `ingested_at − close_time`; số bản ghi Kafka vs MinIO vs ClickHouse | S | `feat/metrics-queries` |
| INF-M6.5 | `docs/architecture.md`, `docs/E2E_REPORT.md`, `docs/DEMO.md` | Sơ đồ, kết quả đo, kịch bản demo | M | `docs/e2e-report` |

**Hoàn thành khi**:
```bash
docker compose --profile full up -d && ./scripts/e2e_check.sh     # exit 0
```
```sql
SELECT quantiles(0.5, 0.95)(dateDiff('millisecond', close_time, ingested_at)) FROM pipeline.market_data WHERE open_time > now() - INTERVAL 1 HOUR;
SELECT symbol, open_time, prev FROM (SELECT symbol, open_time, lagInFrame(open_time) OVER (PARTITION BY symbol ORDER BY open_time) AS prev FROM pipeline.market_data FINAL WHERE open_time > now() - INTERVAL 1 DAY) WHERE prev > toDateTime64(0,3) AND dateDiff('second', prev, open_time) > 60;   -- 0 dòng
```

## 5. Test và CI tối thiểu

- Workflow `ci.yml` (mọi PR):
  1. `docker compose --env-file .env.example config -q` (cho mọi profile).
  2. `check-jsonschema` mọi ví dụ trong `contracts/v1/examples/` (hợp lệ phải pass, `*.invalid.json` phải fail); `redocly lint contracts/v1/openapi.yaml`.
  3. `ruff check raw-sink spark`.
  4. `pytest raw-sink/tests` và `pytest spark/tests` (pyspark local, Java 17 trên runner).
- Tùy chọn tuần 7: job thủ công `workflow_dispatch` dựng profile `core`, nạp fixture, chạy truy vấn kiểm tra.
- PR sửa `contracts/` hoặc `clickhouse/init/` cần 2 review (C + vai trò bị ảnh hưởng).

## 6. Rủi ro riêng và giảm thiểu

| Rủi ro | Giảm thiểu |
|---|---|
| Spark streaming (L) chỉ một người làm, nằm trên đường găng | Bắt đầu tuần 3 với replay; A hỗ trợ `stream_news`; nếu trễ cuối tuần 5 → tạm dùng raw-sink ghi thẳng ClickHouse cho market (ghi rõ là phương án tạm) |
| Jar Spark lệch phiên bản / tải Maven chậm lúc demo | Bake jar vào image (INF-M4.1); ghi phiên bản trong Dockerfile |
| Thiếu RAM khi chạy `full` (~7–9 GB) | Profiles, `mem_limit`, executor 1 GB, chỉ 1 streaming job gộp market+anomaly nếu cần |
| Đổi schema ClickHouse làm mất dữ liệu | Migration bằng file `00N_*.sql` chỉ `ADD`/`CREATE`; `down -v` chỉ dùng khi dev |
| Giảng viên yêu cầu HDFS | ADR INF-M2.5; profile `hadoop` (namenode + datanode) và đổi `s3a://` → `hdfs://` qua biến môi trường |
| Hợp đồng bị sửa tùy tiện | CODEOWNERS cho `contracts/`, review 4 vai trò |

## 7. Vấn đề rà soát thuộc repo này

### Nghiêm trọng
| # | Vấn đề | Vị trí | Sửa ở |
|---|---|---|---|
| C2 | Không ai ghi vào `market_data`/`news_data` → API không có dữ liệu | `clickhouse/init/001_schema.sql:3-41` | INF-M4 |
| C3 | Không có Spark Structured Streaming | `spark/jobs/` | INF-M4, INF-M5 |
| C4 | Không có hợp đồng dữ liệu chung; raw-sink và Spark tự giả định trường | `raw-sink/consumer.py:51-61`, `spark/jobs/batch_market_analysis.py:42-53` | INF-M0 |

### Cao
| # | Vấn đề | Vị trí | Sửa ở |
|---|---|---|---|
| H1 | `analysis_results` là `MergeTree` + job `append` → trùng khi chạy lại | `clickhouse/init/001_schema.sql:43-55`, `spark/jobs/batch_market_analysis.py:117-127` | INF-M3, INF-M5 |
| H3 | Tên object chứa `:` → nguy cơ lỗi Hadoop `Path` khi đọc `s3a://` (chưa xác minh) | `raw-sink/consumer.py:51-61` | INF-M2 |
| H4 | 1 object/bản ghi → small-files, Spark đọc chậm | `raw-sink/consumer.py:86-95`, `spark/jobs/batch_market_analysis.py:37-41` | INF-M2 |
| H5 | Auto-commit, không bắt lỗi → crash lặp/mất bản ghi | `raw-sink/consumer.py:76-95` | INF-M2 |
| H7 | `spark-defaults.conf` mount sai chỗ, không có hiệu lực | `docker-compose.yml:120,147` | INF-M1 |
| H9 | Kết quả 5m vẫn gắn `interval='1m'` | `spark/jobs/batch_market_analysis.py:58,69` | INF-M5 |

### Trung bình
| # | Vấn đề | Vị trí | Sửa ở |
|---|---|---|---|
| M3 | 5m tính cả cửa sổ thiếu nến; MA20 tính khi < 20 điểm | `spark/jobs/batch_market_analysis.py:57-89` | INF-M5 |
| M4 | Mật khẩu mẫu làm fallback trong code | `spark/jobs/batch_market_analysis.py:17-18,122-123`, `raw-sink/consumer.py:22-23` | INF-M2, INF-M4 |
| M5 | Image MinIO `latest` | `docker-compose.yml:44` | INF-M1 |
| M7 | Thiếu `ingested_at` để đo độ trễ | `clickhouse/init/001_schema.sql:3-41` | INF-M3 |
| M10 | Job batch quét toàn bộ lịch sử mỗi lần chạy | `spark/jobs/batch_market_analysis.py:37-41` | INF-M5 |

### Thấp
| # | Vấn đề | Vị trí | Sửa ở |
|---|---|---|---|
| L1 | `.pyc` bị commit; `.gitignore` thiếu `__pycache__/` | `.gitignore:1-3` | INF-M0 |
| L4 | `source` kiểu khác nhau giữa 2 bảng | `clickhouse/init/001_schema.sql:6,28` | INF-M3 |
| L5 | Tên topic hard-code trong raw-sink | `raw-sink/consumer.py:52,77-78` | INF-M2 |
| L6 | Healthcheck Kafka dùng listener EXTERNAL | `docker-compose.yml:24` | INF-M1 |
