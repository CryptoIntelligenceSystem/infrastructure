# Infrastructure

Docker Compose cho pipeline dữ liệu gồm Kafka, MinIO, ClickHouse và Spark. Hợp đồng dữ liệu chung nằm trong [`contracts/`](contracts/README.md).

## Yêu cầu: clone 4 repo cạnh nhau

Code các Spark job nằm trong repo `data-pipeline` và được mount vào container Spark. Mặc định compose tìm repo đó ở `../data-pipeline`:

```text
<thư-mục-cha>/
  data-pipeline/
  infrastructure/        <- chạy docker compose ở đây
  backend-api/
  frontend-dashboard/
```

Nếu để chỗ khác, đặt `PIPELINE_REPO_DIR` trong `.env` (đường dẫn tương đối tính từ thư mục `infrastructure`).

## Khởi động

```powershell
Copy-Item .env.example .env      # đổi mật khẩu; MINIO_ROOT_PASSWORD không chứa @ : /
docker compose up -d --build
docker compose ps
```

Thứ tự khởi động (compose tự xử lý qua `depends_on`):

1. `kafka` → `kafka-init` tạo topic `market_data`, `news_data`
2. `minio` → `mc-init` tạo bucket `raw-data`, `checkpoints`
3. `clickhouse` (lần đầu chạy `clickhouse/init/*.sql`)
4. `spark-master` (chờ Kafka healthy, `mc-init` xong) → `spark-worker` và `spark-batch-worker`
5. `collector` lấy dữ liệu Binance/CoinDesk liên tục và gửi dữ liệu vào Kafka
6. Submit các Spark job từ repo `data-pipeline` (xem README của repo đó): streaming trước (`raw_to_minio`, `market_to_clickhouse`, `news_to_clickhouse`, `volume_anomaly`), batch sau (`batch_ohlcv_5m`, `batch_indicators`, `batch_news_impact`)
7. `backend` cung cấp API trên cổng `5080`, `frontend` phục vụ Dashboard trên `http://localhost:3000` và chuyển tiếp `/api/` đến Backend trong mạng Compose.

Collector được build từ repo `data-pipeline` và chạy trong container `pipeline-collector`.
Thư mục `data-pipeline/data` được mount vào container để giữ dữ liệu đã thu thập và trạng thái
deduplicate sau khi container khởi động lại. Khi sửa code Collector, build lại service:

```powershell
docker compose up -d --build collector
docker compose logs -f collector
```

Dừng riêng Collector:

```powershell
docker compose stop collector
```

## Migration ClickHouse

`clickhouse/init/` chỉ được ClickHouse chạy khi volume **còn trống**. Với database đã có dữ liệu, áp dụng các thay đổi schema bằng:

```powershell
./scripts/migrate_clickhouse.ps1      # hoặc: bash scripts/migrate_clickhouse.sh
```

Script chạy lần lượt mọi `clickhouse/init/*.sql`; các câu lệnh đều là `CREATE ... IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS` nên chạy lại nhiều lần không sao và không xóa dữ liệu.

Bảng trong database `pipeline`:

| Bảng | Engine / ORDER BY | Ghi bởi |
|---|---|---|
| `market_data` | `ReplacingMergeTree(collected_at)` / `(symbol, interval, open_time)` | Spark `market_to_clickhouse` |
| `market_data_rejects` | `ReplacingMergeTree(rejected_at)` / `(topic, partition, offset)` | Spark `market_to_clickhouse` (dead-letter) |
| `news_data_rejects` | `ReplacingMergeTree(rejected_at)` / `(topic, partition, offset)` | Spark `news_to_clickhouse` (dead-letter) |
| `candles_5m` | `ReplacingMergeTree(processed_at)` / `(symbol, interval, open_time)` | Spark `batch_ohlcv_5m` |
| `indicators` | `ReplacingMergeTree(processed_at)` / `(symbol, interval, name, open_time)` | Spark `batch_indicators` |
| `signals` | `ReplacingMergeTree(detected_at)` / `(symbol, signal_type, open_time)` | Spark `volume_anomaly` |
| `news_data` | `ReplacingMergeTree(collected_at)` / `(source, article_id)` | Spark `news_to_clickhouse` |
| `news_impact` | `ReplacingMergeTree(calculated_at)` / `(source, article_id, symbol, horizon_minutes)` | Spark `batch_news_impact` |
| `analysis_results` | `MergeTree` | không còn job nào ghi; giữ lại, không xóa |

Đọc bảng `ReplacingMergeTree` luôn dùng `FINAL` để gộp các bản ghi lặp do chạy lại.

## Dịch vụ và cổng

| Dịch vụ | Cổng host | Ghi chú |
|---|---|---|
| Kafka | `9092` | trong Docker: `kafka:29092` |
| Collector | — | chạy liên tục, log bằng `docker compose logs -f collector` |
| MinIO API / Console | `9000` / `9001` | đăng nhập bằng `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| ClickHouse HTTP / native | `8123` / `9003` | database `pipeline` |
| Spark master | `7077`, UI `8080` | |
| Spark worker UI | `8081` | |
| Spark batch worker UI | `8082` | worker riêng để batch không hết core khi streaming đang chạy |
| Spark UI của job đang chạy | `4040`–`4043` | job submit đầu tiên ở `4040`, job tiếp theo `4041`, ... |
| Backend API / Swagger | `5080` | `http://localhost:5080/swagger` |
| Frontend Dashboard | `3000` | `http://localhost:3000` |

## Spark

- Image `spark/Dockerfile`: `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu`, đã **bake sẵn** connector (không cần `--packages`, không cần mạng khi submit):

  | Jar | Phiên bản | Lý do |
  |---|---|---|
  | `spark-sql-kafka-0-10_2.12`, `spark-token-provider-kafka-0-10_2.12` | 3.5.9 | = Spark |
  | `kafka-clients` | 3.4.1 | Spark 3.5.9 build với Kafka 3.4.1 |
  | `commons-pool2` | 2.11.1 | phụ thuộc của Kafka connector |
  | `hadoop-aws` | 3.3.4 | = Hadoop trong image (build kiểm tra `hadoop-client-api-3.3.4.jar`) |
  | `aws-java-sdk-bundle` | 1.12.262 | bản `hadoop-aws` 3.3.4 yêu cầu |
  | `clickhouse-jdbc` (`-all`) | 0.8.1 | driver `com.clickhouse.jdbc.ClickHouseDriver` (đổi bằng `CLICKHOUSE_JDBC_DRIVER`) |

- `spark/conf/spark-defaults.conf` được mount vào `/opt/spark/conf/`: múi giờ UTC (`spark.sql.session.timeZone`, `-Duser.timezone=UTC`), S3A path-style cho MinIO, mỗi job mặc định `spark.cores.max=1`, executor 640 MB.
- Tài nguyên (đổi trong `.env`): worker chính cung cấp 4 core / 3 GB cho streaming; `spark-batch-worker` cung cấp mặc định 2 core / 2 GB cho batch; master giới hạn 3 GB / 2 CPU vì driver của các job chạy trong container master. Nhờ worker riêng, batch vẫn có core khi cả bốn job streaming đang chạy.
- Spark UI: master `http://localhost:8080` (danh sách job), worker `http://localhost:8081`, batch worker `http://localhost:8082`, từng job `http://localhost:4040`… (tab *Structured Streaming* xem tốc độ xử lý, độ trễ batch).

## Raw sink cũ

`raw-sink` (Python) được thay bằng Spark job `raw_to_minio` và chuyển sang profile `legacy`, không chạy mặc định. Chỉ bật khi cần so sánh:

```powershell
docker compose --profile legacy up -d raw-sink
```

Không chạy song song với `raw_to_minio` vì hai bên sẽ ghi trùng dữ liệu thô.

## Dừng / reset

```powershell
docker compose stop          # dừng, giữ dữ liệu
docker compose down -v       # XÓA toàn bộ volume dữ liệu — chỉ khi chắc chắn muốn reset
```

Không commit `.env`; mật khẩu trong `.env.example` chỉ là giá trị mẫu.
