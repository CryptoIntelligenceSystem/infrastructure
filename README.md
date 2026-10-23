# Infrastructure

Docker Compose cho pipeline dữ liệu gồm Kafka, MinIO, ClickHouse và Spark.

## Khởi động

```powershell
Copy-Item .env.example .env
docker compose up -d
```

Kiểm tra trạng thái:

```powershell
docker compose ps
docker compose logs kafka-init
```

Kafka chỉ mở cho máy host tại `localhost:9092`. Hai topic được tạo tự động:

- `market_data`
- `news_data`

MinIO:

- API: `http://localhost:9000`
- Console: `http://localhost:9001`
- Đăng nhập bằng `MINIO_ROOT_USER` và `MINIO_ROOT_PASSWORD` trong `.env`.

ClickHouse:

- HTTP: `http://localhost:8123`
- Native client: `localhost:9003`
- Database: `pipeline`

Spark:

- Master UI: `http://localhost:8080`
- Worker UI: `http://localhost:8081`
- Master URL trong Docker: `spark://spark-master:7077`
- Job mẫu nằm trong `spark/jobs/`.

Trong cùng network Docker, các service phải dùng tên service để kết nối:

- MinIO S3 API: `http://minio:9000`
- Kafka: `kafka:29092`
- ClickHouse HTTP/JDBC: `clickhouse:8123`

Các bảng được khởi tạo tự động từ `clickhouse/init/001_schema.sql`:

- `pipeline.market_data`
- `pipeline.news_data`
- `pipeline.analysis_results`

Raw sink:

- `raw-sink` đọc hai Kafka topic `market_data` và `news_data`.
- Service tự tạo bucket `raw-data` nếu bucket chưa tồn tại.
- Market được lưu theo `market/YYYY/MM/DD/`.
- News được lưu theo `news/YYYY/MM/DD/`.
- Tên object chứa khóa bản ghi nên chạy lại message sẽ ghi đè đúng object, không tạo bản sao.

Spark job mẫu:

- Đọc raw market JSON từ `s3a://raw-data/market/`.
- Gom nến 1 phút thành nến 5 phút và tính MA20.
- Ghi kết quả vào bảng `pipeline.analysis_results` bằng ClickHouse JDBC.
- S3A, Kafka connector và ClickHouse JDBC được nạp khi chạy `spark-submit` qua Maven packages.

Khởi động Spark:

```powershell
docker compose up -d spark-master spark-worker
docker compose ps
```

Chạy job mẫu sau khi collector đã gửi dữ liệu vào Kafka và raw-sink đã ghi vào MinIO:

```powershell
docker compose exec -T spark-worker `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.clickhouse:clickhouse-jdbc:0.8.1 `
  /opt/spark/jobs/batch_market_analysis.py
```

Nếu cần đọc Kafka trực tiếp bằng Spark Structured Streaming, thêm package:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9
```

Khởi động hoặc build lại raw sink sau khi sửa code:

```powershell
docker compose up -d --build raw-sink
docker compose logs -f raw-sink
```

Truy vấn thử ClickHouse:

```powershell
curl "http://localhost:8123/?database=pipeline&query=SHOW%20TABLES"
```

Dừng dịch vụ nhưng giữ dữ liệu:

```powershell
docker compose stop
```

Xóa container và volume dữ liệu chỉ khi muốn reset toàn bộ môi trường:

```powershell
docker compose down -v
```

Không commit `.env`; mật khẩu trong `.env.example` chỉ là giá trị mẫu và cần đổi trước khi dùng chung.
