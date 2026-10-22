# Infrastructure

Docker Compose cho pipeline dữ liệu gồm Kafka, MinIO và ClickHouse.

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

Các bảng được khởi tạo tự động từ `clickhouse/init/001_schema.sql`:

- `pipeline.market_data`
- `pipeline.news_data`
- `pipeline.analysis_results`

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
