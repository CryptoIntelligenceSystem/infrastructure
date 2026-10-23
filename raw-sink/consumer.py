import json
import logging
import os
import time
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from kafka import KafkaConsumer


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s raw-sink: %(message)s",
)
LOGGER = logging.getLogger("raw-sink")


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "raw-sink-minio")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-data")


def minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def ensure_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
        return
    except ClientError:
        LOGGER.info("Creating MinIO bucket %s", MINIO_BUCKET)
        try:
            s3.create_bucket(Bucket=MINIO_BUCKET)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise


def record_key(topic: str, record: dict) -> str:
    if topic == "market_data":
        timestamp = record["open_time"]
        identifier = f"{record['symbol']}_{record['interval']}_{record['open_time']}"
        prefix = "market"
    else:
        timestamp = record["published_at"]
        identifier = record["article_id"]
        prefix = "news"
    date = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return f"{prefix}/{date:%Y/%m/%d}/{identifier}.json"


def main() -> None:
    s3 = minio_client()
    for attempt in range(1, 11):
        try:
            ensure_bucket(s3)
            break
        except Exception as exc:
            if attempt == 10:
                raise
            LOGGER.warning("MinIO is not ready (%d/10): %s", attempt, exc)
            time.sleep(3)

    consumer = KafkaConsumer(
        "market_data",
        "news_data",
        bootstrap_servers=[server.strip() for server in KAFKA_BOOTSTRAP_SERVERS.split(",")],
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    LOGGER.info("Consuming market_data/news_data into s3://%s", MINIO_BUCKET)
    for message in consumer:
        key = record_key(message.topic, message.value)
        body = json.dumps(message.value, ensure_ascii=False).encode("utf-8")
        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        LOGGER.info("Stored %s/%s at s3://%s/%s", message.topic, message.partition, MINIO_BUCKET, key)


if __name__ == "__main__":
    main()
