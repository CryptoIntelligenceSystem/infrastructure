"""Example batch job: MinIO raw market data -> 5m candles/MA20 -> ClickHouse."""

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def configure_s3a(spark: SparkSession) -> None:
    """Configure Hadoop S3A for the MinIO service inside Docker."""
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.s3a.endpoint", env("MINIO_ENDPOINT", "http://minio:9000"))
    conf.set("fs.s3a.access.key", env("MINIO_ACCESS_KEY", "minioadmin"))
    conf.set("fs.s3a.secret.key", env("MINIO_SECRET_KEY", "change-this-minio-password"))
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("market-analysis-batch")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_s3a(spark)

    raw_path = env("RAW_MARKET_PATH", "s3a://raw-data/market/")
    market = (
        spark.read.format("json")
        .option("recursiveFileLookup", "true")
        .load(raw_path)
        .select(
            "symbol",
            "interval",
            F.to_timestamp("open_time").alias("open_ts"),
            F.col("open").cast("double").alias("open"),
            F.col("high").cast("double").alias("high"),
            F.col("low").cast("double").alias("low"),
            F.col("close").cast("double").alias("close"),
            F.col("volume").cast("double").alias("volume"),
            F.col("quote_volume").cast("double").alias("quote_volume"),
            F.col("trade_count").cast("long").alias("trade_count"),
        )
        .where(F.col("symbol").isNotNull() & F.col("open_ts").isNotNull())
    )

    candles = (
        market.groupBy("symbol", "interval", F.window("open_ts", "5 minutes").alias("time_window"))
        .agg(
            F.expr("min_by(open, open_ts)").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.expr("max_by(close, open_ts)").alias("close"),
            F.sum("volume").alias("volume"),
            F.sum("quote_volume").alias("quote_volume"),
            F.sum("trade_count").alias("trade_count"),
        )
        .select(
            "symbol",
            "interval",
            F.col("time_window.start").alias("event_time"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "trade_count",
        )
    )

    order_window = (
        Window.partitionBy("symbol", "interval")
        .orderBy("event_time")
        .rowsBetween(-19, 0)
    )
    result = (
        candles.withColumn("ma20", F.avg("close").over(order_window))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("result_type", F.lit("5m_ma20"))
        .withColumn(
            "metadata",
            F.to_json(
                F.struct(
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "quote_volume",
                    "trade_count",
                )
            ),
        )
        .select(
            "result_type",
            "symbol",
            "interval",
            "event_time",
            F.col("ma20").cast("double").alias("value"),
            "metadata",
            "created_at",
        )
    )

    jdbc_url = env("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/pipeline")
    (
        result.write.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", "analysis_results")
        .option("user", env("CLICKHOUSE_USER", "pipeline_user"))
        .option("password", env("CLICKHOUSE_PASSWORD", "change-this-clickhouse-password"))
        .option("batchsize", "1000")
        .mode("append")
        .save()
    )

    print(f"Wrote {result.count()} analysis rows to ClickHouse")
    spark.stop()


if __name__ == "__main__":
    main()
