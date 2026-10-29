-- Tables written by the Spark jobs in data-pipeline/spark_jobs.
-- Idempotent: runs on a fresh volume (docker-entrypoint-initdb.d) and can be re-applied to an
-- existing database with scripts/migrate_clickhouse.sh. Never drops anything.
-- Every table is a ReplacingMergeTree keyed on its natural key, so replays after a streaming
-- restart or a batch rerun collapse into one row (read with FINAL).

-- When Spark wrote the candle; processed_at - close_time is the end-to-end latency.
-- Nullable: rows written before this migration have no processing time.
ALTER TABLE pipeline.market_data
    ADD COLUMN IF NOT EXISTS processed_at Nullable(DateTime64(3, 'UTC'));

-- Dead letter for market_data messages that fail contract validation.
CREATE TABLE IF NOT EXISTS pipeline.market_data_rejects
(
    topic LowCardinality(String),
    partition Int32,
    offset Int64,
    kafka_timestamp DateTime64(3, 'UTC'),
    key Nullable(String),
    raw_value String,
    errors String,              -- "; "-separated validation errors
    rejected_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(rejected_at)
PARTITION BY toYYYYMM(kafka_timestamp)
ORDER BY (topic, partition, offset);

CREATE TABLE IF NOT EXISTS pipeline.candles_5m
(
    symbol LowCardinality(String),
    interval LowCardinality(String),
    open_time DateTime64(3, 'UTC'),
    close_time DateTime64(3, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    quote_volume Float64,
    trade_count UInt64,
    candle_count UInt8,         -- number of 1m candles aggregated (always 5: incomplete buckets are skipped)
    processed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, interval, open_time);

CREATE TABLE IF NOT EXISTS pipeline.indicators
(
    symbol LowCardinality(String),
    interval LowCardinality(String),
    name LowCardinality(String),        -- e.g. ma20
    open_time DateTime64(3, 'UTC'),
    value Nullable(Float64),            -- NULL while the window is not full yet
    processed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, interval, name, open_time);

CREATE TABLE IF NOT EXISTS pipeline.signals
(
    symbol LowCardinality(String),
    signal_type LowCardinality(String), -- e.g. volume_anomaly
    open_time DateTime64(3, 'UTC'),     -- candle that triggered the signal
    value Float64,                      -- observed volume
    baseline_mean Float64,
    baseline_stddev Float64,
    threshold Float64,                  -- mean + k * stddev
    details String,                     -- JSON with rule parameters
    detected_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(detected_at)
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, signal_type, open_time);

-- pipeline.analysis_results (001_schema.sql) is no longer written by any job. Kept, not dropped.
