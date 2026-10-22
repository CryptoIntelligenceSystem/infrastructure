CREATE DATABASE IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS pipeline.market_data
(
    data_type String,
    source String,
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
    collected_at DateTime64(3, 'UTC'),
    schema_version LowCardinality(String)
)
ENGINE = ReplacingMergeTree(collected_at)
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, interval, open_time);

CREATE TABLE IF NOT EXISTS pipeline.news_data
(
    data_type String,
    source LowCardinality(String),
    article_id String,
    title String,
    summary String,
    url String,
    published_at DateTime64(3, 'UTC'),
    collected_at DateTime64(3, 'UTC'),
    symbols Array(String),
    sentiment Nullable(String),
    schema_version LowCardinality(String)
)
ENGINE = ReplacingMergeTree(collected_at)
PARTITION BY toYYYYMM(published_at)
ORDER BY (source, article_id);

CREATE TABLE IF NOT EXISTS pipeline.analysis_results
(
    result_type LowCardinality(String),
    symbol LowCardinality(String),
    interval LowCardinality(String),
    event_time DateTime64(3, 'UTC'),
    value Float64,
    metadata String,
    created_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (result_type, symbol, interval, event_time);
