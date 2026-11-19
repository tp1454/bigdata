CREATE TABLE IF NOT EXISTS analytics.crypto_ohlcv
(
    symbol LowCardinality(String),
    interval LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (source, symbol, interval, timestamp)
TTL toDateTime(timestamp) + INTERVAL 90 DAY;
