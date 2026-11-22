CREATE TABLE IF NOT EXISTS analytics.crypto_ohlcv_local ON CLUSTER analytics_cluster
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
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/crypto_ohlcv', '{replica}', ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (source, symbol, interval, timestamp)
TTL toDateTime(timestamp) + INTERVAL 7 DAY TO VOLUME 'cold',
    toDateTime(timestamp) + INTERVAL 90 DAY DELETE
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS analytics.crypto_ohlcv ON CLUSTER analytics_cluster
AS analytics.crypto_ohlcv_local
ENGINE = Distributed(analytics_cluster, analytics, crypto_ohlcv_local, cityHash64(symbol, interval));
