CREATE DATABASE IF NOT EXISTS analytics;

-- Delivery is at-least-once (Kafka offsets commit with the Flink checkpoint),
-- so every table dedupes on its natural key with ReplacingMergeTree. Queries
-- must use FINAL (or argMax) to see the deduped result before a merge runs.

-- 1. Giao dich co phieu (Yahoo)
CREATE TABLE IF NOT EXISTS analytics.equities_prices
(
    symbol LowCardinality(String),
    ts DateTime64(3, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume UInt64,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts)
TTL ts + INTERVAL 90 DAY;

-- 2. Giao dich Binance
CREATE TABLE IF NOT EXISTS analytics.crypto_trades
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    price Float64,
    quantity Float64,
    trade_id UInt64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (source, symbol, trade_id)
TTL timestamp + INTERVAL 90 DAY;

-- 3. Du lieu thi truong CoinGecko
CREATE TABLE IF NOT EXISTS analytics.crypto_market_data
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    price Float64,
    market_cap Nullable(Float64),
    volume_24h Nullable(Float64),
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (source, symbol, timestamp)
TTL timestamp + INTERVAL 90 DAY;

-- 4. Ty gia Forex
CREATE TABLE IF NOT EXISTS analytics.forex_rates
(
    pair LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    rate Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (source, pair, timestamp)
TTL timestamp + INTERVAL 90 DAY;

-- 5. Chi so kinh te BLS va Treasury (daily, keep forever)
CREATE TABLE IF NOT EXISTS analytics.macro_indicators
(
    indicator LowCardinality(String),
    timestamp Date,
    value Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (source, indicator, timestamp);

-- 6. Ho so doanh nghiep SEC EDGAR (slow, keep forever)
CREATE TABLE IF NOT EXISTS analytics.company_filings
(
    cik String,
    entity_name Nullable(String),
    sic Nullable(String),
    accession_number String,
    filing_date Date,
    form String,
    primary_document Nullable(String),
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (cik, accession_number);

-- 7. Ban ghi loi parse (dead-letter), giu 30 ngay
CREATE TABLE IF NOT EXISTS analytics.pipeline_errors
(
    topic LowCardinality(String),
    error String,
    raw String,
    ts DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ts)
TTL ts + INTERVAL 30 DAY;
