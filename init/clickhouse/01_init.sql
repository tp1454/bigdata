CREATE DATABASE IF NOT EXISTS analytics ON CLUSTER analytics_cluster;

-- Delivery is at-least-once (Kafka offsets commit with the Flink checkpoint),
-- so every table dedupes on its natural key with ReplacingMergeTree. Queries
-- must use FINAL (or argMax) to see the deduped result before a merge runs.
--
-- Two shards x two replicas. Writes and reads use the Distributed table with
-- the public name; the local Replicated table is the storage on each node.
-- Shard keys spread a hot symbol (crypto trades) or keep one entity together
-- (prices by symbol) and are chosen per table below.

-- 1. Giao dich co phieu (Yahoo)
CREATE TABLE IF NOT EXISTS analytics.equities_prices_local ON CLUSTER analytics_cluster
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
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/equities_prices', '{replica}', ingested_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY TO VOLUME 'cold',
    toDateTime(ts) + INTERVAL 90 DAY DELETE
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS analytics.equities_prices ON CLUSTER analytics_cluster
AS analytics.equities_prices_local
ENGINE = Distributed(analytics_cluster, analytics, equities_prices_local, cityHash64(symbol));

-- 2. Giao dich Binance. Shard on trade_id so BTC's volume spreads evenly.
CREATE TABLE IF NOT EXISTS analytics.crypto_trades_local ON CLUSTER analytics_cluster
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    price Float64,
    quantity Float64,
    trade_id UInt64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/crypto_trades', '{replica}', ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (source, symbol, trade_id)
TTL toDateTime(timestamp) + INTERVAL 7 DAY TO VOLUME 'cold',
    toDateTime(timestamp) + INTERVAL 90 DAY DELETE
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS analytics.crypto_trades ON CLUSTER analytics_cluster
AS analytics.crypto_trades_local
ENGINE = Distributed(analytics_cluster, analytics, crypto_trades_local, cityHash64(trade_id));

-- 3. Du lieu thi truong CoinGecko
CREATE TABLE IF NOT EXISTS analytics.crypto_market_data_local ON CLUSTER analytics_cluster
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    price Float64,
    market_cap Nullable(Float64),
    volume_24h Nullable(Float64),
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/crypto_market_data', '{replica}', ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (source, symbol, timestamp)
TTL toDateTime(timestamp) + INTERVAL 7 DAY TO VOLUME 'cold',
    toDateTime(timestamp) + INTERVAL 90 DAY DELETE
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS analytics.crypto_market_data ON CLUSTER analytics_cluster
AS analytics.crypto_market_data_local
ENGINE = Distributed(analytics_cluster, analytics, crypto_market_data_local, cityHash64(symbol));

-- 4. Ty gia Forex
CREATE TABLE IF NOT EXISTS analytics.forex_rates_local ON CLUSTER analytics_cluster
(
    pair LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    rate Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/forex_rates', '{replica}', ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (source, pair, timestamp)
TTL toDateTime(timestamp) + INTERVAL 7 DAY TO VOLUME 'cold',
    toDateTime(timestamp) + INTERVAL 90 DAY DELETE
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS analytics.forex_rates ON CLUSTER analytics_cluster
AS analytics.forex_rates_local
ENGINE = Distributed(analytics_cluster, analytics, forex_rates_local, cityHash64(pair));

-- 5. Chi so kinh te BLS va Treasury (daily, keep 10 years)
CREATE TABLE IF NOT EXISTS analytics.macro_indicators_local ON CLUSTER analytics_cluster
(
    indicator LowCardinality(String),
    timestamp Date,
    value Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/macro_indicators', '{replica}', ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (source, indicator, timestamp)
TTL toDate(timestamp) + INTERVAL 1 YEAR TO VOLUME 'cold',
    toDate(timestamp) + INTERVAL 10 YEAR DELETE
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS analytics.macro_indicators ON CLUSTER analytics_cluster
AS analytics.macro_indicators_local
ENGINE = Distributed(analytics_cluster, analytics, macro_indicators_local, cityHash64(indicator));

-- 6. Ho so doanh nghiep SEC EDGAR (slow, keep 10 years)
CREATE TABLE IF NOT EXISTS analytics.company_filings_local ON CLUSTER analytics_cluster
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
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/company_filings', '{replica}', ingested_at)
PARTITION BY toYYYYMM(filing_date)
ORDER BY (cik, accession_number)
TTL toDate(filing_date) + INTERVAL 1 YEAR TO VOLUME 'cold',
    toDate(filing_date) + INTERVAL 10 YEAR DELETE
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS analytics.company_filings ON CLUSTER analytics_cluster
AS analytics.company_filings_local
ENGINE = Distributed(analytics_cluster, analytics, company_filings_local, cityHash64(cik));

-- 7. Ban ghi loi parse (dead-letter), giu 30 ngay
CREATE TABLE IF NOT EXISTS analytics.pipeline_errors_local ON CLUSTER analytics_cluster
(
    topic LowCardinality(String),
    error String,
    raw String,
    ts DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/pipeline_errors', '{replica}')
PARTITION BY toYYYYMM(ts)
ORDER BY (ts)
TTL toDateTime(ts) + INTERVAL 7 DAY TO VOLUME 'cold',
    toDateTime(ts) + INTERVAL 30 DAY DELETE
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS analytics.pipeline_errors ON CLUSTER analytics_cluster
AS analytics.pipeline_errors_local
ENGINE = Distributed(analytics_cluster, analytics, pipeline_errors_local, cityHash64(topic, ts));
