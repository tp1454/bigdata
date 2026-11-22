-- No-key sources: Yahoo option chains, SEC 13F filings and the Nasdaq Trader
-- symbol directory. Slow, keyed snapshots; TTLs match the other reference data.

-- Option-chain aggregates per symbol/expiry (put/call volume and open interest).
CREATE TABLE IF NOT EXISTS analytics.options_metrics_local ON CLUSTER analytics_cluster
(
    symbol LowCardinality(String),
    expiry Date,
    timestamp DateTime64(3, 'UTC'),
    underlying_price Float64,
    call_volume Float64,
    put_volume Float64,
    call_oi Float64,
    put_oi Float64,
    put_call_volume_ratio Nullable(Float64),
    put_call_oi_ratio Nullable(Float64),
    volume_oi_ratio Nullable(Float64),
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/options_metrics', '{replica}', ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, expiry, timestamp)
TTL toDateTime(timestamp) + INTERVAL 1 YEAR TO VOLUME 'cold',
    toDateTime(timestamp) + INTERVAL 5 YEAR DELETE
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS analytics.options_metrics ON CLUSTER analytics_cluster
AS analytics.options_metrics_local
ENGINE = Distributed(analytics_cluster, analytics, options_metrics_local, cityHash64(symbol));

-- SEC 13F holdings, aggregated to one row per filer/period/CUSIP.
-- shortcut: a partial 13F amendment only replaces the holdings it lists;
-- omitted holdings from the original filing stay until the period expires.
CREATE TABLE IF NOT EXISTS analytics.institutional_holdings_local ON CLUSTER analytics_cluster
(
    filer_cik String,
    filer_name String,
    period Date,
    filing_date Date,
    form LowCardinality(String),
    accession_number String,
    cusip String,
    issuer String,
    title String,
    shares Float64,
    share_type LowCardinality(String),
    value Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/institutional_holdings', '{replica}', ingested_at)
PARTITION BY toYYYYMM(period)
ORDER BY (filer_cik, period, cusip, title, share_type)
TTL toDate(period) + INTERVAL 1 YEAR TO VOLUME 'cold',
    toDate(period) + INTERVAL 10 YEAR DELETE
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS analytics.institutional_holdings ON CLUSTER analytics_cluster
AS analytics.institutional_holdings_local
ENGINE = Distributed(analytics_cluster, analytics, institutional_holdings_local, cityHash64(filer_cik));

-- Ticker catalogue from Nasdaq Trader, joined to SEC CIKs by ticker.
CREATE TABLE IF NOT EXISTS analytics.symbol_catalog_local ON CLUSTER analytics_cluster
(
    symbol LowCardinality(String),
    name String,
    exchange LowCardinality(String),
    etf UInt8,
    cik Nullable(UInt32),
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/symbol_catalog', '{replica}', ingested_at)
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (symbol, exchange)
TTL toDateTime(ingested_at) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS analytics.symbol_catalog ON CLUSTER analytics_cluster
AS analytics.symbol_catalog_local
ENGINE = Distributed(analytics_cluster, analytics, symbol_catalog_local, cityHash64(symbol));
