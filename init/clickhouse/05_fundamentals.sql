-- Corporate actions (slow, keep 10 years)
CREATE TABLE IF NOT EXISTS analytics.corporate_actions_local ON CLUSTER analytics_cluster
(
    symbol LowCardinality(String),
    action LowCardinality(String),
    action_date Date,
    value Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/corporate_actions', '{replica}', ingested_at)
PARTITION BY toYYYYMM(action_date)
ORDER BY (source, symbol, action, action_date)
TTL toDate(action_date) + INTERVAL 1 YEAR TO VOLUME 'cold',
    toDate(action_date) + INTERVAL 10 YEAR DELETE
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS analytics.corporate_actions ON CLUSTER analytics_cluster
AS analytics.corporate_actions_local
ENGINE = Distributed(analytics_cluster, analytics, corporate_actions_local, cityHash64(symbol));

-- Financial statement line items (annual/quarterly) plus earnings history.
CREATE TABLE IF NOT EXISTS analytics.fundamentals_local ON CLUSTER analytics_cluster
(
    symbol LowCardinality(String),
    statement LowCardinality(String),
    period LowCardinality(String),
    end_date Date,
    line_item LowCardinality(String),
    value Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fundamentals', '{replica}', ingested_at)
PARTITION BY toYYYYMM(end_date)
ORDER BY (source, symbol, statement, period, end_date, line_item)
TTL toDate(end_date) + INTERVAL 1 YEAR TO VOLUME 'cold',
    toDate(end_date) + INTERVAL 10 YEAR DELETE
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS analytics.fundamentals ON CLUSTER analytics_cluster
AS analytics.fundamentals_local
ENGINE = Distributed(analytics_cluster, analytics, fundamentals_local, cityHash64(symbol));

-- Insider transactions.
CREATE TABLE IF NOT EXISTS analytics.insider_transactions_local ON CLUSTER analytics_cluster
(
    symbol LowCardinality(String),
    insider String,
    position Nullable(String),
    transaction LowCardinality(String),
    transaction_date Date,
    shares Float64,
    value Nullable(Float64),
    ownership LowCardinality(String),
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/insider_transactions', '{replica}', ingested_at)
PARTITION BY toYYYYMM(transaction_date)
ORDER BY (source, symbol, transaction_date, insider, transaction, shares)
TTL toDate(transaction_date) + INTERVAL 1 YEAR TO VOLUME 'cold',
    toDate(transaction_date) + INTERVAL 10 YEAR DELETE
SETTINGS storage_policy = 'tiered';

CREATE TABLE IF NOT EXISTS analytics.insider_transactions ON CLUSTER analytics_cluster
AS analytics.insider_transactions_local
ENGINE = Distributed(analytics_cluster, analytics, insider_transactions_local, cityHash64(symbol));
