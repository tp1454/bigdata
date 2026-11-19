-- Dividends and splits, one row per event.
CREATE TABLE IF NOT EXISTS analytics.corporate_actions
(
    symbol LowCardinality(String),
    action LowCardinality(String),
    action_date Date,
    value Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (source, symbol, action, action_date);

-- Financial statement line items (annual/quarterly) plus earnings history.
CREATE TABLE IF NOT EXISTS analytics.fundamentals
(
    symbol LowCardinality(String),
    statement LowCardinality(String),
    period LowCardinality(String),
    end_date Date,
    line_item String,
    value Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (source, symbol, statement, period, end_date, line_item);

-- Insider transactions.
CREATE TABLE IF NOT EXISTS analytics.insider_transactions
(
    symbol LowCardinality(String),
    insider String,
    position String,
    transaction String,
    transaction_date Date,
    shares Float64,
    value Nullable(Float64),
    ownership String,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (source, symbol, transaction_date, insider, transaction, shares);
