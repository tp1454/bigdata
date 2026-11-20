-- Per-shard precomputation. Each replica refreshes its own local table from
-- its local bars, so a symbol (one shard by cityHash64) is never split.
-- Created on each node separately: ON CLUSTER with a refreshable MV crashes
-- the 24.8 DDL worker, so this file is not in the normal init directory.
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.equities_indicators_mv
REFRESH EVERY 5 MINUTE
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts)
AS
WITH bars AS (
    SELECT
        symbol, ts, open, high, low, close, volume,
        lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY ts) AS prev_close,
        row_number() OVER (PARTITION BY symbol ORDER BY ts) AS bar
    FROM analytics.equities_prices_clean_local
),
features AS (
    SELECT
        *,
        greatest(close - prev_close, 0) AS gain,
        greatest(prev_close - close, 0) AS loss,
        greatest(high - low, abs(high - prev_close), abs(low - prev_close)) AS true_range
    FROM bars
),
rolling AS (
    SELECT
        *,
        avg(gain) OVER w14 AS avg_gain,
        avg(loss) OVER w14 AS avg_loss,
        avg(close) OVER w20 AS sma_20,
        avg(close) OVER w50 AS sma_50,
        avg(close) OVER w200 AS sma_200,
        stddevPop(close) OVER w20 AS stddev_20,
        avg(true_range) OVER w14 AS atr_14,
        avg(volume) OVER w20 AS volume_sma_20,
        lagInFrame(close, 1) OVER w AS close_1,
        lagInFrame(close, 5) OVER w AS close_5
    FROM features
    WINDOW
        w AS (PARTITION BY symbol ORDER BY ts),
        w14 AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
        w20 AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
        w50 AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
        w200 AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
)
SELECT
    symbol, ts, close,
    if(bar >= 20, sma_20, NULL) AS sma_20,
    if(bar >= 50, sma_50, NULL) AS sma_50,
    if(bar >= 200, sma_200, NULL) AS sma_200,
    if(bar >= 20, sma_20 + 2 * stddev_20, NULL) AS bb_upper,
    if(bar >= 20, sma_20 - 2 * stddev_20, NULL) AS bb_lower,
    if(bar >= 15, if(avg_loss = 0, 100, 100 - 100 / (1 + avg_gain / avg_loss)), NULL) AS rsi_14_sma,
    if(bar >= 15, atr_14, NULL) AS atr_14_sma,
    if(bar >= 2, close / close_1 - 1, NULL) AS return_1,
    if(bar >= 6, close / close_5 - 1, NULL) AS return_5,
    if(bar >= 21, stddevPop(close / close_1 - 1) OVER w20, NULL) AS volatility_20,
    if(bar >= 20, volume_sma_20, NULL) AS volume_sma_20
FROM rolling
WINDOW w20 AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW);

CREATE TABLE IF NOT EXISTS analytics.equities_indicators_snapshot ON CLUSTER analytics_cluster
AS analytics.equities_indicators_mv
ENGINE = Distributed(analytics_cluster, analytics, equities_indicators_mv, cityHash64(symbol));
