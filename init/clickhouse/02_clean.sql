-- Read-side cleaning for equities: collapse at-least-once replays and drop
-- physically impossible bars before indicators or API consumers use the series.
-- argMax on ingested_at replaces FINAL so the view does not force a full merge.

-- Cluster-wide read path (queries the Distributed table).
CREATE OR REPLACE VIEW analytics.equities_prices_clean ON CLUSTER analytics_cluster AS
SELECT symbol, ts, open, high, low, close, volume
FROM (
    SELECT
        symbol,
        ts,
        argMax(open, ingested_at) AS open,
        argMax(high, ingested_at) AS high,
        argMax(low, ingested_at) AS low,
        argMax(close, ingested_at) AS close,
        argMax(volume, ingested_at) AS volume
    FROM analytics.equities_prices
    GROUP BY symbol, ts
)
WHERE open > 0
  AND high > 0
  AND low > 0
  AND close > 0
  AND high >= greatest(open, close, low)
  AND low <= least(open, close, high)
  AND volume >= 0;

-- Same cleaning for one shard's local rows. The refreshable materialized
-- view reads this so each shard only recomputes its own symbols.
CREATE OR REPLACE VIEW analytics.equities_prices_clean_local ON CLUSTER analytics_cluster AS
SELECT symbol, ts, open, high, low, close, volume
FROM (
    SELECT
        symbol,
        ts,
        argMax(open, ingested_at) AS open,
        argMax(high, ingested_at) AS high,
        argMax(low, ingested_at) AS low,
        argMax(close, ingested_at) AS close,
        argMax(volume, ingested_at) AS volume
    FROM analytics.equities_prices_local
    GROUP BY symbol, ts
)
WHERE open > 0
  AND high > 0
  AND low > 0
  AND close > 0
  AND high >= greatest(open, close, low)
  AND low <= least(open, close, high)
  AND volume >= 0;
