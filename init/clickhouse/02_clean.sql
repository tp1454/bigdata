-- Read-side cleaning for equities: collapse at-least-once replays and drop
-- physically impossible bars before indicators or API consumers use the series.
CREATE OR REPLACE VIEW analytics.equities_prices_clean AS
SELECT symbol, ts, open, high, low, close, volume
FROM analytics.equities_prices
FINAL
WHERE open > 0
  AND high > 0
  AND low > 0
  AND close > 0
  AND high >= low;
