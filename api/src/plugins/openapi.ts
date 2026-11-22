import { openapi } from '@elysia/openapi'

const description = `Query ClickHouse tables populated by Kafka → Flink.

All reads go through \`FINAL\` on the replicated cluster, so duplicated
at-least-once writes are removed before the response is built.

## Two ways to query

- **Native endpoints** (\`/api/*\`) return the pipeline rows almost as stored.
  They are small and stable, and each accepts a few named query parameters.
- **Alpha Vantage compatible routes** expose one documented endpoint per
  function under \`/alphavantage/<FUNCTION>\`, grouped like the
  [Alpha Vantage](https://www.alphavantage.co/documentation/) docs. A
  \`GET /query?function=...\` shim stays for existing clients; \`apikey\` is
  ignored.

## Data notes

- Equities are one-minute Yahoo bars; daily, weekly and monthly series are
  rolled up from them and limited by table retention.
- Crypto OHLCV is daily Binance klines; intraday crypto series are built from
  \`crypto_trades\`.
- FX rates are single values sampled once a minute, so intraday "OHLC" bars
  have equal open/high/low/close for each interval.
- Values are illustrative pipeline output, not a licensed market-data feed.`

export const openapiPlugin = openapi({
  documentation: {
    info: {
      title: 'Analytics API',
      version: '0.1.0',
      description,
    },
    servers: [{ url: 'http://localhost:4000', description: 'Local compose stack' }],
    externalDocs: {
      url: 'https://www.alphavantage.co/documentation/',
      description: 'Alpha Vantage API reference',
    },
    tags: [
      {
        name: 'query',
        description:
          '`GET /query?function=...` compatibility shim. Prefer the per-function routes.',
      },
      {
        name: 'stocks',
        description:
          'Time series, quotes, search, listings, movers and option ratios.',
      },
      {
        name: 'indicators',
        description: 'Technical indicators computed from the equity bars.',
      },
      {
        name: 'fundamentals',
        description:
          'Statements, earnings, corporate actions, insider and 13F holdings.',
      },
      {
        name: 'currencies',
        description: 'Fiat FX and cryptocurrency time series.',
      },
      {
        name: 'economic',
        description: 'Treasury yields, CPI, inflation and unemployment.',
      },
      {
        name: 'commodities',
        description: 'Commodity price history from the macro indicator topic.',
      },
      {
        name: 'health',
        description: 'Liveness check that also pings ClickHouse.',
      },
      {
        name: 'crypto',
        description: 'Raw crypto trades from `analytics.crypto_trades`.',
      },
      {
        name: 'equities',
        description: 'Equity OHLCV bars from `analytics.equities_prices`.',
      },
      {
        name: 'forex',
        description: 'FX rates from `analytics.forex_rates`.',
      },
    ],
    ...({
      'x-tagGroups': [
        {
          name: 'Alpha Vantage compatible',
          tags: [
            'query',
            'stocks',
            'indicators',
            'fundamentals',
            'currencies',
            'economic',
            'commodities',
          ],
        },
        {
          name: 'Native endpoints',
          tags: ['health', 'crypto', 'equities', 'forex'],
        },
      ],
    } as Record<string, unknown>),
  },
})
