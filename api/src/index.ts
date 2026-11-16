import { Elysia, t } from 'elysia'
import { cors } from '@elysiajs/cors'
import { openapi } from '@elysia/openapi'
import { createClient } from '@clickhouse/client'

const ch = createClient({
  url: process.env.CLICKHOUSE_URL ?? 'http://clickhouse:8123',
  username: process.env.CLICKHOUSE_USER ?? 'default',
  password: process.env.CLICKHOUSE_PASSWORD ?? 'clickhouse',
  database: process.env.CLICKHOUSE_DB ?? 'analytics',
  clickhouse_settings: {
    // return UInt64 ids as JSON numbers instead of quoted strings
    output_format_json_quote_64bit_integers: 0,
  },
})

const app = new Elysia()
  .use(cors())
  .use(openapi({
    documentation: {
      info: {
        title: 'Analytics API',
        version: '0.1.0',
        description: 'Query ClickHouse tables populated by Kafka -> Flink.',
      },
      tags: [
        { name: 'health', description: 'Service health' },
        { name: 'crypto', description: 'Crypto trades' },
      ],
    },
  }))
  .get('/health', async () => ({
    ok: await ch.ping(),
    service: 'analytics-api',
  }), {
    detail: {
      tags: ['health'],
      operationId: 'health.check',
      summary: 'Liveness and ClickHouse ping',
    },
  })
  .get('/api/latest', async ({ query }) => {
    const rs = await ch.query({
      query: `SELECT timestamp, symbol, price, quantity
              FROM crypto_trades
              WHERE source = 'binance' AND symbol = {symbol:String}
              ORDER BY timestamp DESC
              LIMIT 1`,
      query_params: { symbol: query.symbol },
      format: 'JSONEachRow',
    })
    return await rs.json()
  }, {
    query: t.Object({ symbol: t.String({ minLength: 1, maxLength: 20 }) }),
    detail: {
      tags: ['crypto'],
      operationId: 'crypto.latest',
      summary: 'Latest trade for a symbol',
    },
  })
  .get('/api/trades', async ({ query }) => {
    // limit is a clamped number, so it is safe to inline; symbol stays parameterized.
    const limit = Math.min(Math.max(Number(query.limit ?? 100), 1), 1000)
    const rs = await ch.query({
      query: `SELECT timestamp, symbol, price, quantity, trade_id, source
              FROM crypto_trades
              WHERE source = 'binance' AND symbol = {symbol:String}
              ORDER BY timestamp DESC
              LIMIT ${limit}`,
      query_params: { symbol: query.symbol },
      format: 'JSONEachRow',
    })
    return await rs.json()
  }, {
    query: t.Object({
      symbol: t.String({ minLength: 1, maxLength: 20 }),
      limit: t.Optional(t.Numeric()),
    }),
    detail: {
      tags: ['crypto'],
      operationId: 'crypto.trades',
      summary: 'Recent trades for a symbol',
    },
  })
  .onError(({ code, error, set }) => {
    if (code === 'VALIDATION') {
      set.status = 400
      return { error: error.message }
    }
    console.error(error)
    set.status = 502
    return { error: 'clickhouse query failed' }
  })
  .listen({ port: 4000, hostname: '0.0.0.0' })

console.log(`API listening on ${app.server?.hostname}:${app.server?.port}`)
