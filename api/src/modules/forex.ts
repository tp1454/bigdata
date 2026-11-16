import { Elysia, t } from 'elysia'

import { ch } from '../clickhouse'
import { pair } from '../params'

interface Rate {
  pair: string
  timestamp: string
  rate: number
  source: string
}

export const forex = new Elysia({ prefix: '/api/forex' }).get(
  '/latest',
  async ({ query }) => {
    const rs = await ch.query({
      query: `SELECT pair, timestamp, rate, source
              FROM forex_rates
              WHERE pair = {pair:String}
              ORDER BY timestamp DESC
              LIMIT 1`,
      query_params: { pair: query.pair },
      format: 'JSONEachRow',
    })
    const data = await rs.json<Rate[]>()

    return {
      meta: { pair: query.pair, count: data.length },
      data,
    }
  },
  {
    query: t.Object({ pair }),
    detail: {
      tags: ['forex'],
      operationId: 'forex.latest',
      summary: 'Latest rate for a currency pair',
    },
  },
)
