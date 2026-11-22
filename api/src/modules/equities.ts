import { Elysia, t } from 'elysia'

import { ch } from '../clickhouse'
import { clampLimit, limit, symbol } from '../params'

interface Bar {
  ts: string
  open: number
  high: number
  low: number
  close: number
  volume: number
}

export const equities = new Elysia({ prefix: '/api/equities' }).get(
  '/daily',
  async ({ query }) => {
    const rowLimit = clampLimit(query.limit)
    const rs = await ch.query({
      query: `SELECT ts, open, high, low, close, volume
              FROM equities_prices FINAL
              WHERE symbol = {symbol:String}
              ORDER BY ts DESC
              LIMIT ${rowLimit}`,
      query_params: { symbol: query.symbol },
      format: 'JSONEachRow',
    })
    const data = await rs.json<Bar[]>()

    return {
      meta: { symbol: query.symbol, limit: rowLimit, count: data.length },
      data,
    }
  },
  {
    query: t.Object({ symbol, limit }),
    detail: {
      tags: ['equities'],
      operationId: 'equities.daily',
      summary: 'Latest OHLCV bars for an equity',
      description:
        'Newest rows from `equities_prices` for `symbol`, newest first. The producer stores one-minute Yahoo bars. Example: `GET /api/equities/daily?symbol=AAPL&limit=100`.'
    },
  },
)
