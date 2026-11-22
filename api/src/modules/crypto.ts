import { Elysia, t } from 'elysia'

import { ch } from '../clickhouse'
import { clampLimit, limit, source, symbol } from '../params'

interface Trade {
  timestamp: string
  symbol: string
  price: number
  quantity: number
  trade_id: number
  source: string
}

const DEFAULT_SOURCE = 'binance'

export const crypto = new Elysia({ prefix: '/api/crypto' })
  .get(
    '/latest',
    async ({ query }) => {
      const tradeSource = query.source ?? DEFAULT_SOURCE
      const rs = await ch.query({
        query: `SELECT timestamp, symbol, price, quantity, trade_id, source
                FROM crypto_trades FINAL
                WHERE symbol = {symbol:String} AND source = {source:String}
                ORDER BY timestamp DESC
                LIMIT 1`,
        query_params: { symbol: query.symbol, source: tradeSource },
        format: 'JSONEachRow',
      })
      const data = await rs.json<Trade>()

      return {
        meta: { symbol: query.symbol, source: tradeSource, count: data.length },
        data,
      }
    },
    {
      query: t.Object({ symbol, source }),
      detail: {
        tags: ['crypto'],
        operationId: 'crypto.latest',
        summary: 'Latest trade for a symbol',
        description:
          'Newest row for `symbol` and `source`. Example: `GET /api/crypto/latest?symbol=BTCUSDT`.'
      },
    },
  )
  .get(
    '/trades',
    async ({ query }) => {
      const tradeSource = query.source ?? DEFAULT_SOURCE
      const rowLimit = clampLimit(query.limit)
      const rs = await ch.query({
        query: `SELECT timestamp, symbol, price, quantity, trade_id, source
                FROM crypto_trades FINAL
                WHERE symbol = {symbol:String} AND source = {source:String}
                ORDER BY timestamp DESC
                LIMIT ${rowLimit}`,
        query_params: { symbol: query.symbol, source: tradeSource },
        format: 'JSONEachRow',
      })
      const data = await rs.json<Trade>()

      return {
        meta: {
          symbol: query.symbol,
          source: tradeSource,
          limit: rowLimit,
          count: data.length,
        },
        data,
      }
    },
    {
      query: t.Object({ symbol, source, limit }),
      detail: {
        tags: ['crypto'],
        operationId: 'crypto.trades',
        summary: 'Recent trades for a symbol',
        description:
          'Newest trades for `symbol` and `source`, newest first. Example: `GET /api/crypto/trades?symbol=BTCUSDT&limit=50`.'
      },
    },
  )
