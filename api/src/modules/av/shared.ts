import { Elysia, t } from 'elysia'

import { AvError, type AvPayload, type Params } from '../../av/core'

export type Handler = (
  p: Params,
) => AvPayload | Response | Promise<AvPayload | Response>

export type RouteDef = {
  fn: string
  tag: string
  summary: string
  description: string
  schema: ReturnType<typeof params>
  handler: Handler
}

const PARAMS: Record<string, string> = {
  function: 'Alpha Vantage function name, for example `TIME_SERIES_DAILY`.',
  apikey: 'Accepted for client compatibility and ignored.',
  symbol: 'Equity ticker or cryptocurrency code, for example `AAPL` or `BTC`.',
  keywords: 'Company name or symbol fragment for `SYMBOL_SEARCH`.',
  interval:
    '`1min`, `5min`, `15min`, `30min`, `60min`, `daily`, `weekly`, `monthly`, or an economic interval.',
  outputsize: '`compact` (latest 100 rows, default) or `full`.',
  datatype: '`json` (default) or `csv`.',
  time_period: 'Look-back window for the indicator.',
  series_type: '`close` (default), `open`, `high` or `low`.',
  nbdevup: 'Upper Bollinger band multiplier, default `2`.',
  nbdevdn: 'Lower Bollinger band multiplier, default `2`.',
  fastperiod: 'MACD fast EMA period, default `12`.',
  slowperiod: 'MACD slow EMA period, default `26`.',
  signalperiod: 'MACD signal period, default `9`.',
  from_currency: 'Source currency or crypto code, for example `EUR` or `BTC`.',
  to_currency: 'Destination currency or crypto code, for example `USD`.',
  from_symbol: 'FX source currency code, for example `EUR`.',
  to_symbol: 'FX destination currency code, for example `USD`.',
  market: 'Crypto market currency, default `USD`.',
  maturity:
    'Treasury maturity: `3month`, `2year`, `5year`, `7year`, `10year` or `30year`.',
  state: 'Listing status: `active` (default) or `delisted`.',
  date: '`YYYY-MM-DD` date filter.',
  from: '`YYYY-MM-DD` lower bound for insider transactions.',
  month: '`YYYY-MM` month selector. Accepted for compatibility.',
  entitlement: 'Accepted for compatibility; not used.',
}

export function params(...names: string[]) {
  const shape: Record<string, ReturnType<typeof t.Optional>> = {}
  for (const name of names) {
    shape[name] = t.Optional(t.String({ description: PARAMS[name] }))
  }
  return t.Object(shape, { additionalProperties: true })
}

export const allParams = params(...Object.keys(PARAMS))

export const wrap =
  (handler: Handler) =>
  async ({ query }: { query: Params }) => {
    try {
      const result = await handler(query)
      return result instanceof Response ? result : result.json
    } catch (error) {
      if (error instanceof AvError) {
        return { 'Error Message': error.message }
      }
      throw error
    }
  }

// Registers one documented route per function, so each shows up as its own
// operation in the OpenAPI UI instead of a single dispatcher.
export const routesPlugin = (prefix: string, routes: RouteDef[]) => {
  let app = new Elysia({ prefix })
  for (const route of routes) {
    app = app.get(`/${route.fn}`, wrap(route.handler), {
      query: route.schema,
      detail: {
        tags: [route.tag],
        operationId: `av.${route.fn.toLowerCase()}`,
        summary: route.summary,
        description: route.description,
      },
    })
  }
  return app
}
