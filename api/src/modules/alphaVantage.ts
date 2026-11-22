import { Elysia } from 'elysia'

import { AvError, type Params } from '../av/core'
import {
  allParams,
  routesPlugin,
  wrap,
  type Handler,
  type RouteDef,
} from './av/shared'
import { stocksRoutes } from './av/stocks'
import { indicatorsRoutes } from './av/indicators'
import { fundamentalsRoutes } from './av/fundamentals'
import { currenciesRoutes } from './av/currencies'
import { macroRoutes } from './av/macro'

const ROUTES: RouteDef[] = [
  ...stocksRoutes,
  ...indicatorsRoutes,
  ...fundamentalsRoutes,
  ...currenciesRoutes,
  ...macroRoutes,
]

const byFunction = new Map(ROUTES.map((route) => [route.fn, route]))

const supported = ROUTES.map((route) => route.fn).join(', ')

const dispatch: Handler = (p: Params) => {
  const fn = (p.function ?? '').toUpperCase()
  if (!fn) {
    throw new AvError('Invalid API call: missing required parameter function.')
  }
  const route = byFunction.get(fn)
  if (!route) {
    throw new AvError(
      `Invalid API call: ${fn} is not served by this API. Supported functions: ${supported}.`,
    )
  }
  return route.handler(p)
}

const description = `Compatibility shim for clients that call Alpha Vantage's
\`GET /query?function=...\`. Each function also has its own documented route at
\`/alphavantage/<FUNCTION>\`; prefer those for new integrations.

\`apikey\` is accepted and ignored. Unsupported functions return HTTP 200 with
\`{ "Error Message": "..." }\`, matching Alpha Vantage.`

export const alphaVantage = new Elysia()
  .use(routesPlugin('/alphavantage', stocksRoutes))
  .use(routesPlugin('/alphavantage', indicatorsRoutes))
  .use(routesPlugin('/alphavantage', fundamentalsRoutes))
  .use(routesPlugin('/alphavantage', currenciesRoutes))
  .use(routesPlugin('/alphavantage', macroRoutes))
  .get('/query', wrap(dispatch), {
    query: allParams,
    detail: {
      tags: ['query'],
      operationId: 'av.query',
      summary: 'Alpha Vantage /query compatibility shim',
      description,
    },
  })
