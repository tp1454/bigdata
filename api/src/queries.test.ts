import { describe, expect, it } from 'bun:test'

import { app } from './app'

const queries = (globalThis as unknown as { chQueries: string[] }).chQueries

const request = (path: string) =>
  app.handle(new Request(`http://localhost${path}`))

describe('reads on the replicated cluster', () => {
  it.each([
    ['/api/equities/daily?symbol=AAPL', 'FROM analytics.equities_prices_clean'],
    ['/api/crypto/latest?symbol=BTCUSDT', 'FROM crypto_trades FINAL'],
    ['/api/crypto/trades?symbol=BTCUSDT', 'FROM crypto_trades FINAL'],
    ['/api/forex/latest?pair=EURUSD', 'FROM forex_rates FINAL'],
  ])('%s reads %s', async (path, source) => {
    queries.length = 0

    const res = await request(path)

    expect(res.status).toBe(200)
    expect(queries).toHaveLength(1)
    expect(queries[0]).toContain(source)
  })
})
