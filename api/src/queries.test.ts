import { describe, expect, it } from 'bun:test'

import { app } from './app'

const queries = (globalThis as unknown as { chQueries: string[] }).chQueries

const request = (path: string) =>
  app.handle(new Request(`http://localhost${path}`))

describe('reads on the replicated cluster', () => {
  it.each([
    '/api/equities/daily?symbol=AAPL',
    '/api/crypto/latest?symbol=BTCUSDT',
    '/api/crypto/trades?symbol=BTCUSDT',
    '/api/forex/latest?pair=EURUSD',
  ])('%s dedupes with FINAL', async (path) => {
    queries.length = 0

    const res = await request(path)

    expect(res.status).toBe(200)
    expect(queries).toHaveLength(1)
    expect(queries[0]).toMatch(/FROM \w+ FINAL/)
  })
})
