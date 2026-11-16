import { describe, expect, it } from 'bun:test'

import { app } from './app'

const request = (path: string) =>
  app.handle(new Request(`http://localhost${path}`))

describe('api', () => {
  it('rejects an empty symbol before touching ClickHouse', async () => {
    const res = await request('/api/crypto/latest?symbol=')
    expect(res.status).toBe(400)
  })

  it('returns 404 for an unknown path', async () => {
    const res = await request('/nope')
    expect(res.status).toBe(404)
  })

  it('publishes the routes in the OpenAPI spec', async () => {
    const res = await request('/openapi/json')
    expect(res.status).toBe(200)

    const spec = (await res.json()) as { paths: Record<string, unknown> }
    expect(spec.paths['/api/crypto/latest']).toBeDefined()
    expect(spec.paths['/api/equities/daily']).toBeDefined()
    expect(spec.paths['/api/forex/latest']).toBeDefined()
  })
})
