import { Elysia } from 'elysia'

import { ch } from '../clickhouse'

export const health = new Elysia().get(
  '/health',
  async () => ({
    ok: await ch.ping(),
    service: 'analytics-api',
  }),
  {
    detail: {
      tags: ['health'],
      operationId: 'health.check',
      summary: 'Liveness and ClickHouse ping',
    },
  },
)
