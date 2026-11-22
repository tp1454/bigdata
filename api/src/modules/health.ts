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
      description:
        'Returns `{ ok, service }`. `ok` is the result of a `ping()` against ClickHouse shard 1.',
    },
  },
)
