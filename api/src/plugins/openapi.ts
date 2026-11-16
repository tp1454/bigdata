import { openapi } from '@elysia/openapi'

export const openapiPlugin = openapi({
  documentation: {
    info: {
      title: 'Analytics API',
      version: '0.1.0',
      description: 'Query ClickHouse tables populated by Kafka -> Flink.',
    },
    tags: [
      { name: 'health', description: 'Service health' },
      { name: 'crypto', description: 'Crypto trades' },
      { name: 'equities', description: 'Equity OHLCV bars' },
      { name: 'forex', description: 'FX rates' },
    ],
  },
})
