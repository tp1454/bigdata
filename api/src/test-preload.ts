import { mock } from 'bun:test'

process.env.CLICKHOUSE_PASSWORD ??= 'test'

type Responder = (
  query: string,
  params?: Record<string, unknown>,
) => unknown

const queries: string[] = []
const state = { respond: ((): unknown[] => []) as Responder }

;(globalThis as { chQueries?: string[] }).chQueries = queries
;(globalThis as { chTest?: { respond: Responder } }).chTest = state

mock.module('@clickhouse/client', () => ({
  createClient: () => ({
    query: async ({
      query,
      query_params,
    }: {
      query: string
      query_params?: Record<string, unknown>
    }) => {
      queries.push(query)
      return { json: async () => state.respond(query, query_params) }
    },
    ping: async () => true,
  }),
}))
