import { Elysia } from 'elysia'

// Registered as a global handler so it also covers routes defined after it.
export const errors = new Elysia({ name: 'errors' }).onError(
  { as: 'global' },
  ({ code, error, set }) => {
    if (code === 'VALIDATION') {
      set.status = 400
      return { error: error.message }
    }

    if (code === 'NOT_FOUND') {
      set.status = 404
      return { error: 'not found' }
    }

    console.error(error)
    set.status = 502
    return { error: 'clickhouse query failed' }
  },
)
