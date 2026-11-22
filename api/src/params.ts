import { t } from 'elysia'

export const symbol = t.String({
  minLength: 1,
  maxLength: 20,
  description: 'Ticker symbol, for example `AAPL`.',
  examples: ['AAPL'],
})
export const pair = t.String({
  minLength: 3,
  maxLength: 12,
  description: 'Currency pair without a separator, for example `EURUSD`.',
  examples: ['EURUSD'],
})
export const source = t.Optional(
  t.String({
    minLength: 1,
    maxLength: 32,
    description: 'Trade source. Defaults to `binance`.',
    examples: ['binance'],
  }),
)
export const limit = t.Optional(
  t.Numeric({
    description: 'Maximum rows to return, clamped to 1-1000. Defaults to 100.',
    examples: [100],
  }),
)

export const MAX_LIMIT = 1000

// limit is clamped and truncated before it is inlined into LIMIT.
export function clampLimit(value: unknown, fallback = 100): number {
  const parsed = Number(value ?? fallback)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.trunc(parsed), 1), MAX_LIMIT)
}
