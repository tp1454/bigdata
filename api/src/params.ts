import { t } from 'elysia'

export const symbol = t.String({ minLength: 1, maxLength: 20 })
export const pair = t.String({ minLength: 3, maxLength: 12 })
export const source = t.Optional(t.String({ minLength: 1, maxLength: 32 }))
export const limit = t.Optional(t.Numeric())

export const MAX_LIMIT = 1000

// limit is clamped and truncated before it is inlined into LIMIT.
export function clampLimit(value: unknown, fallback = 100): number {
  const parsed = Number(value ?? fallback)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.trunc(parsed), 1), MAX_LIMIT)
}
