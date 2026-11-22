export type Params = Record<string, string | undefined>

export type Bar = {
  date: string
  open: number
  high: number
  low: number
  close: number
  volume: number
}

export type AvPayload = { json: unknown; csv?: string }

export class AvError extends Error {}

export function need(p: Params, name: string): string {
  const value = p[name]
  if (!value) {
    throw new AvError(`Invalid API call: missing required parameter ${name}.`)
  }
  return value
}

export function oneOf<T extends string>(
  value: string,
  name: string,
  allowed: readonly T[],
): T {
  if (!(allowed as readonly string[]).includes(value)) {
    throw new AvError(
      `Invalid ${name}: ${value}. Supported values: ${allowed.join(', ')}.`,
    )
  }
  return value as T
}

export const px = (value: number): string => Number(value).toFixed(4)
export const vol = (value: number): string => String(Math.round(value))
export const ratio = (value: number): string => String(Number(value.toFixed(2)))
export const pct = (value: number): string => `${value.toFixed(4)}%`
export const raw = (value: unknown): string => String(value ?? '')

export const wantsCsv = (p: Params): boolean => p.datatype === 'csv'

export const csvText = (
  header: string[],
  rows: (string | number)[][],
): string => [header, ...rows].map((row) => row.join(',')).join('\n') + '\n'

export const csvResponse = (body: string): Response =>
  new Response(body, {
    headers: { 'content-type': 'text/csv; charset=utf-8' },
  })

export const pickLimit = (p: Params): number =>
  p.outputsize === 'full' ? 100000 : 100

export function symbolsFrom(rawList: string): string[] {
  return rawList
    .split(',')
    .map((s) => s.trim().toUpperCase())
    .filter((s) => /^[A-Z0-9.-]{1,20}$/.test(s))
}

export const isCrypto = (code: string): boolean =>
  ['BTC', 'ETH', 'SOL'].includes(code.toUpperCase())
