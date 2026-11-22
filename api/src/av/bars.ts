import { ch } from '../clickhouse'
import type { Bar } from './core'

type Row = Record<string, unknown>

async function rows(
  query: string,
  params?: Record<string, unknown>,
): Promise<Bar[]> {
  const rs = await ch.query({
    query,
    query_params: params,
    format: 'JSONEachRow',
  })
  const data = await rs.json<Row[]>()
  return (Array.isArray(data) ? data : []).map((r) => ({
    date: String(r.d ?? ''),
    open: Number(r.o),
    high: Number(r.h),
    low: Number(r.l),
    close: Number(r.c),
    volume: Number(r.v ?? 0),
  }))
}

const INTERVALS = {
  '1min': 1,
  '5min': 5,
  '15min': 15,
  '30min': 30,
  '60min': 60,
} as const

export type IntradayInterval = keyof typeof INTERVALS

export const intradayMinutes = (interval: string): number =>
  INTERVALS[interval as IntradayInterval] ?? 0

// Minute bars land in equities_prices_clean (deduped and sanity-checked);
// daily/weekly/monthly roll them up in JS.
export function equityIntraday(
  symbol: string,
  minutes: number,
  limit: number,
): Promise<Bar[]> {
  return rows(
    `SELECT formatDateTime(toTimeZone(toStartOfInterval(ts, INTERVAL ${minutes} MINUTE), 'America/New_York'), '%Y-%m-%d %H:%i:%S') AS d,
            argMin(open, ts) AS o,
            max(high) AS h,
            min(low) AS l,
            argMax(close, ts) AS c,
            sum(volume) AS v
     FROM analytics.equities_prices_clean
     WHERE symbol = {symbol:String}
     GROUP BY d
     ORDER BY d DESC
     LIMIT ${limit}`,
    { symbol },
  )
}

export function equityDaily(symbol: string, limit = 100000): Promise<Bar[]> {
  return rows(
    `SELECT toString(toDate(toTimeZone(ts, 'America/New_York'))) AS d,
            argMin(open, ts) AS o,
            max(high) AS h,
            min(low) AS l,
            argMax(close, ts) AS c,
            sum(volume) AS v
     FROM analytics.equities_prices_clean
     WHERE symbol = {symbol:String}
     GROUP BY d
     ORDER BY d DESC
     LIMIT ${limit}`,
    { symbol },
  )
}

export function fxIntraday(
  pair: string,
  minutes: number,
  limit: number,
): Promise<Bar[]> {
  return rows(
    `SELECT formatDateTime(toStartOfInterval(timestamp, INTERVAL ${minutes} MINUTE), '%Y-%m-%d %H:%i:%S') AS d,
            argMin(rate, timestamp) AS o,
            max(rate) AS h,
            min(rate) AS l,
            argMax(rate, timestamp) AS c,
            0 AS v
     FROM analytics.forex_rates FINAL
     WHERE pair = {pair:String}
     GROUP BY d
     ORDER BY d DESC
     LIMIT ${limit}`,
    { pair },
  )
}

export function fxDaily(pair: string): Promise<Bar[]> {
  return rows(
    `SELECT toString(toDate(timestamp)) AS d,
            argMin(rate, timestamp) AS o,
            max(rate) AS h,
            min(rate) AS l,
            argMax(rate, timestamp) AS c,
            0 AS v
     FROM analytics.forex_rates FINAL
     WHERE pair = {pair:String}
     GROUP BY d
     ORDER BY d DESC`,
    { pair },
  )
}

export function cryptoDaily(pair: string): Promise<Bar[]> {
  return rows(
    `SELECT toString(toDate(timestamp)) AS d,
            argMax(open, ingested_at) AS o,
            argMax(high, ingested_at) AS h,
            argMax(low, ingested_at) AS l,
            argMax(close, ingested_at) AS c,
            argMax(volume, ingested_at) AS v
     FROM analytics.crypto_ohlcv FINAL
     WHERE symbol = {pair:String} AND interval = '1d'
     GROUP BY d
     ORDER BY d DESC`,
    { pair },
  )
}

export function cryptoTradesIntraday(
  pair: string,
  minutes: number,
  limit: number,
): Promise<Bar[]> {
  return rows(
    `SELECT formatDateTime(toStartOfInterval(timestamp, INTERVAL ${minutes} MINUTE), '%Y-%m-%d %H:%i:%S') AS d,
            argMin(price, timestamp) AS o,
            max(price) AS h,
            min(price) AS l,
            argMax(price, timestamp) AS c,
            sum(quantity) AS v
     FROM analytics.crypto_trades FINAL
     WHERE symbol = {pair:String}
     GROUP BY d
     ORDER BY d DESC
     LIMIT ${limit}`,
    { pair },
  )
}

// Bars arrive newest first. Weeks are Monday-to-Sunday buckets and keep the
// last trading day of the bucket as the label, like Alpha Vantage.
const weekKey = (date: string): string =>
  String(Math.floor((Date.parse(date) - Date.UTC(1970, 0, 5)) / 86400000 / 7))
export function bucket(bars: Bar[], period: 'weekly' | 'monthly'): Bar[] {
  const out: Bar[] = []
  let current: Bar | undefined
  let key = ''
  for (const bar of bars) {
    const k =
      period === 'weekly'
        ? weekKey(bar.date)
        : bar.date.slice(0, 7)
    if (k !== key || !current) {
      current = { ...bar }
      out.push(current)
      key = k
      continue
    }
    current.open = bar.open
    current.volume += bar.volume
    current.high = Math.max(current.high, bar.high)
    current.low = Math.min(current.low, bar.low)
  }
  return out
}

export type Action = { action: string; date: string; value: number }

export async function corporateActions(symbol: string): Promise<Action[]> {
  const rs = await ch.query({
    query: `SELECT action, toString(action_date) AS d, value
            FROM analytics.corporate_actions FINAL
            WHERE symbol = {symbol:String}
            ORDER BY action_date ASC`,
    query_params: { symbol },
    format: 'JSONEachRow',
  })
  const data = await rs.json<Row[]>()
  return (Array.isArray(data) ? data : []).map((r) => ({
    action: String(r.action),
    date: String(r.d),
    value: Number(r.value),
  }))
}

export type AdjustedDay = {
  date: string
  raw: Bar
  adj: Bar
  dividend: number
  split: number
}

// Stored bars are as-traded (the producers pass auto_adjust=False), so raw
// prices stay as stored and the adjusted series scales by the split ratios and
// the multiplicative (prevClose - dividend) / prevClose factors of later events.
export function adjustDaily(daysAsc: Bar[], actions: Action[]): AdjustedDay[] {
  const splits = actions.filter((a) => a.action === 'split')
  const dividends = actions.filter((a) => a.action === 'dividend')
  const out: AdjustedDay[] = []

  for (const day of daysAsc) {
    let splitCum = 1
    let splitCoefficient = 1
    for (const split of splits) {
      if (split.date > day.date) splitCum *= split.value
      if (split.date === day.date) splitCoefficient = split.value
    }

    let divFactor = 1
    let dividend = 0
    for (const div of dividends) {
      if (div.date === day.date) dividend = div.value
      if (div.date <= day.date) continue
      const prev = [...daysAsc]
        .reverse()
        .find((bar) => bar.date < div.date)
      if (prev && prev.close > 0) {
        divFactor *= (prev.close - div.value) / prev.close
      }
    }

    const rawBar: Bar = { ...day }
    const factor = splitCum * divFactor
    const adjBar: Bar = {
      date: day.date,
      open: day.open * factor,
      high: day.high * factor,
      low: day.low * factor,
      close: day.close * factor,
      volume: day.volume,
    }

    out.push({
      date: day.date,
      raw: rawBar,
      adj: adjBar,
      dividend,
      split: splitCoefficient,
    })
  }
  return out
}

export function bucketAdjusted(
  daysDesc: AdjustedDay[],
  period: 'weekly' | 'monthly',
): { date: string; bar: Bar; dividend: number; split: number }[] {
  const out: { date: string; bar: Bar; dividend: number; split: number }[] = []
  let key = ''
  for (const day of daysDesc) {
    const k =
      period === 'weekly'
        ? weekKey(day.date)
        : day.date.slice(0, 7)
    if (k !== key || out.length === 0) {
      out.push({
        date: day.date,
        bar: { ...day.adj },
        dividend: day.dividend,
        split: day.split,
      })
      key = k
      continue
    }
    const current = out[out.length - 1]
    current.bar.open = day.adj.open
    current.bar.high = Math.max(current.bar.high, day.adj.high)
    current.bar.low = Math.min(current.bar.low, day.adj.low)
    current.bar.volume += day.adj.volume
    current.dividend += day.dividend
    current.split *= day.split
  }
  return out
}
