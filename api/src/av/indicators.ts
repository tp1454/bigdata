import type { Bar } from './core'
import { csvText, need, oneOf, px, wantsCsv, type Params } from './core'
import type { AvPayload } from './core'
import {
  bucket,
  equityDaily,
  equityIntraday,
  intradayMinutes,
} from './bars'

export type Kind = 'SMA' | 'EMA' | 'WMA' | 'RSI' | 'ATR' | 'BBANDS' | 'MACD'

const INDICATOR_NAMES: Record<Kind, string> = {
  SMA: 'Simple Moving Average (SMA)',
  EMA: 'Exponential Moving Average (EMA)',
  WMA: 'Weighted Moving Average (WMA)',
  RSI: 'Relative Strength Index (RSI)',
  ATR: 'Average True Range (ATR)',
  BBANDS: 'Bollinger Bands (BBANDS)',
  MACD: 'Moving Average Convergence / Divergence (MACD)',
}

type Row = { date: string; values: Record<string, number | null> }

const sma = (values: number[], n: number): (number | null)[] => {
  const out: (number | null)[] = []
  let sum = 0
  for (let i = 0; i < values.length; i += 1) {
    sum += values[i]
    if (i >= n) sum -= values[i - n]
    out.push(i >= n - 1 ? sum / n : null)
  }
  return out
}

const ema = (values: number[], n: number): (number | null)[] => {
  const out: (number | null)[] = []
  const k = 2 / (n + 1)
  let prev: number | null = null
  let sum = 0
  for (let i = 0; i < values.length; i += 1) {
    if (i < n) sum += values[i]
    if (i === n - 1) prev = sum / n
    else if (i >= n && prev !== null) prev = values[i] * k + prev * (1 - k)
    out.push(i >= n - 1 ? prev : null)
  }
  return out
}

const wma = (values: number[], n: number): (number | null)[] => {
  const out: (number | null)[] = []
  const norm = (n * (n + 1)) / 2
  for (let i = 0; i < values.length; i += 1) {
    if (i < n - 1) {
      out.push(null)
      continue
    }
    let sum = 0
    for (let j = 0; j < n; j += 1) sum += values[i - j] * (n - j)
    out.push(sum / norm)
  }
  return out
}

const rsi = (values: number[], n: number): (number | null)[] => {
  const out: (number | null)[] = Array(values.length).fill(null)
  if (values.length <= n) return out
  let gain = 0
  let loss = 0
  for (let i = 1; i <= n; i += 1) {
    const change = values[i] - values[i - 1]
    if (change >= 0) gain += change
    else loss -= change
  }
  gain /= n
  loss /= n
  out[n] = loss === 0 ? 100 : 100 - 100 / (1 + gain / loss)
  for (let i = n + 1; i < values.length; i += 1) {
    const change = values[i] - values[i - 1]
    gain = (gain * (n - 1) + Math.max(change, 0)) / n
    loss = (loss * (n - 1) + Math.max(-change, 0)) / n
    out[i] = loss === 0 ? 100 : 100 - 100 / (1 + gain / loss)
  }
  return out
}

const atr = (bars: Bar[], n: number): (number | null)[] => {
  const out: (number | null)[] = Array(bars.length).fill(null)
  const tr = bars.map((bar, i) => {
    if (i === 0) return bar.high - bar.low
    const prev = bars[i - 1].close
    return Math.max(
      bar.high - bar.low,
      Math.abs(bar.high - prev),
      Math.abs(bar.low - prev),
    )
  })
  if (bars.length < n) return out
  let avg = tr.slice(0, n).reduce((a, b) => a + b, 0) / n
  out[n - 1] = avg
  for (let i = n; i < bars.length; i += 1) {
    avg = (avg * (n - 1) + tr[i]) / n
    out[i] = avg
  }
  return out
}

const bbands = (
  values: number[],
  n: number,
  up: number,
  down: number,
): { upper: (number | null)[]; middle: (number | null)[]; lower: (number | null)[] } => {
  const middle = sma(values, n)
  const upper: (number | null)[] = []
  const lower: (number | null)[] = []
  for (let i = 0; i < values.length; i += 1) {
    if (middle[i] === null) {
      upper.push(null)
      lower.push(null)
      continue
    }
    let variance = 0
    for (let j = i - n + 1; j <= i; j += 1) {
      variance += (values[j] - (middle[i] as number)) ** 2
    }
    const sd = Math.sqrt(variance / n)
    upper.push((middle[i] as number) + up * sd)
    lower.push((middle[i] as number) - down * sd)
  }
  return { upper, middle, lower }
}

const macd = (
  values: number[],
  fast: number,
  slow: number,
  signal: number,
): { macd: (number | null)[]; signal: (number | null)[]; hist: (number | null)[] } => {
  const fastEma = ema(values, fast)
  const slowEma = ema(values, slow)
  const line = values.map((_, i) =>
    fastEma[i] !== null && slowEma[i] !== null
      ? (fastEma[i] as number) - (slowEma[i] as number)
      : null,
  )
  const first = line.findIndex((v) => v !== null)
  const signalLine: (number | null)[] = Array(values.length).fill(null)
  if (first >= 0) {
    const segment = line.slice(first) as number[]
    const computed = ema(segment, signal)
    computed.forEach((v, i) => {
      signalLine[first + i] = v
    })
  }
  const hist = line.map((v, i) =>
    v !== null && signalLine[i] !== null ? v - (signalLine[i] as number) : null,
  )
  return { macd: line, signal: signalLine, hist }
}

async function loadSeries(p: Params, symbol: string): Promise<Bar[]> {
  const interval = p.interval ?? 'daily'
  if (intradayMinutes(interval)) {
    const bars = await equityIntraday(
      symbol,
      intradayMinutes(interval),
      100000,
    )
    return bars.slice().reverse()
  }
  const daily = (await equityDaily(symbol)).slice().reverse()
  if (interval === 'weekly' || interval === 'monthly') {
    return bucket(daily.slice().reverse(), interval).slice().reverse()
  }
  if (interval !== 'daily') {
    oneOf(interval, 'interval', ['daily', 'weekly', 'monthly'])
  }
  return daily
}

export async function indicatorPayload(kind: Kind, p: Params): Promise<AvPayload> {
  const symbol = need(p, 'symbol').toUpperCase()
  const interval = p.interval ?? 'daily'
  const seriesType = oneOf(p.series_type ?? 'close', 'series_type', [
    'close',
    'open',
    'high',
    'low',
  ])
  const period = Math.max(1, Number(p.time_period ?? 20) || 20)

  const bars = await loadSeries(p, symbol)
  if (bars.length === 0) {
    return {
      json: { [`Technical Analysis: ${kind}`]: {} },
    }
  }
  const values = bars.map((bar) => bar[seriesType])

  let points: Row[] = []
  if (kind === 'SMA' || kind === 'EMA' || kind === 'WMA') {
    const computed =
      kind === 'SMA' ? sma(values, period) : kind === 'EMA' ? ema(values, period) : wma(values, period)
    points = bars.map((bar, i) => ({
      date: bar.date,
      values: { [kind]: computed[i] },
    }))
  } else if (kind === 'RSI') {
    const computed = rsi(values, period)
    points = bars.map((bar, i) => ({ date: bar.date, values: { RSI: computed[i] } }))
  } else if (kind === 'ATR') {
    const computed = atr(bars, period)
    points = bars.map((bar, i) => ({ date: bar.date, values: { ATR: computed[i] } }))
  } else if (kind === 'BBANDS') {
    const up = Number(p.nbdevup ?? 2) || 2
    const down = Number(p.nbdevdn ?? 2) || 2
    const computed = bbands(values, period, up, down)
    points = bars.map((bar, i) => ({
      date: bar.date,
      values: {
        'Real Upper Band': computed.upper[i],
        'Real Middle Band': computed.middle[i],
        'Real Lower Band': computed.lower[i],
      },
    }))
  } else {
    const fast = Math.max(1, Number(p.fastperiod ?? 12) || 12)
    const slow = Math.max(fast + 1, Number(p.slowperiod ?? 26) || 26)
    const signal = Math.max(1, Number(p.signalperiod ?? 9) || 9)
    const computed = macd(values, fast, slow, signal)
    points = bars.map((bar, i) => ({
      date: bar.date,
      values: {
        MACD: computed.macd[i],
        MACD_Signal: computed.signal[i],
        MACD_Hist: computed.hist[i],
      },
    }))
  }

  const seriesKey = `Technical Analysis: ${kind}`
  const filled = points
    .filter((point) => Object.values(point.values).every((v) => v !== null))
    .reverse()

  const series: Record<string, Record<string, string>> = {}
  for (const point of filled) {
    series[point.date] = Object.fromEntries(
      Object.entries(point.values).map(([key, value]) => [key, px(value as number)]),
    )
  }

  const keys = Object.keys(filled[0]?.values ?? points[0]?.values ?? {})
  const json = {
    'Meta Data': {
      '1: Symbol': symbol,
      '2: Indicator': INDICATOR_NAMES[kind],
      '3: Last Refreshed': filled[0]?.date ?? bars[bars.length - 1].date,
      '4: Interval': interval,
      '5: Time Period': kind === 'MACD' ? Math.max(1, Number(p.slowperiod ?? 26) || 26) : period,
      '6: Series Type': seriesType,
      '7: Time Zone': 'US/Eastern',
    },
    [seriesKey]: series,
  }

  if (!wantsCsv(p)) return { json }
  return {
    json,
    csv: csvText(
      ['timestamp', ...keys],
      filled.map((point) => [
        point.date,
        ...keys.map((key) => px(point.values[key] as number)),
      ]),
    ),
  }
}
