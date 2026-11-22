import { ch } from '../clickhouse'
import {
  AvError,
  csvResponse,
  csvText,
  isCrypto,
  need,
  oneOf,
  pickLimit,
  px,
  vol,
  wantsCsv,
  type AvPayload,
  type Bar,
  type Params,
} from './core'
import {
  bucket,
  cryptoDaily,
  cryptoTradesIntraday,
  fxDaily,
  fxIntraday,
  intradayMinutes,
} from './bars'

const CURRENCY_NAMES: Record<string, string> = {
  USD: 'United States Dollar',
  EUR: 'Euro',
  GBP: 'British Pound Sterling',
  JPY: 'Japanese Yen',
  AUD: 'Australian Dollar',
  CAD: 'Canadian Dollar',
  CHF: 'Swiss Franc',
  CNY: 'Chinese Yuan',
  BTC: 'Bitcoin',
  ETH: 'Ethereum',
  SOL: 'Solana',
}

const COIN_IDS: Record<string, string> = {
  BTC: 'bitcoin',
  ETH: 'ethereum',
  SOL: 'solana',
}

const CRYPTO_NAMES: Record<string, string> = {
  BTC: 'Bitcoin',
  ETH: 'Ethereum',
  SOL: 'Solana',
}

const name = (code: string): string => CURRENCY_NAMES[code] ?? code

type Row = Record<string, unknown>

async function rows(query: string, params?: Record<string, unknown>): Promise<Row[]> {
  const rs = await ch.query({
    query,
    query_params: params,
    format: 'JSONEachRow',
  })
  const data = await rs.json<Row>()
  return Array.isArray(data) ? data : []
}

const fxFields = (bar: Bar): Record<string, string> => ({
  '1. open': px(bar.open),
  '2. high': px(bar.high),
  '3. low': px(bar.low),
  '4. close': px(bar.close),
})

const cryptoFields = (bar: Bar): Record<string, string> => ({
  ...fxFields(bar),
  '5. volume': vol(bar.volume),
})

const toMap = (
  bars: Bar[],
  fields: (bar: Bar) => Record<string, string>,
): Record<string, Record<string, string>> => {
  const out: Record<string, Record<string, string>> = {}
  for (const bar of bars) out[bar.date] = fields(bar)
  return out
}

const seriesCsv = (bars: Bar[], volume: boolean): string =>
  csvText(
    volume
      ? ['timestamp', 'open', 'high', 'low', 'close', 'volume']
      : ['timestamp', 'open', 'high', 'low', 'close'],
    bars.map((bar) => [
      bar.date,
      px(bar.open),
      px(bar.high),
      px(bar.low),
      px(bar.close),
      ...(volume ? [vol(bar.volume)] : []),
    ]),
  )

const emit = (p: Params, json: unknown, csv?: string): AvPayload | Response => {
  if (wantsCsv(p) && csv) return csvResponse(csv)
  return { json }
}

export async function currencyExchangeRate(
  p: Params,
): Promise<AvPayload> {
  const from = need(p, 'from_currency').toUpperCase()
  const to = need(p, 'to_currency').toUpperCase()

  let rate: number | undefined
  let refreshed = ''

  if (isCrypto(from)) {
    if (to !== 'USD' && to !== 'USDT') {
      throw new AvError(
        `Invalid API call: crypto rates are quoted in USD; ${from}/${to} is not supported.`,
      )
    }
    const pair = `${from}${to === 'USD' ? 'USDT' : to}`
    const coin = COIN_IDS[from]
    if (coin) {
      const latest = await rows(
        `SELECT argMax(price, timestamp) AS price,
                formatDateTime(max(timestamp), '%Y-%m-%d %H:%i:%S') AS ts
         FROM analytics.crypto_market_data FINAL
         WHERE symbol = {coin:String}
         GROUP BY symbol`,
        { coin },
      )
      if (latest[0]) {
        rate = Number(latest[0].price)
        refreshed = String(latest[0].ts)
      }
    }
    if (rate === undefined) {
      const latest = await rows(
        `SELECT argMax(close, timestamp) AS price,
                formatDateTime(max(timestamp), '%Y-%m-%d %H:%i:%S') AS ts
         FROM analytics.crypto_ohlcv FINAL
         WHERE symbol = {pair:String}
         GROUP BY symbol`,
        { pair },
      )
      if (latest[0]) {
        rate = Number(latest[0].price)
        refreshed = String(latest[0].ts)
      }
    }
  } else {
    const pair = `${from}${to}`
    const direct = await rows(
      `SELECT argMax(rate, timestamp) AS rate,
              formatDateTime(max(timestamp), '%Y-%m-%d %H:%i:%S') AS ts
       FROM analytics.forex_rates FINAL
       WHERE pair = {pair:String}
       GROUP BY pair`,
      { pair },
    )
    if (direct[0]) {
      rate = Number(direct[0].rate)
      refreshed = String(direct[0].ts)
    } else {
      const inverse = await rows(
        `SELECT argMax(rate, timestamp) AS rate,
                formatDateTime(max(timestamp), '%Y-%m-%d %H:%i:%S') AS ts
         FROM analytics.forex_rates FINAL
         WHERE pair = {pair:String}
         GROUP BY pair`,
        { pair: `${to}${from}` },
      )
      if (inverse[0] && Number(inverse[0].rate)) {
        rate = 1 / Number(inverse[0].rate)
        refreshed = String(inverse[0].ts)
      }
    }
  }

  if (rate === undefined) {
    throw new AvError(`Invalid API call: no exchange rate for ${from}/${to}.`)
  }

  return {
    json: {
      'Realtime Currency Exchange Rate': {
        '1. From_Currency Code': from,
        '2. From_Currency Name': name(from),
        '3. To_Currency Code': to,
        '4. To_Currency Name': name(to),
        '5. Exchange Rate': px(rate),
        '6. Last Refreshed': refreshed,
        '7. Time Zone': 'UTC',
        '8. Bid Price': px(rate),
        '9. Ask Price': px(rate),
      },
    },
  }
}

export async function fxSeries(
  p: Params,
  period: 'intraday' | 'daily' | 'weekly' | 'monthly',
): Promise<AvPayload | Response> {
  const from = need(p, 'from_symbol').toUpperCase()
  const to = need(p, 'to_symbol').toUpperCase()
  const pair = `${from}${to}`

  let bars: Bar[] = []
  let interval = ''
  let info = ''
  let seriesKey = ''

  if (period === 'intraday') {
    interval = oneOf(p.interval ?? '5min', 'interval', [
      '1min',
      '5min',
      '15min',
      '30min',
      '60min',
    ])
    bars = await fxIntraday(pair, intradayMinutes(interval), pickLimit(p))
    info = `FX Intraday (${interval}) open, high, low, close prices`
    seriesKey = `Time Series FX (${interval})`
  } else {
    const daily = await fxDaily(pair)
    bars =
      period === 'daily'
        ? p.outputsize === 'full'
          ? daily
          : daily.slice(0, 100)
        : bucket(daily, period as 'weekly' | 'monthly')
    const label = period.charAt(0).toUpperCase() + period.slice(1)
    info = `Forex ${label} Prices (open, high, low, close)`
    seriesKey = `Time Series FX (${label})`
  }

  const meta: Record<string, string | number> = {
    '1. Information': info,
    '2. From Symbol': from,
    '3. To Symbol': to,
  }
  if (interval) {
    meta['4. Interval'] = interval
    meta['5. Output Size'] = p.outputsize === 'full' ? 'Full size' : 'Compact'
    meta['6. Time Zone'] = 'UTC'
  } else {
    meta['4. Output Size'] = p.outputsize === 'full' ? 'Full size' : 'Compact'
    meta['5. Last Refreshed'] = bars[0]?.date ?? ''
    meta['6. Time Zone'] = 'UTC'
  }

  return emit(
    p,
    { 'Meta Data': meta, [seriesKey]: toMap(bars, fxFields) },
    seriesCsv(bars, false),
  )
}

const cryptoPair = (symbol: string, market: string): string =>
  `${symbol}${market === 'USD' ? 'USDT' : market}`

export async function digitalSeries(
  p: Params,
  period: 'intraday' | 'daily' | 'weekly' | 'monthly',
): Promise<AvPayload | Response> {
  const symbol = need(p, 'symbol').toUpperCase()
  const market = (p.market ?? 'USD').toUpperCase()
  const pair = cryptoPair(symbol, market)
  const coinName = CRYPTO_NAMES[symbol] ?? symbol

  let bars: Bar[] = []
  let interval = ''
  let info = ''
  let seriesKey = ''

  if (period === 'intraday') {
    interval = oneOf(p.interval ?? '5min', 'interval', [
      '1min',
      '5min',
      '15min',
      '30min',
      '60min',
    ])
    bars = await cryptoTradesIntraday(
      pair,
      intradayMinutes(interval),
      pickLimit(p),
    )
    info = `Crypto Intraday (${interval}) open, high, low, close prices and volume`
    seriesKey = `Time Series Crypto (${interval})`
  } else {
    const daily = await cryptoDaily(pair)
    bars =
      period === 'daily'
        ? daily
        : bucket(daily, period as 'weekly' | 'monthly')
    const label = {
      daily: 'Digital Currency Daily',
      weekly: 'Digital Currency Weekly',
      monthly: 'Digital Currency Monthly',
    }[period]
    info = `${label} Prices and Volumes`
    seriesKey = `Time Series (${label})`
  }
  const meta: Record<string, string | number> = {
    '1. Information': info,
    '2. Digital Currency Code': symbol,
    '3. Digital Currency Name': coinName,
    '4. Market Code': market,
    '5. Market Name': name(market),
    '6. Last Refreshed': bars[0]?.date ?? '',
    '7. Time Zone': 'UTC',
  }

  return emit(
    p,
    { 'Meta Data': meta, [seriesKey]: toMap(bars, cryptoFields) },
    seriesCsv(bars, true),
  )
}
