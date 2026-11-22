import { ch } from '../clickhouse'
import {
  AvError,
  csvResponse,
  csvText,
  need,
  oneOf,
  pct,
  pickLimit,
  px,
  ratio,
  symbolsFrom,
  vol,
  wantsCsv,
  type AvPayload,
  type Bar,
  type Params,
} from './core'
import {
  adjustDaily,
  bucket,
  bucketAdjusted,
  corporateActions,
  equityDaily,
  equityIntraday,
  intradayMinutes,
} from './bars'

const INTERVALS = ['1min', '5min', '15min', '30min', '60min'] as const

const compact = (rows: Bar[], p: Params): Bar[] =>
  p.outputsize === 'full' ? rows : rows.slice(0, 100)

const rowsToJson = (
  rows: Bar[],
  fields: (bar: Bar) => Record<string, string>,
): Record<string, Record<string, string>> => {
  const out: Record<string, Record<string, string>> = {}
  for (const bar of rows) out[bar.date] = fields(bar)
  return out
}

const plainFields = (bar: Bar): Record<string, string> => ({
  '1. open': px(bar.open),
  '2. high': px(bar.high),
  '3. low': px(bar.low),
  '4. close': px(bar.close),
  '5. volume': vol(bar.volume),
})

const fxFields = (bar: Bar): Record<string, string> => ({
  '1. open': px(bar.open),
  '2. high': px(bar.high),
  '3. low': px(bar.low),
  '4. close': px(bar.close),
})

const seriesCsv = (
  rows: Bar[],
  withVolume: boolean,
): string =>
  csvText(
    withVolume
      ? ['timestamp', 'open', 'high', 'low', 'close', 'volume']
      : ['timestamp', 'open', 'high', 'low', 'close'],
    rows.map((bar) => [
      bar.date,
      px(bar.open),
      px(bar.high),
      px(bar.low),
      px(bar.close),
      ...(withVolume ? [vol(bar.volume)] : []),
    ]),
  )

function payload(
  p: Params,
  json: unknown,
  csv?: string,
): AvPayload | Response {
  if (wantsCsv(p) && csv) return csvResponse(csv)
  return { json }
}

export async function intraday(p: Params): Promise<AvPayload | Response> {
  const symbol = need(p, 'symbol').toUpperCase()
  const interval = oneOf(p.interval ?? '5min', 'interval', INTERVALS)
  const bars = await equityIntraday(
    symbol,
    intradayMinutes(interval),
    pickLimit(p),
  )

  const json = {
    'Meta Data': {
      '1. Information': `Intraday (${interval}) open, high, low, close prices and volume`,
      '2. Symbol': symbol,
      '3. Last Refreshed': bars[0]?.date ?? '',
      '4. Interval': interval,
      '5. Output Size': p.outputsize === 'full' ? 'Full size' : 'Compact',
      '6. Time Zone': 'US/Eastern',
    },
    [`Time Series (${interval})`]: rowsToJson(bars, plainFields),
  }
  return payload(p, json, seriesCsv(bars, true))
}

async function dailyWithActions(symbol: string) {
  const stored = (await equityDaily(symbol)).slice().reverse()
  const actions = await corporateActions(symbol)
  return adjustDaily(stored, actions)
}

export async function daily(
  p: Params,
  adjusted: boolean,
): Promise<AvPayload | Response> {
  const symbol = need(p, 'symbol').toUpperCase()
  const days = (await dailyWithActions(symbol)).slice().reverse()
  const rows = compact(
    days.map((day) => (adjusted ? day.adj : day.raw)),
    p,
  )

  if (!adjusted) {
    const json = {
      'Meta Data': {
        '1. Information': 'Daily Prices (open, high, low, close) and Volumes',
        '2. Symbol': symbol,
        '3. Last Refreshed': rows[0]?.date ?? '',
        '4. Output Size': p.outputsize === 'full' ? 'Full size' : 'Compact',
        '5. Time Zone': 'US/Eastern',
      },
      'Time Series (Daily)': rowsToJson(rows, plainFields),
    }
    return payload(p, json, seriesCsv(rows, true))
  }

  const byDate = new Map(days.map((day) => [day.date, day]))
  const series: Record<string, Record<string, string>> = {}
  for (const row of rows) {
    const day = byDate.get(row.date)
    series[row.date] = {
      '1. open': px(day?.raw.open ?? row.open),
      '2. high': px(day?.raw.high ?? row.high),
      '3. low': px(day?.raw.low ?? row.low),
      '4. close': px(day?.raw.close ?? row.close),
      '5. adjusted close': px(row.close),
      '6. volume': vol(day?.raw.volume ?? row.volume),
      '7. dividend amount': px(day?.dividend ?? 0),
      '8. split coefficient': px(day?.split ?? 1),
    }
  }
  const json = {
    'Meta Data': {
      '1. Information': 'Daily Time Series with Splits and Dividend Events',
      '2. Symbol': symbol,
      '3. Last Refreshed': rows[0]?.date ?? '',
      '4. Output Size': p.outputsize === 'full' ? 'Full size' : 'Compact',
      '5. Time Zone': 'US/Eastern',
    },
    'Time Series (Daily)': series,
  }
  return payload(
    p,
    json,
    csvText(
      ['timestamp', 'open', 'high', 'low', 'close', 'adjusted close', 'volume', 'dividend amount'],
      rows.map((row) => {
        const day = byDate.get(row.date)
        return [
          row.date,
          px(day?.raw.open ?? row.open),
          px(day?.raw.high ?? row.high),
          px(day?.raw.low ?? row.low),
          px(day?.raw.close ?? row.close),
          px(row.close),
          vol(day?.raw.volume ?? row.volume),
          px(day?.dividend ?? 0),
        ]
      }),
    ),
  )
}

export async function weeklyMonthly(
  p: Params,
  period: 'weekly' | 'monthly',
  adjusted: boolean,
): Promise<AvPayload | Response> {
  const symbol = need(p, 'symbol').toUpperCase()
  const days = (await dailyWithActions(symbol)).slice().reverse()
  const title = period === 'weekly' ? 'Weekly' : 'Monthly'
  const seriesKey = adjusted
    ? `${title} Adjusted Time Series`
    : `${title} Time Series`

  if (!adjusted) {
    const rows = compact(
      bucket(days.map((day) => day.raw), period),
      p,
    )
    const json = {
      'Meta Data': {
        '1. Information': `${title} Prices (open, high, low, close) and Volumes`,
        '2. Symbol': symbol,
        '3. Last Refreshed': rows[0]?.date ?? '',
        '4. Time Zone': 'US/Eastern',
      },
      [seriesKey]: rowsToJson(rows, plainFields),
    }
    return payload(p, json, seriesCsv(rows, true))
  }

  const rawRows = bucket(days.map((day) => day.raw), period)
  const grouped = compact(bucketAdjusted(days, period), p)
  const series: Record<string, Record<string, string>> = {}
  grouped.forEach((day, i) => {
    const rawBar = rawRows[i] ?? day.bar
    series[day.date] = {
      '1. open': px(rawBar.open),
      '2. high': px(rawBar.high),
      '3. low': px(rawBar.low),
      '4. close': px(rawBar.close),
      '5. adjusted close': px(day.bar.close),
      '6. volume': vol(rawBar.volume),
      '7. dividend amount': px(day.dividend),
      '8. split coefficient': px(day.split),
    }
  })
  const json = {
    'Meta Data': {
      '1. Information': `${title} Adjusted Prices with Splits and Dividend Events`,
      '2. Symbol': symbol,
      '3. Last Refreshed': grouped[0]?.date ?? '',
      '4. Time Zone': 'US/Eastern',
    },
    [seriesKey]: series,
  }
  return payload(p, json)
}

export async function globalQuote(p: Params): Promise<AvPayload | Response> {
  const symbol = need(p, 'symbol').toUpperCase()
  const days = (await dailyWithActions(symbol)).slice().reverse()
  const last = days[0]
  const prev = days[1]
  if (!last) {
    return {
      json: {
        'Global Quote': {},
      },
    }
  }
  const close = last.raw.close
  const previous = prev?.raw.close ?? close
  const change = close - previous
  const json = {
    'Global Quote': {
      '01. symbol': symbol,
      '02. open': px(last.raw.open),
      '03. high': px(last.raw.high),
      '04. low': px(last.raw.low),
      '05. price': px(close),
      '06. volume': vol(last.raw.volume),
      '07. latest trading day': last.date,
      '08. previous close': px(previous),
      '09. change': px(change),
      '10. change percent': pct(previous ? (change / previous) * 100 : 0),
    },
  }
  return payload(
    p,
    json,
    csvText(
      ['symbol', 'open', 'high', 'low', 'price', 'volume', 'latest trading day', 'previous close', 'change', 'change percent'],
      [[
        symbol,
        px(last.raw.open),
        px(last.raw.high),
        px(last.raw.low),
        px(close),
        vol(last.raw.volume),
        last.date,
        px(previous),
        px(change),
        pct(previous ? (change / previous) * 100 : 0),
      ]],
    ),
  )
}

export async function bulkQuotes(p: Params): Promise<AvPayload | Response> {
  const symbols = symbolsFrom(need(p, 'symbol'))
  if (symbols.length === 0) {
    throw new AvError('Invalid API call: symbol is empty.')
  }
  const rs = await ch.query({
    query: `SELECT symbol,
                   argMax(close, ts) AS price,
                   argMax(volume, ts) AS volume,
                   formatDateTime(max(ts), '%Y-%m-%d %H:%i:%S') AS timestamp
            FROM analytics.equities_prices_clean
            WHERE symbol IN {symbols:Array(String)}
            GROUP BY symbol
            ORDER BY symbol`,
    query_params: { symbols },
    format: 'JSONEachRow',
  })
  const data = await rs.json<Record<string, unknown>[]>()
  const quotes = (Array.isArray(data) ? data : []).map((row) => ({
    symbol: String(row.symbol),
    price: px(Number(row.price)),
    volume: vol(Number(row.volume)),
    timestamp: String(row.timestamp),
  }))
  const json = {
    endpoint: 'Realtime Bulk Quotes',
    last_updated: quotes[0]?.timestamp ?? '',
    quotes,
  }
  return payload(
    p,
    json,
    csvText(
      ['symbol', 'price', 'volume', 'timestamp'],
      quotes.map((q) => [q.symbol, q.price, q.volume, q.timestamp]),
    ),
  )
}

type CatalogRow = {
  symbol: string
  name: string
  exchange: string
  etf: number
}

async function catalog(where: string, params: Record<string, unknown>, limit = 20) {
  const rs = await ch.query({
    query: `SELECT symbol, name, exchange, etf
            FROM analytics.symbol_catalog FINAL
            WHERE ${where}
            ORDER BY symbol
            LIMIT ${limit}`,
    query_params: params,
    format: 'JSONEachRow',
  })
  const data = await rs.json<Record<string, unknown>[]>()
  return (Array.isArray(data) ? data : []).map(
    (row): CatalogRow => ({
      symbol: String(row.symbol),
      name: String(row.name),
      exchange: String(row.exchange),
      etf: Number(row.etf),
    }),
  )
}

export async function symbolSearch(p: Params): Promise<AvPayload | Response> {
  const keywords = need(p, 'keywords')
  const matches = await catalog(
    'symbol ILIKE {kw:String} OR name ILIKE {kw:String}',
    { kw: `%${keywords}%` },
  )
  const lowered = keywords.toLowerCase()
  const bestMatches = matches.map((row) => {
    const score =
      row.symbol.toLowerCase() === lowered
        ? 1
        : row.symbol.toLowerCase().startsWith(lowered) ||
            row.name.toLowerCase().startsWith(lowered)
          ? 0.9
          : 0.7
    return {
      '1. symbol': row.symbol,
      '2. name': row.name,
      '3. type': row.etf ? 'ETF' : 'Equity',
      '4. region': 'United States',
      '5. marketOpen': '09:30',
      '6. marketClose': '16:00',
      '7. timezone': 'UTC-05',
      '8. currency': 'USD',
      '9. matchScore': score.toFixed(4),
    }
  })
  return payload(
    p,
    { bestMatches },
    csvText(
      ['symbol', 'name', 'type', 'region', 'marketOpen', 'marketClose', 'timezone', 'currency', 'matchScore'],
      bestMatches.map((row) => Object.values(row)),
    ),
  )
}

export async function listingStatus(p: Params): Promise<AvPayload | Response> {
  const state = p.state ?? 'active'
  if (state === 'delisted') {
    const empty = csvText(
      ['symbol', 'name', 'exchange', 'assetType', 'ipoDate', 'delistingDate', 'status'],
      [],
    )
    return p.datatype === 'json'
      ? { json: [] }
      : csvResponse(empty)
  }
  const rows = await catalog('1 = 1', {}, 100000)
  const list = rows.map((row) => ({
    symbol: row.symbol,
    name: row.name,
    exchange: row.exchange,
    assetType: row.etf ? 'ETF' : 'Stock',
    ipoDate: '',
    delistingDate: '',
    status: 'Active',
  }))
  if (p.datatype === 'json') return { json: list }
  return csvResponse(
    csvText(
      ['symbol', 'name', 'exchange', 'assetType', 'ipoDate', 'delistingDate', 'status'],
      list.map((row) => Object.values(row)),
    ),
  )
}

function easternNow(): { weekday: number; minutes: number } {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    hour12: false,
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
  }).formatToParts(new Date())
  const get = (type: string) =>
    parts.find((part) => part.type === type)?.value ?? ''
  const weekday = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].indexOf(
    get('weekday'),
  )
  const hour = Number(get('hour')) % 24
  const minute = Number(get('minute'))
  return { weekday, minutes: hour * 60 + minute }
}

export function marketStatus(): AvPayload {
  const { weekday, minutes } = easternNow()
  const equityOpen =
    weekday >= 1 && weekday <= 5 && minutes >= 570 && minutes < 960
  return {
    json: {
      endpoint: 'Global Market Open & Close Status',
      markets: [
        {
          market_type: 'Equity',
          region: 'United States',
          primary_exchanges: 'NASDAQ, NYSE, AMEX, BATS',
          local_open: '09:30',
          local_close: '16:00',
          current_status: equityOpen ? 'open' : 'closed',
          notes: '',
        },
        {
          market_type: 'Forex',
          region: 'Global',
          primary_exchanges: 'FX',
          local_open: '00:00',
          local_close: '23:59',
          current_status: weekday >= 1 && weekday <= 5 ? 'open' : 'closed',
          notes: 'Twenty-four hour market, Sunday 22:00 UTC to Friday 22:00 UTC.',
        },
        {
          market_type: 'Cryptocurrency',
          region: 'Global',
          primary_exchanges: 'Crypto',
          local_open: '00:00',
          local_close: '23:59',
          current_status: 'open',
          notes: 'Twenty-four hour market.',
        },
      ],
    },
  }
}

type MoverRow = {
  symbol: string
  last: number
  previous: number
  volume: number
  date: string
}

export async function topGainersLosers(p: Params): Promise<AvPayload | Response> {
  const rs = await ch.query({
    query: `WITH daily AS (
              SELECT symbol, toDate(ts) AS d,
                     argMax(close, ts) AS c,
                     argMax(volume, ts) AS v
              FROM analytics.equities_prices_clean
              WHERE ts >= now() - INTERVAL 10 DAY
              GROUP BY symbol, d
            ), ranked AS (
              SELECT symbol, d, c, v,
                     lagInFrame(c) OVER (PARTITION BY symbol ORDER BY d) AS prev
              FROM daily
            )
            SELECT symbol,
                   argMax(c, d) AS last,
                   argMax(prev, d) AS previous,
                   argMax(v, d) AS volume,
                   toString(max(d)) AS date
            FROM ranked
            WHERE prev > 0
            GROUP BY symbol`,
    format: 'JSONEachRow',
  })
  const data = await rs.json<Record<string, unknown>[]>()
  const movers: MoverRow[] = (Array.isArray(data) ? data : []).map((row) => ({
    symbol: String(row.symbol),
    last: Number(row.last),
    previous: Number(row.previous),
    volume: Number(row.volume),
    date: String(row.date),
  }))
  const moved = movers.map((row) => ({
    ...row,
    change: row.last - row.previous,
    changePct: ((row.last - row.previous) / row.previous) * 100,
  }))
  const entry = (row: (typeof moved)[number]) => ({
    ticker: row.symbol,
    price: px(row.last),
    change_amount: px(row.change),
    change_percentage: `${row.changePct.toFixed(4)}%`,
    volume: vol(row.volume),
  })
  const json = {
    metadata: 'Top gainers, losers, and most actively traded US tickers',
    last_updated: `${moved[0]?.date ?? ''} 16:00:00 US/Eastern`,
    top_gainers: moved
      .slice()
      .sort((a, b) => b.changePct - a.changePct)
      .slice(0, 20)
      .map(entry),
    top_losers: moved
      .slice()
      .sort((a, b) => a.changePct - b.changePct)
      .slice(0, 20)
      .map(entry),
    most_actively_traded: moved
      .slice()
      .sort((a, b) => b.volume - a.volume)
      .slice(0, 20)
      .map(entry),
  }
  return payload(p, json)
}

type RatioRow = { expiry: string; put: number; call: number; ts: string }

export async function putCallRatio(
  p: Params,
  historical: boolean,
): Promise<AvPayload | Response> {
  const symbol = need(p, 'symbol').toUpperCase()
  const conditions = ['symbol = {symbol:String}']
  const params: Record<string, unknown> = { symbol }
  if (historical && p.date) {
    conditions.push('toDate(timestamp) = toDate({date:String})')
    params.date = p.date
  }
  const rs = await ch.query({
    query: `SELECT toString(expiry) AS expiry,
                   argMax(put_volume, timestamp) AS put,
                   argMax(call_volume, timestamp) AS call,
                   formatDateTime(max(timestamp), '%Y-%m-%d %H:%i:%S') AS ts
            FROM analytics.options_metrics FINAL
            WHERE ${conditions.join(' AND ')}
            GROUP BY expiry
            ORDER BY ts DESC, expiry ASC`,
    query_params: params,
    format: 'JSONEachRow',
  })
  const data = await rs.json<Record<string, unknown>[]>()
  const rows = (Array.isArray(data) ? data : [])
    .map(
      (row): RatioRow => ({
        expiry: String(row.expiry),
        put: Number(row.put),
        call: Number(row.call),
        ts: String(row.ts),
      }),
    )
  if (rows.length === 0) {
    return { json: { symbol, put_call_ratio_full_chain: '0', put_call_ratio_by_expiration: [] } }
  }
  const latestDay = rows[0].ts.slice(0, 10)
  const fresh = rows.filter((row) => row.ts.startsWith(latestDay))
  const put = fresh.reduce((sum, row) => sum + row.put, 0)
  const call = fresh.reduce((sum, row) => sum + row.call, 0)
  const json = {
    symbol,
    put_call_ratio_full_chain: ratio(call ? put / call : 0),
    put_call_ratio_by_expiration: fresh.map((row) => ({
      date: row.expiry,
      value: ratio(row.call ? row.put / row.call : 0),
    })),
  }
  return payload(
    p,
    json,
    csvText(
      ['expiration_date', 'put_call_ratio'],
      json.put_call_ratio_by_expiration.map((row) => [row.date, row.value]),
    ),
  )
}
