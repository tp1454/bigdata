import { ch } from '../clickhouse'
import {
  AvError,
  csvResponse,
  csvText,
  oneOf,
  px,
  wantsCsv,
  type AvPayload,
  type Bar,
  type Params,
} from './core'
import { bucket } from './bars'

type Row = Record<string, unknown>

async function indicatorRows(indicator: string): Promise<Bar[]> {
  const rs = await ch.query({
    query: `SELECT toString(timestamp) AS d, value AS c
            FROM analytics.macro_indicators FINAL
            WHERE indicator = {indicator:String}
            ORDER BY timestamp DESC`,
    query_params: { indicator },
    format: 'JSONEachRow',
  })
  const data = await rs.json<Row[]>()
  return (Array.isArray(data) ? data : []).map((row) => ({
    date: String(row.d),
    open: Number(row.c),
    high: Number(row.c),
    low: Number(row.c),
    close: Number(row.c),
    volume: 0,
  }))
}

const rowsOf = (bars: Bar[]) =>
  bars.map((bar) => ({ date: bar.date, value: px(bar.close) }))

const economic = (
  name: string,
  interval: string,
  unit: string,
  bars: Bar[],
): AvPayload => ({
  json: {
    name,
    interval,
    unit,
    data: rowsOf(bars),
  },
})

const economicCsv = (name: string, interval: string, unit: string, bars: Bar[]) =>
  csvText(
    ['name', 'interval', 'unit', 'date', 'value'],
    bars.map((bar) => [name, interval, unit, bar.date, px(bar.close)]),
  )

const periodic = (bars: Bar[], interval: string): Bar[] => {
  if (interval === 'daily') return bars
  if (interval === 'weekly') return bucket(bars, 'weekly')
  if (interval === 'monthly') return bucket(bars, 'monthly')
  return bars
}

const annual = (bars: Bar[]): Bar[] => {
  const out: Bar[] = []
  let year = ''
  let sum = 0
  let count = 0
  for (const bar of bars) {
    const y = bar.date.slice(0, 4)
    if (y !== year) {
      if (count) {
        out.push({ ...bars[0], date: `${year}-12-31`, close: sum / count })
      }
      year = y
      sum = 0
      count = 0
    }
    sum += bar.close
    count += 1
  }
  if (count) out.push({ ...bars[0], date: `${year}-12-31`, close: sum / count })
  return out
}

const TREASURY: Record<string, { indicator: string; name: string }> = {
  '3month': { indicator: 'TREASURY_YIELD_3M', name: '3-Month Treasury Constant Maturity Rate' },
  '2year': { indicator: 'TREASURY_YIELD_2Y', name: '2-Year Treasury Constant Maturity Rate' },
  '10year': { indicator: 'TREASURY_YIELD_10Y', name: '10-Year Treasury Constant Maturity Rate' },
  '30year': { indicator: 'TREASURY_YIELD_30Y', name: '30-Year Treasury Constant Maturity Rate' },
}

export async function treasuryYield(p: Params): Promise<AvPayload | Response> {
  const maturity = oneOf(p.maturity ?? '10year', 'maturity', [
    '3month',
    '2year',
    '5year',
    '7year',
    '10year',
    '30year',
  ])
  const definition = TREASURY[maturity]
  if (!definition) {
    throw new AvError(
      `Invalid API call: the ${maturity.replace('year', '-year')} maturity is not collected. Supported: ${Object.keys(TREASURY).join(', ')}.`,
    )
  }
  const interval = oneOf(p.interval ?? 'monthly', 'interval', [
    'daily',
    'weekly',
    'monthly',
  ])
  const bars = periodic(await indicatorRows(definition.indicator), interval)
  const json = economic(definition.name, interval, 'percent', bars)
  if (!wantsCsv(p)) return json
  return csvResponse(
    economicCsv(definition.name, interval, 'percent', bars),
  )
}

export async function consumerPriceIndex(p: Params): Promise<AvPayload | Response> {
  const interval = oneOf(p.interval ?? 'monthly', 'interval', [
    'monthly',
    'annual',
  ])
  const monthly = await indicatorRows('CPI')
  const bars = interval === 'annual' ? annual(monthly) : monthly
  const name = 'Consumer Price Index for all Urban Consumers'
  const unit = 'index 1982-1984=100'
  if (!wantsCsv(p)) return economic(name, interval, unit, bars)
  return csvResponse(economicCsv(name, interval, unit, bars))
}

export async function unemployment(p: Params): Promise<AvPayload | Response> {
  const interval = oneOf(p.interval ?? 'monthly', 'interval', [
    'monthly',
    'annual',
  ])
  const monthly = await indicatorRows('UNEMPLOYMENT')
  const bars = interval === 'annual' ? annual(monthly) : monthly
  const name = 'Unemployment Rate'
  const unit = 'percent'
  if (!wantsCsv(p)) return economic(name, interval, unit, bars)
  return csvResponse(economicCsv(name, interval, unit, bars))
}

export async function inflation(p: Params): Promise<AvPayload | Response> {
  const interval = oneOf(p.interval ?? 'monthly', 'interval', [
    'monthly',
    'annual',
  ])
  const monthly = await indicatorRows('CPI')
  const byMonth = new Map(monthly.map((bar) => [bar.date.slice(0, 7), bar.close]))
  const computed: Bar[] = []
  for (const bar of monthly) {
    const previous = byMonth.get(
      `${Number(bar.date.slice(0, 4)) - 1}${bar.date.slice(4, 7)}`,
    )
    if (previous) {
      computed.push({
        ...bar,
        close: ((bar.close - previous) / previous) * 100,
      })
    }
  }
  const bars = interval === 'annual' ? annual(computed) : computed
  const name = 'Inflation'
  const unit = 'percent'
  if (!wantsCsv(p)) return economic(name, interval, unit, bars)
  return csvResponse(economicCsv(name, interval, unit, bars))
}

export const COMMODITIES: Record<
  string,
  { indicator: string; name: string; unit: string }
> = {
  WTI: { indicator: 'COMMODITY_WTI', name: 'Crude Oil Prices: West Texas Intermediate (WTI)', unit: 'dollars per barrel' },
  BRENT: { indicator: 'COMMODITY_BRENT', name: 'Crude Oil Prices: Brent - Europe', unit: 'dollars per barrel' },
  NATURAL_GAS: { indicator: 'COMMODITY_NATURAL_GAS', name: 'Natural Gas', unit: 'dollars per million Btu' },
  COPPER: { indicator: 'COMMODITY_COPPER', name: 'Global Price of Copper', unit: 'dollars per metric ton' },
  WHEAT: { indicator: 'COMMODITY_WHEAT', name: 'Global Price of Wheat', unit: 'dollars per metric ton' },
  CORN: { indicator: 'COMMODITY_CORN', name: 'Global Price of Corn', unit: 'dollars per metric ton' },
  COTTON: { indicator: 'COMMODITY_COTTON', name: 'Global Price of Cotton', unit: 'dollars per metric ton' },
  SUGAR: { indicator: 'COMMODITY_SUGAR', name: 'Global Price of Sugar', unit: 'cents per pound' },
  COFFEE: { indicator: 'COMMODITY_COFFEE', name: 'Global Price of Coffee', unit: 'cents per pound' },
}

export async function commodity(
  fn: keyof typeof COMMODITIES,
  p: Params,
): Promise<AvPayload | Response> {
  const definition = COMMODITIES[fn]
  const interval = oneOf(p.interval ?? 'monthly', 'interval', [
    'daily',
    'weekly',
    'monthly',
  ])
  const bars = periodic(await indicatorRows(definition.indicator), interval)
  if (!wantsCsv(p)) {
    return economic(definition.name, interval, definition.unit, bars)
  }
  return csvResponse(
    economicCsv(definition.name, interval, definition.unit, bars),
  )
}
