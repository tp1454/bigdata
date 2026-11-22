import { beforeEach, describe, expect, it } from 'bun:test'

import { app } from './app'

type Responder = (query: string, params?: Record<string, unknown>) => unknown
const chTest = (globalThis as unknown as { chTest: { respond: Responder } }).chTest

const bars = (count: number) =>
  Array.from({ length: count }, (_, i) => {
    const day = String(20 - (i % 20)).padStart(2, '0')
    return {
      d: `2026-11-${day}`,
      o: 100 + i,
      h: 105 + i,
      l: 99 + i,
      c: 104 + i,
      v: 1000 + i,
    }
  })

const request = (path: string) =>
  app.handle(new Request(`http://localhost${path}`))

beforeEach(() => {
  chTest.respond = () => []
})

describe('alphavantage compatibility', () => {
  it('serves TIME_SERIES_DAILY with AV meta and keys', async () => {
    chTest.respond = (query) => {
      if (query.includes('FROM analytics.corporate_actions')) return []
      if (query.includes('FROM analytics.equities_prices')) {
        return [
          { d: '2026-11-20', o: 100, h: 105, l: 99, c: 104, v: 1000 },
          { d: '2026-11-19', o: 98, h: 101, l: 97, c: 100, v: 900 },
        ]
      }
      return []
    }

    const res = await request('/query?function=TIME_SERIES_DAILY&symbol=aapl')
    expect(res.status).toBe(200)
    const body = (await res.json()) as Record<string, any>
    expect(body['Meta Data']['2. Symbol']).toBe('AAPL')
    expect(body['Meta Data']['4. Output Size']).toBe('Compact')
    expect(body['Time Series (Daily)']['2026-11-20']).toEqual({
      '1. open': '100.0000',
      '2. high': '105.0000',
      '3. low': '99.0000',
      '4. close': '104.0000',
      '5. volume': '1000',
    })
  })

  it('computes GLOBAL_QUOTE change and percent', async () => {
    chTest.respond = (query) => {
      if (query.includes('FROM analytics.corporate_actions')) return []
      return [
        { d: '2026-11-20', o: 100, h: 106, l: 99, c: 104, v: 1000 },
        { d: '2026-11-19', o: 98, h: 101, l: 97, c: 100, v: 900 },
      ]
    }

    const res = await request('/query?function=GLOBAL_QUOTE&symbol=AAPL')
    const body = (await res.json()) as Record<string, any>
    const quote = body['Global Quote']
    expect(quote['05. price']).toBe('104.0000')
    expect(quote['08. previous close']).toBe('100.0000')
    expect(quote['09. change']).toBe('4.0000')
    expect(quote['10. change percent']).toBe('4.0000%')
  })

  it('serves SYMBOL_SEARCH best matches', async () => {
    chTest.respond = () => [
      { symbol: 'AAPL', name: 'Apple Inc.', exchange: 'NASDAQ', etf: 0 },
    ]
    const res = await request('/query?function=SYMBOL_SEARCH&keywords=app')
    const body = (await res.json()) as Record<string, any>
    expect(body.bestMatches[0]['1. symbol']).toBe('AAPL')
    expect(body.bestMatches[0]['9. matchScore']).toBe('0.9000')
  })

  it('maps income statement line items to AV fields', async () => {
    chTest.respond = () => [
      { end_date: '2025-12-31', period: 'annual', line_item: 'Total Revenue', value: 100 },
      { end_date: '2025-12-31', period: 'annual', line_item: 'Gross Profit', value: 40 },
      { end_date: '2025-09-30', period: 'quarterly', line_item: 'Total Revenue', value: 25 },
    ]
    const res = await request('/query?function=INCOME_STATEMENT&symbol=IBM')
    const body = (await res.json()) as Record<string, any>
    expect(body.annualReports[0].fiscalDateEnding).toBe('2025-12-31')
    expect(body.annualReports[0].totalRevenue).toBe('100')
    expect(body.quarterlyReports[0].totalRevenue).toBe('25')
  })

  it('serves CPI in the economic indicator shape', async () => {
    chTest.respond = () => [
      { d: '2026-09-01', c: 336.94 },
      { d: '2026-08-01', c: 334.98 },
    ]
    const res = await request('/query?function=CPI')
    const body = (await res.json()) as Record<string, any>
    expect(body.name).toBe('Consumer Price Index for all Urban Consumers')
    expect(body.unit).toBe('index 1982-1984=100')
    expect(body.data[0]).toEqual({ date: '2026-09-01', value: '336.9400' })
  })

  it('computes the realtime put-call ratio per expiration', async () => {
    chTest.respond = () => [
      { expiry: '2026-11-27', put: 300, call: 200, ts: '2026-11-22 04:00:00' },
      { expiry: '2026-12-04', put: 100, call: 100, ts: '2026-11-22 04:00:00' },
    ]
    const res = await request('/query?function=REALTIME_PUT_CALL_RATIO&symbol=AAPL')
    const body = (await res.json()) as Record<string, any>
    expect(body.put_call_ratio_full_chain).toBe('1.33')
    expect(body.put_call_ratio_by_expiration[0]).toEqual({
      date: '2026-11-27',
      value: '1.5',
    })
  })

  it('serves technical indicators with the AV series key', async () => {
    chTest.respond = () => bars(30)
    const res = await request(
      '/query?function=SMA&symbol=AAPL&time_period=5&series_type=close',
    )
    const body = (await res.json()) as Record<string, any>
    expect(body['Meta Data']['2: Indicator']).toBe('Simple Moving Average (SMA)')
    const series = body['Technical Analysis: SMA']
    const first = Object.values(series)[0] as Record<string, string>
    expect(first.SMA).toBeDefined()
  })

  it('returns CSV when datatype=csv', async () => {
    chTest.respond = (query) => {
      if (query.includes('FROM analytics.corporate_actions')) return []
      return [{ d: '2026-11-20', o: 100, h: 105, l: 99, c: 104, v: 1000 }]
    }
    const res = await request(
      '/query?function=TIME_SERIES_DAILY&symbol=AAPL&datatype=csv',
    )
    expect(res.headers.get('content-type')).toContain('text/csv')
    expect(await res.text()).toStartWith('timestamp,open,high,low,close,volume')
  })

  it('serves each function on its own route', async () => {
    chTest.respond = (query) => {
      if (query.includes('FROM analytics.corporate_actions')) return []
      return [
        { d: '2026-11-20', o: 100, h: 106, l: 99, c: 104, v: 1000 },
        { d: '2026-11-19', o: 98, h: 101, l: 97, c: 100, v: 900 },
      ]
    }

    const quote = await request('/alphavantage/GLOBAL_QUOTE?symbol=AAPL')
    expect(quote.status).toBe(200)
    const body = (await quote.json()) as Record<string, any>
    expect(body['Global Quote']['05. price']).toBe('104.0000')

    const csv = await request(
      '/alphavantage/TIME_SERIES_DAILY?symbol=AAPL&datatype=csv',
    )
    expect(csv.headers.get('content-type')).toContain('text/csv')
  })

  it('documents every function as a separate operation', async () => {
    const res = await request('/openapi/json')
    const spec = (await res.json()) as Record<string, any>
    const paths = Object.keys(spec.paths)
    expect(paths).toContain('/alphavantage/TIME_SERIES_DAILY')
    expect(paths).toContain('/alphavantage/INCOME_STATEMENT')
    expect(paths).toContain('/alphavantage/CPI')
    expect(paths.filter((p) => p.startsWith('/alphavantage/')).length).toBeGreaterThan(40)
    expect(
      spec.paths['/alphavantage/GLOBAL_QUOTE'].get.tags,
    ).toEqual(['stocks'])
    expect(spec['x-tagGroups'][0].tags).toContain('indicators')
  })

  it('rejects unsupported functions with an AV error', async () => {
    const res = await request('/query?function=NEWS_SENTIMENT')
    const body = (await res.json()) as Record<string, string>
    expect(body['Error Message']).toContain('not served')
  })
})
