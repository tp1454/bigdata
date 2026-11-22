import { describe, expect, it } from 'bun:test'

import type { Action, Bar } from './core'
import { adjustDaily, bucket, bucketAdjusted } from './bars'

const bar = (
  date: string,
  open: number,
  close: number,
  volume = 1000,
): Bar => ({
  date,
  open,
  high: open + 5,
  low: open - 1,
  close,
  volume,
})

describe('adjustDaily', () => {
  it('keeps as-traded prices and scales the adjusted series by a later split', () => {
    const days = [bar('2026-01-02', 100, 100), bar('2026-01-06', 200, 200, 2000)]
    const splits: Action[] = [{ action: 'split', date: '2026-01-06', value: 2 }]

    const [before, onSplit] = adjustDaily(days, splits)

    expect(before.raw).toEqual(days[0])
    expect(before.adj.close).toBe(200)
    expect(before.adj.volume).toBe(1000)
    expect(before.split).toBe(1)
    expect(onSplit.raw.close).toBe(200)
    expect(onSplit.adj.close).toBe(200)
    expect(onSplit.split).toBe(2)
  })

  it('applies a dividend factor only to the days before the ex-date', () => {
    const days = [bar('2026-01-02', 100, 100), bar('2026-01-05', 100, 100)]
    const dividends: Action[] = [
      { action: 'dividend', date: '2026-01-05', value: 1 },
    ]

    const [before, onExDate] = adjustDaily(days, dividends)

    expect(before.raw.close).toBe(100)
    expect(before.adj.close).toBeCloseTo(99, 10)
    expect(before.dividend).toBe(0)
    expect(onExDate.adj.close).toBeCloseTo(100, 10)
    expect(onExDate.dividend).toBe(1)
  })

  it('returns the stored bars unchanged when there are no actions', () => {
    const days = [bar('2026-01-02', 100, 101)]
    const [day] = adjustDaily(days, [])
    expect(day.raw).toEqual(days[0])
    expect(day.adj).toEqual(days[0])
  })
})

describe('bucket', () => {
  it('builds a Monday-to-Friday bar from newest-first input', () => {
    const rows = [bar('2026-01-06', 200, 210, 300), bar('2026-01-05', 100, 110, 200)]
    expect(bucket(rows, 'weekly')).toEqual([
      { date: '2026-01-06', open: 100, high: 205, low: 99, close: 210, volume: 500 },
    ])
  })

  it('buckets monthly by the calendar month', () => {
    const rows = [bar('2026-02-02', 50, 55), bar('2026-01-30', 40, 45)]
    expect(bucket(rows, 'monthly').map((row) => row.date)).toEqual([
      '2026-02-02',
      '2026-01-30',
    ])
  })
})

describe('bucketAdjusted', () => {
  it('keeps the newest close and the oldest open of the week', () => {
    const days = adjustDaily(
      [bar('2026-01-05', 100, 100), bar('2026-01-06', 100, 101)],
      [],
    )
    const [week] = bucketAdjusted(days.reverse(), 'weekly')
    expect(week.date).toBe('2026-01-06')
    expect(week.bar.open).toBe(100)
    expect(week.bar.close).toBe(101)
    expect(week.bar.volume).toBe(2000)
  })
})
