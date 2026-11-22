import { params, type RouteDef } from './shared'
import { indicatorPayload, type Kind } from '../../av/indicators'

const indicator = (
  fn: Kind,
  tag: string,
  summary: string,
  description: string,
): RouteDef => ({
  fn,
  tag,
  summary,
  description,
  schema: params(
    'symbol',
    'interval',
    'time_period',
    'series_type',
    'nbdevup',
    'nbdevdn',
    'fastperiod',
    'slowperiod',
    'signalperiod',
    'datatype',
    'apikey',
  ),
  handler: (p) => indicatorPayload(fn, p),
})

export const indicatorsRoutes: RouteDef[] = [
  indicator(
    'SMA',
    'indicators',
    'Simple moving average',
    'Rolling mean of `series_type` over `time_period`.',
  ),
  indicator(
    'EMA',
    'indicators',
    'Exponential moving average',
    'EMA seeded with the SMA of the first `time_period` bars.',
  ),
  indicator(
    'WMA',
    'indicators',
    'Weighted moving average',
    'Linearly weighted mean of `series_type` over `time_period`.',
  ),
  indicator(
    'RSI',
    'indicators',
    'Relative strength index',
    "Wilder's RSI over `time_period` changes.",
  ),
  indicator(
    'ATR',
    'indicators',
    'Average true range',
    "Wilder's ATR over `time_period` bars.",
  ),
  indicator(
    'BBANDS',
    'indicators',
    'Bollinger bands',
    'SMA middle band with upper and lower bands at `nbdevup` / `nbdevdn` population standard deviations.',
  ),
  indicator(
    'MACD',
    'indicators',
    'MACD line, signal and histogram',
    'EMA(`fastperiod`) minus EMA(`slowperiod`), plus the EMA(`signalperiod`) signal line and histogram.',
  ),
]
