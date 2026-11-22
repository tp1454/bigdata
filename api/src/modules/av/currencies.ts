import { params, type RouteDef } from './shared'
import {
  currencyExchangeRate,
  digitalSeries,
  fxSeries,
} from '../../av/currencies'

type Period = 'intraday' | 'daily' | 'weekly' | 'monthly'

const fx = (
  fn: string,
  period: Period,
  summary: string,
  description: string,
): RouteDef => ({
  fn,
  tag: 'currencies',
  summary,
  description,
  schema:
    period === 'intraday'
      ? params('from_symbol', 'to_symbol', 'interval', 'outputsize', 'datatype', 'apikey')
      : params('from_symbol', 'to_symbol', 'outputsize', 'datatype', 'apikey'),
  handler: (p) => fxSeries(p, period),
})

const crypto = (
  fn: string,
  period: Period,
  summary: string,
  description: string,
): RouteDef => ({
  fn,
  tag: 'currencies',
  summary,
  description,
  schema:
    period === 'intraday'
      ? params('symbol', 'market', 'interval', 'outputsize', 'datatype', 'apikey')
      : params('symbol', 'market', 'datatype', 'apikey'),
  handler: (p) => digitalSeries(p, period),
})

export const currenciesRoutes: RouteDef[] = [
  {
    fn: 'CURRENCY_EXCHANGE_RATE',
    tag: 'currencies',
    summary: 'Latest exchange rate',
    description:
      'Latest FX rate for a fiat pair, or the latest crypto price for BTC/ETH/SOL against USD. Reversed fiat pairs are inverted.',
    schema: params('from_currency', 'to_currency', 'apikey'),
    handler: currencyExchangeRate,
  },
  fx(
    'FX_INTRADAY',
    'intraday',
    'Intraday FX rates',
    'FX rates aggregated into OHLC buckets. The producer samples one rate per minute, so each bar is flat.',
  ),
  fx('FX_DAILY', 'daily', 'Daily FX rates', 'Daily OHLC built from the sampled FX rates.'),
  fx('FX_WEEKLY', 'weekly', 'Weekly FX rates', 'Weekly OHLC bucketed Monday to Sunday.'),
  fx('FX_MONTHLY', 'monthly', 'Monthly FX rates', 'Monthly OHLC labelled by the last sampled day.'),
  crypto(
    'CRYPTO_INTRADAY',
    'intraday',
    'Intraday crypto candles',
    'Candles built from `crypto_trades`, including summed quantity as volume.',
  ),
  crypto('DIGITAL_CURRENCY_DAILY', 'daily', 'Daily crypto candles', 'Daily Binance klines from `crypto_ohlcv`.'),
  crypto('DIGITAL_CURRENCY_WEEKLY', 'weekly', 'Weekly crypto candles', 'Weekly candles bucketed from the daily klines.'),
  crypto('DIGITAL_CURRENCY_MONTHLY', 'monthly', 'Monthly crypto candles', 'Monthly candles bucketed from the daily klines.'),
]
