import { params, type RouteDef } from './shared'
import {
  bulkQuotes,
  daily,
  globalQuote,
  intraday,
  listingStatus,
  marketStatus,
  putCallRatio,
  symbolSearch,
  topGainersLosers,
  weeklyMonthly,
} from '../../av/stocks'

export const stocksRoutes: RouteDef[] = [
  {
    fn: 'TIME_SERIES_INTRADAY',
    tag: 'stocks',
    summary: 'Intraday equity bars',
    description:
      'One-minute Yahoo bars rolled up to `interval`. `interval` is required: `1min`, `5min`, `15min`, `30min` or `60min`.',
    schema: params('symbol', 'interval', 'outputsize', 'datatype', 'apikey'),
    handler: (p) => intraday(p),
  },
  {
    fn: 'TIME_SERIES_DAILY',
    tag: 'stocks',
    summary: 'Daily equity bars',
    description:
      'As-traded daily OHLCV, rolled up from the one-minute bars. `outputsize=compact` returns the latest 100 trading days.',
    schema: params('symbol', 'outputsize', 'datatype', 'apikey'),
    handler: (p) => daily(p, false),
  },
  {
    fn: 'TIME_SERIES_DAILY_ADJUSTED',
    tag: 'stocks',
    summary: 'Daily equity bars, split and dividend adjusted',
    description:
      'Daily OHLCV with `adjusted close`, `dividend amount` and `split coefficient` from `corporate_actions`. Adjustment is an approximation computed on read.',
    schema: params('symbol', 'outputsize', 'datatype', 'apikey'),
    handler: (p) => daily(p, true),
  },
  {
    fn: 'TIME_SERIES_WEEKLY',
    tag: 'stocks',
    summary: 'Weekly equity bars',
    description:
      'As-traded weekly OHLCV, bucketed Monday to Sunday from the daily bars.',
    schema: params('symbol', 'datatype', 'apikey'),
    handler: (p) => weeklyMonthly(p, 'weekly', false),
  },
  {
    fn: 'TIME_SERIES_WEEKLY_ADJUSTED',
    tag: 'stocks',
    summary: 'Weekly equity bars, split and dividend adjusted',
    description:
      'Weekly OHLCV plus adjusted close, dividend amount and split coefficient.',
    schema: params('symbol', 'datatype', 'apikey'),
    handler: (p) => weeklyMonthly(p, 'weekly', true),
  },
  {
    fn: 'TIME_SERIES_MONTHLY',
    tag: 'stocks',
    summary: 'Monthly equity bars',
    description: 'As-traded monthly OHLCV, labelled by the last trading day.',
    schema: params('symbol', 'datatype', 'apikey'),
    handler: (p) => weeklyMonthly(p, 'monthly', false),
  },
  {
    fn: 'TIME_SERIES_MONTHLY_ADJUSTED',
    tag: 'stocks',
    summary: 'Monthly equity bars, split and dividend adjusted',
    description:
      'Monthly OHLCV plus adjusted close, dividend amount and split coefficient.',
    schema: params('symbol', 'datatype', 'apikey'),
    handler: (p) => weeklyMonthly(p, 'monthly', true),
  },
  {
    fn: 'GLOBAL_QUOTE',
    tag: 'stocks',
    summary: 'Latest quote for one symbol',
    description:
      'Latest close, previous close, change and change percent for `symbol`.',
    schema: params('symbol', 'datatype', 'apikey'),
    handler: globalQuote,
  },
  {
    fn: 'REALTIME_BULK_QUOTES',
    tag: 'stocks',
    summary: 'Latest quotes for several symbols',
    description:
      'Comma-separated `symbol` list, up to the number of tracked tickers. Returns price, volume and timestamp per symbol.',
    schema: params('symbol', 'datatype', 'apikey'),
    handler: bulkQuotes,
  },
  {
    fn: 'SYMBOL_SEARCH',
    tag: 'stocks',
    summary: 'Search the symbol catalogue',
    description:
      'Best matches from `symbol_catalog` by symbol or company name. `keywords` is required.',
    schema: params('keywords', 'datatype', 'apikey'),
    handler: symbolSearch,
  },
  {
    fn: 'LISTING_STATUS',
    tag: 'stocks',
    summary: 'Active listing status',
    description:
      'Active symbols and ETFs from `symbol_catalog`, as CSV by default. `datatype=json` returns a JSON array.',
    schema: params('state', 'datatype', 'date', 'apikey'),
    handler: listingStatus,
  },
  {
    fn: 'MARKET_STATUS',
    tag: 'stocks',
    summary: 'US equity, FX and crypto market status',
    description:
      'Current open/closed state for US equities, the 24/5 FX market and the 24/7 crypto market.',
    schema: params('apikey'),
    handler: () => marketStatus(),
  },
  {
    fn: 'TOP_GAINERS_LOSERS',
    tag: 'stocks',
    summary: 'Top gainers, losers and most active',
    description:
      'Ranks tracked equities by the latest daily change and volume.',
    schema: params('datatype', 'apikey'),
    handler: topGainersLosers,
  },
  {
    fn: 'REALTIME_PUT_CALL_RATIO',
    tag: 'stocks',
    summary: 'Latest put-call ratio',
    description:
      'Full-chain and per-expiration put-call ratios from `options_metrics`, using the newest snapshot per expiry.',
    schema: params('symbol', 'datatype', 'apikey'),
    handler: (p) => putCallRatio(p, false),
  },
  {
    fn: 'HISTORICAL_PUT_CALL_RATIO',
    tag: 'stocks',
    summary: 'Put-call ratio for a date',
    description:
      'Same shape as `REALTIME_PUT_CALL_RATIO` for the `date` (defaults to the newest snapshot).',
    schema: params('symbol', 'date', 'datatype', 'apikey'),
    handler: (p) => putCallRatio(p, true),
  },
]
