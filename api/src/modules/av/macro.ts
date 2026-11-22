import { params, type RouteDef } from './shared'
import {
  COMMODITIES,
  commodity,
  consumerPriceIndex,
  inflation,
  treasuryYield,
  unemployment,
} from '../../av/macro'

const economic = {
  schema: params('interval', 'datatype', 'apikey'),
  tag: 'economic',
} as const

export const macroRoutes: RouteDef[] = [
  {
    fn: 'TREASURY_YIELD',
    summary: 'US Treasury constant-maturity yield',
    description:
      'Daily, weekly or monthly yield for `maturity`. Only 3-month, 2-year, 10-year and 30-year maturities are collected.',
    schema: params('interval', 'maturity', 'datatype', 'apikey'),
    tag: 'economic',
    handler: treasuryYield,
  },
  {
    fn: 'CPI',
    ...economic,
    summary: 'Consumer Price Index',
    description: 'BLS CPI for all urban consumers, monthly or annual.',
    handler: consumerPriceIndex,
  },
  {
    fn: 'INFLATION',
    ...economic,
    summary: 'Year-over-year inflation',
    description: 'CPI percentage change computed against the same month one year earlier.',
    handler: inflation,
  },
  {
    fn: 'UNEMPLOYMENT',
    ...economic,
    summary: 'Unemployment rate',
    description: 'BLS unemployment rate, monthly or annual.',
    handler: unemployment,
  },
  ...Object.entries(COMMODITIES).map(
    ([fn, definition]): RouteDef => ({
      fn,
      tag: 'commodities',
      summary: `${definition.name} history`,
      description: `Daily, weekly or monthly close from the \`${definition.indicator}\` macro series, in ${definition.unit}.`,
      schema: params('interval', 'datatype', 'apikey'),
      handler: (p) => commodity(fn as keyof typeof COMMODITIES, p),
    }),
  ),
]
