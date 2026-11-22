import { params, type RouteDef } from './shared'
import {
  dividends,
  earnings,
  institutionalHoldings,
  insiderTransactions,
  splits,
  statement,
} from '../../av/fundamentals'

export const fundamentalsRoutes: RouteDef[] = [
  {
    fn: 'INCOME_STATEMENT',
    tag: 'fundamentals',
    summary: 'Annual and quarterly income statements',
    description:
      'Rows from `analytics.fundamentals` mapped to Alpha Vantage income-statement fields, split into `annualReports` and `quarterlyReports`.',
    schema: params('symbol', 'apikey'),
    handler: (p) => statement('INCOME_STATEMENT', p),
  },
  {
    fn: 'BALANCE_SHEET',
    tag: 'fundamentals',
    summary: 'Annual and quarterly balance sheets',
    description:
      'Balance-sheet line items from `analytics.fundamentals`, newest fiscal period first.',
    schema: params('symbol', 'apikey'),
    handler: (p) => statement('BALANCE_SHEET', p),
  },
  {
    fn: 'CASH_FLOW',
    tag: 'fundamentals',
    summary: 'Annual and quarterly cash flow statements',
    description:
      'Cash-flow line items from `analytics.fundamentals`, newest fiscal period first.',
    schema: params('symbol', 'apikey'),
    handler: (p) => statement('CASH_FLOW', p),
  },
  {
    fn: 'EARNINGS',
    tag: 'fundamentals',
    summary: 'Quarterly earnings history',
    description:
      'Reported EPS, estimated EPS, surprise and surprise percentage from the earnings rows.',
    schema: params('symbol', 'apikey'),
    handler: earnings,
  },
  {
    fn: 'DIVIDENDS',
    tag: 'fundamentals',
    summary: 'Dividend history',
    description:
      'Dividend events from `corporate_actions`. Only the ex-dividend date and amount are collected, so the other dates are empty.',
    schema: params('symbol', 'datatype', 'apikey'),
    handler: dividends,
  },
  {
    fn: 'SPLITS',
    tag: 'fundamentals',
    summary: 'Split history',
    description: 'Split events from `corporate_actions` with the split factor.',
    schema: params('symbol', 'datatype', 'apikey'),
    handler: splits,
  },
  {
    fn: 'INSIDER_TRANSACTIONS',
    tag: 'fundamentals',
    summary: 'Insider transactions',
    description:
      'Insider buys, sales and awards. The SEC transaction code is derived from the source description.',
    schema: params('symbol', 'from', 'datatype', 'apikey'),
    handler: insiderTransactions,
  },
  {
    fn: 'INSTITUTIONAL_HOLDINGS',
    tag: 'fundamentals',
    summary: 'Institutional (13F) holdings',
    description:
      'Aggregates `institutional_holdings` per filer. The table keys holdings by issuer name, so the match uses the symbol catalogue name rather than a CUSIP map.',
    schema: params('symbol', 'datatype', 'apikey'),
    handler: institutionalHoldings,
  },
]
