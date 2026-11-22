import { ch } from '../clickhouse'
import {
  need,
  pct,
  px,
  raw,
  vol,
  wantsCsv,
  csvResponse,
  csvText,
  type AvPayload,
  type Params,
} from './core'

type Row = Record<string, unknown>

async function query(queryText: string, params?: Record<string, unknown>): Promise<Row[]> {
  const rs = await ch.query({
    query: queryText,
    query_params: params,
    format: 'JSONEachRow',
  })
  const data = await rs.json<Row>()
  return Array.isArray(data) ? data : []
}

const INCOME: Record<string, string> = {
  'Total Revenue': 'totalRevenue',
  'Cost Of Revenue': 'costOfRevenue',
  'Gross Profit': 'grossProfit',
  'Research And Development': 'researchAndDevelopment',
  'Selling General And Administration': 'sellingGeneralAndAdministrative',
  'Operating Expense': 'operatingExpenses',
  'Operating Income': 'operatingIncome',
  'Net Non Operating Interest Income Expense': 'netInterestIncome',
  'Interest Expense': 'interestExpense',
  'Pretax Income': 'incomeBeforeTax',
  'Tax Provision': 'incomeTaxExpense',
  'Net Income Common Stockholders': 'netIncome',
  'EBITDA': 'ebitda',
  'Reconciled Depreciation': 'depreciationAndAmortization',
  'Diluted Average Shares': 'dilutedAverageShares',
  'Basic Average Shares': 'basicAverageShares',
  'Diluted EPS': 'dilutedEPS',
  'Basic EPS': 'basicEPS',
}

const BALANCE: Record<string, string> = {
  'Total Assets': 'totalAssets',
  'Total Liabilities Net Minority Interest': 'totalLiabilities',
  'Stockholders Equity': 'totalShareholderEquity',
  'Cash And Cash Equivalents': 'cashAndCashEquivalentsAtCarryingValue',
  'Cash Cash Equivalents And Short Term Investments': 'cashAndShortTermInvestments',
  'Current Assets': 'currentAssets',
  'Current Liabilities': 'currentLiabilities',
  'Total Debt': 'shortLongTermDebtTotal',
  'Long Term Debt': 'longTermDebt',
  'Inventory': 'inventory',
  'Retained Earnings': 'retainedEarnings',
  'Ordinary Shares Number': 'commonStockSharesOutstanding',
  'Net Tangible Assets': 'netTangibleAssets',
}

const CASHFLOW: Record<string, string> = {
  'Operating Cash Flow': 'operatingCashflow',
  'Investing Cash Flow': 'cashflowFromInvestment',
  'Financing Cash Flow': 'cashflowFromFinancing',
  'Free Cash Flow': 'freeCashFlow',
  'Capital Expenditure': 'capitalExpenditures',
  'Net Income From Continuing Operations': 'netIncome',
  'Depreciation And Amortization': 'depreciationDepletionAndAmortization',
  'Change In Cash': 'changeInCash',
  'Dividend Paid': 'dividendPayout',
  'Repurchase Of Capital Stock': 'paymentsForRepurchaseOfCommonStock',
}

const STATEMENTS = {
  INCOME_STATEMENT: { statement: 'income', map: INCOME },
  BALANCE_SHEET: { statement: 'balance', map: BALANCE },
  CASH_FLOW: { statement: 'cashflow', map: CASHFLOW },
} as const

export type StatementFunction = keyof typeof STATEMENTS

export async function statement(
  fn: StatementFunction,
  p: Params,
): Promise<AvPayload> {
  const symbol = need(p, 'symbol').toUpperCase()
  const { statement: kind, map } = STATEMENTS[fn]
  const rows = await query(
    `SELECT toString(end_date) AS end_date, period, line_item, value
     FROM analytics.fundamentals FINAL
     WHERE symbol = {symbol:String} AND statement = {statement:String}
     ORDER BY end_date DESC`,
    { symbol, statement: kind },
  )

  const reports = new Map<string, { period: string; values: Record<string, string> }>()
  for (const row of rows) {
    const end = String(row.end_date)
    const field = map[String(row.line_item)]
    if (!field) continue
    const entry = reports.get(end) ?? {
      period: String(row.period),
      values: {},
    }
    entry.values[field] = String(Number(row.value))
    reports.set(end, entry)
  }

  const build = (period: 'annual' | 'quarterly') =>
    [...reports.entries()]
      .filter(([, entry]) => entry.period === period)
      .sort((a, b) => b[0].localeCompare(a[0]))
      .map(([end, entry]) => ({
        fiscalDateEnding: end,
        reportedCurrency: 'USD',
        ...entry.values,
      }))

  return {
    json: {
      symbol,
      annualReports: build('annual'),
      quarterlyReports: build('quarterly'),
    },
  }
}

export async function earnings(p: Params): Promise<AvPayload> {
  const symbol = need(p, 'symbol').toUpperCase()
  const rows = await query(
    `SELECT toString(end_date) AS end_date, line_item, value
     FROM analytics.fundamentals FINAL
     WHERE symbol = {symbol:String} AND statement = 'earnings'
     ORDER BY end_date DESC`,
    { symbol },
  )

  const byYear = new Map<string, Record<string, number>>()
  for (const row of rows) {
    const end = String(row.end_date)
    const entry = byYear.get(end) ?? {}
    entry[String(row.line_item)] = Number(row.value)
    byYear.set(end, entry)
  }

  const quarterlyEarnings = [...byYear.entries()]
    .sort((a, b) => b[0].localeCompare(a[0]))
    .map(([end, values]) => ({
      fiscalDateEnding: end,
      reportedDate: '',
      reportedEPS: values.epsActual !== undefined ? String(values.epsActual) : 'None',
      estimatedEPS: values.epsEstimate !== undefined ? String(values.epsEstimate) : 'None',
      surprise: values.epsDifference !== undefined ? String(values.epsDifference) : 'None',
      surprisePercentage:
        values.surprisePercent !== undefined ? String(values.surprisePercent) : 'None',
      reportTime: '',
    }))

  return { json: { symbol, annualEarnings: [], quarterlyEarnings } }
}

export async function dividends(p: Params): Promise<AvPayload | Response> {
  const symbol = need(p, 'symbol').toUpperCase()
  const rows = await query(
    `SELECT toString(action_date) AS d, value
     FROM analytics.corporate_actions FINAL
     WHERE symbol = {symbol:String} AND action = 'dividend'
     ORDER BY action_date DESC`,
    { symbol },
  )
  const data = rows.map((row) => ({
    ex_dividend_date: String(row.d),
    declaration_date: '',
    record_date: '',
    payment_date: '',
    amount: px(Number(row.value)),
  }))
  const json = { symbol, data }
  if (!wantsCsv(p)) return { json }
  return csvResponse(
    csvText(
      ['symbol', 'ex_dividend_date', 'declaration_date', 'record_date', 'payment_date', 'amount'],
      data.map((row) => [symbol, ...Object.values(row)]),
    ),
  )
}

export async function splits(p: Params): Promise<AvPayload | Response> {
  const symbol = need(p, 'symbol').toUpperCase()
  const rows = await query(
    `SELECT toString(action_date) AS d, value
     FROM analytics.corporate_actions FINAL
     WHERE symbol = {symbol:String} AND action = 'split'
     ORDER BY action_date DESC`,
    { symbol },
  )
  const data = rows.map((row) => ({
    effective_date: String(row.d),
    split_factor: px(Number(row.value)),
  }))
  const json = { symbol, data }
  if (!wantsCsv(p)) return { json }
  return csvResponse(
    csvText(
      ['symbol', 'effective_date', 'split_factor'],
      data.map((row) => [symbol, row.effective_date, row.split_factor]),
    ),
  )
}

const DISPOSITION = (transaction: string): string => {
  const text = transaction.toLowerCase()
  if (text.includes('sale')) return 'S'
  if (text.includes('purchase') || text.includes('buy')) return 'P'
  if (text.includes('gift')) return 'G'
  if (text.includes('conversion') || text.includes('exercise')) return 'M'
  if (text.includes('award') || text.includes('grant')) return 'A'
  if (text.includes('tax') || text.includes('payment')) return 'F'
  return 'J'
}

export async function insiderTransactions(p: Params): Promise<AvPayload | Response> {
  const symbol = need(p, 'symbol').toUpperCase()
  const conditions = ['symbol = {symbol:String}']
  const params: Record<string, unknown> = { symbol }
  if (p.from) {
    conditions.push('transaction_date >= toDate({from:String})')
    params.from = p.from
  }
  const rows = await query(
    `SELECT toString(transaction_date) AS d, insider, position, transaction,
            shares, value, ownership
     FROM analytics.insider_transactions FINAL
     WHERE ${conditions.join(' AND ')}
     ORDER BY transaction_date DESC
     LIMIT 1000`,
    params,
  )
  const data = rows.map((row) => {
    const shares = Number(row.shares)
    const value = row.value === null || row.value === undefined ? 0 : Number(row.value)
    return {
      transaction_date: String(row.d),
      ticker: symbol,
      executive: raw(row.insider),
      executive_title: raw(row.position),
      security_type:
        raw(row.ownership) === 'D'
          ? 'Common Stock (Direct)'
          : 'Common Stock (Indirect)',
      acquisition_or_disposal: DISPOSITION(raw(row.transaction)),
      shares: String(shares),
      share_price: shares ? px(value / shares) : '0',
    }
  })
  const json = { data }
  if (!wantsCsv(p)) return { json }
  return csvResponse(
    csvText(
      ['transaction_date', 'ticker', 'executive', 'executive_title', 'security_type', 'acquisition_or_disposal', 'shares', 'share_price'],
      data.map((row) => Object.values(row)),
    ),
  )
}

export async function institutionalHoldings(p: Params): Promise<AvPayload | Response> {
  const symbol = need(p, 'symbol').toUpperCase()
  const catalog = await query(
    `SELECT name FROM analytics.symbol_catalog FINAL
     WHERE symbol = {symbol:String}
     LIMIT 1`,
    { symbol },
  )
  const name = String(catalog[0]?.name ?? '').replace(/[.,]/g, '')
  const token = name.split(/\s+/).slice(0, 2).join(' ')

  const rows = token
    ? await query(
        `WITH periods AS (
            SELECT filer_name, period,
                   sum(shares) AS shares_held,
                   max(filing_date) AS last_reported
            FROM analytics.institutional_holdings FINAL
            WHERE positionCaseInsensitive(issuer, {token:String}) > 0
            GROUP BY filer_name, period
         ), ranked AS (
            SELECT *,
                   row_number() OVER (PARTITION BY filer_name ORDER BY period DESC) AS rn
            FROM periods
         )
         SELECT cur.filer_name AS filer_name,
                cur.shares_held AS shares_held,
                toString(cur.last_reported) AS last_reported,
                ifNull(prev.shares_held, cur.shares_held) AS previous_shares
         FROM ranked AS cur
         LEFT JOIN ranked AS prev
           ON cur.filer_name = prev.filer_name AND prev.rn = 2
         WHERE cur.rn = 1
         ORDER BY cur.shares_held DESC
         LIMIT 100`,
        { token },
      )
    : []

  const parsed = rows.map((row) => {
    const shares = Number(row.shares_held)
    const previous = Number(row.previous_shares)
    return { name: raw(row.filer_name), shares, change: shares - previous, previous, last: raw(row.last_reported) }
  })
  const holdings = parsed.map((row) => ({
    holder_name: row.name,
    shares_held: vol(row.shares),
    shares_changed: vol(row.change),
    shares_changed_percentage:
      row.previous > 0 ? pct((row.change / row.previous) * 100) : '0%',
    change_type:
      row.change > 0 ? 'increased' : row.change < 0 ? 'decreased' : 'unchanged',
    last_reported: row.last,
  }))
  const total = (pick: (row: (typeof parsed)[number]) => boolean) =>
    parsed.filter(pick).reduce((sum, row) => sum + row.shares, 0)
  const totalChange = (pick: (row: (typeof parsed)[number]) => boolean) =>
    parsed.filter(pick).reduce((sum, row) => sum + row.change, 0)
  const totalShares = total(() => true)

  const json = {
    symbol,
    total_institutional_holders: String(holdings.length),
    total_institutional_shares: vol(totalShares),
    holders_with_increased_holdings: String(parsed.filter((row) => row.change > 0).length),
    shares_with_increased_holdings: vol(totalChange((row) => row.change > 0)),
    holders_with_decreased_holdings: String(parsed.filter((row) => row.change < 0).length),
    shares_with_decreased_holdings: vol(totalChange((row) => row.change < 0)),
    holders_with_unchanged_holdings: String(parsed.filter((row) => row.change === 0).length),
    shares_with_unchanged_holdings: vol(total((row) => row.change === 0)),
    total_institutional_ownership_percentage: '',
    holdings,
  }
  if (!wantsCsv(p)) return { json }
  return csvResponse(
    csvText(
      ['symbol', 'holder_name', 'shares_held', 'shares_changed', 'shares_changed_percentage', 'change_type', 'last_reported'],
      holdings.map((row) => [symbol, ...Object.values(row)]),
    ),
  )
}
