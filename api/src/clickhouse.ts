import { createClient } from '@clickhouse/client'

import { config } from './config'

export const ch = createClient({
  url: config.clickhouse.url,
  username: config.clickhouse.username,
  password: config.clickhouse.password,
  database: config.clickhouse.database,
  clickhouse_settings: {
    // UInt64 columns (trade_id) as JSON numbers; the dashboard/API contract
    // relies on numeric ids staying below 2^53.
    output_format_json_quote_64bit_integers: 0,
  },
})
