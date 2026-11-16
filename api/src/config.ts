function required(name: string): string {
  const value = process.env[name]
  if (!value) {
    throw new Error(`${name} is required (set it in .env / compose)`)
  }
  return value
}

export const config = {
  host: process.env.API_HOST ?? '0.0.0.0',
  port: Number(process.env.API_PORT ?? 4000),
  clickhouse: {
    url: process.env.CLICKHOUSE_URL ?? 'http://clickhouse:8123',
    username: process.env.CLICKHOUSE_USER ?? 'default',
    password: required('CLICKHOUSE_PASSWORD'),
    database: process.env.CLICKHOUSE_DB ?? 'analytics',
  },
} as const
