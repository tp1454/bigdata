# kafka-flink-clickhouse-grafana

Local streaming analytics pipeline: financial market producers publish to
Kafka, a PyFlink job parses and batches them into ClickHouse, and Grafana
reads ClickHouse.

```
producers (13) --> Kafka (3 brokers, 10 topics) --> Flink (parse + sink) --> ClickHouse (analytics.*) --> Grafana (2 dashboards)
```

## Scope and data terms

This is a local, single-user demo for personal, educational, or internal use.
It is not affiliated with Alpha Vantage and is not a data-redistribution
service. Do not expose it as a public or Alpha Vantage-compatible API: free
access to an upstream source is not a license to redistribute its data.

| Source | Used for | Before scaling |
|---|---|---|
| Yahoo Finance (`yfinance`) | equities, indices, commodities, fundamentals, insiders, corporate actions | Unofficial; check Yahoo's terms and rate limits |
| Binance, CoinGecko | crypto trades, market data, klines | Check API terms, rate limits, and redistribution rules |
| Frankfurter (ECB) | FX rates | Check terms and attribution requirements |
| BLS, US Treasury | macro series, yield curve | Public data; respect each site's terms and rate limits |
| SEC EDGAR | filings | Set a real `SEC_CONTACT_EMAIL`; follow SEC's fair-access policy |

Keep credentials in `.env` (gitignored) and never commit them. For commercial
use, redistribution, or an SLA, obtain a license from the data source or use a
licensed provider. Confirm each source's current terms; this is not legal
advice.

## Quickstart

```bash
make setup        # copies .env.example -> .env; set real passwords
make deploy       # build, start, then run the smoke check
make url          # Grafana / Flink UI / ClickHouse / Kafka endpoints
```

The first start creates the Kafka topics and the ClickHouse schema. On an
existing volume, `make deploy` re-applies topics and schema and resubmits the
Flink job. Use `make nuke` only when the schema or broker layout changed
incompatibly; it deletes all local data. The RF3 layout uses new
`volumes/kafka-data-1..3` and a fresh Zookeeper, so an old single-broker
cluster needs `make nuke` (or a `kafka-reassign-partitions` migration); the
topic script does not rewrite existing RF1 assignments.

## Services and ports

| Service      | URL / port                    |
|--------------|-------------------------------|
| Grafana      | http://localhost:3000         |
| Flink UI     | http://localhost:8081         |
| ClickHouse   | http://localhost:8123         |
| Kafka        | localhost:29092 (bootstrap; 29092-29094) |

Credentials and ports come from `.env` (see `.env.example`). Compose refuses
to start without `CLICKHOUSE_PASSWORD` and `GRAFANA_ADMIN_PASSWORD`.

## Topics and tables

| Kafka topic              | ClickHouse table                  | Producer                  |
|--------------------------|-----------------------------------|---------------------------|
| `equities.prices`        | `analytics.equities_prices`       | Yahoo Finance, indices    |
| `crypto.trades`          | `analytics.crypto_trades`         | Binance websocket         |
| `crypto.ohlcv`           | `analytics.crypto_ohlcv`          | Binance klines            |
| `crypto.market`          | `analytics.crypto_market_data`    | CoinGecko                 |
| `forex.rates`            | `analytics.forex_rates`           | Frankfurter (ECB)         |
| `macro.indicators`       | `analytics.macro_indicators`      | BLS, US Treasury, Yahoo   |
| `fundamentals.filings`   | `analytics.company_filings`       | SEC EDGAR                 |
| `fundamentals.actions`   | `analytics.corporate_actions`     | Yahoo dividends/splits    |
| `fundamentals.statements` | `analytics.fundamentals`         | Yahoo statements/earnings |
| `fundamentals.insider`   | `analytics.insider_transactions`  | Yahoo insiders            |
| (parse failures)         | `analytics.pipeline_errors`       | Flink dead-letter         |

Topics get 3 partitions. Tick topics keep 7 days in Kafka; ClickHouse keeps
90 days of tick data. Fundamentals topics keep 30 days, and
`fundamentals.filings` is compacted.

## Delivery semantics

The Flink job is **at-least-once**: each record is inserted before the task
continues, Kafka offsets commit with the checkpoint, and a failed insert fails
the task instead of dropping rows, so the batch is replayed. PyFlink has no
checkpoint hook for Python sinks, which is why there is no local buffer.
Replays are deduplicated by ClickHouse `ReplacingMergeTree` on natural keys
(`trade_id`, `symbol + ts`, ...); inserts use server-side `async_insert` so the
small writes are batched into parts by ClickHouse itself.

Deduplication happens on merge, so read tick tables with `argMax(...)` (as the
dashboards do) or `FINAL` before relying on uniqueness.

Unparseable records are written to `pipeline_errors` (topic, error, truncated
raw payload) instead of being dropped silently.

## Indicators and analytics

`analytics.equities_prices_clean` dedupes replays and drops impossible bars.
`analytics.equities_indicators` builds SMA(20/50/200), Bollinger bands,
simple-average RSI(14) and ATR(14) (`rsi_14_sma`, `atr_14_sma`), 1/5-bar
returns, 20-bar volatility, and a 20-bar volume average over the stored
bars (1-minute for equities, daily for indices).
Both are views, so they stay current without a write job; run
`make schema` once on an existing volume.

## Make targets

Run `make` or `make help` for the full list.

| Target                 | What it does                                         |
|------------------------|------------------------------------------------------|
| `make setup`           | create `.env`, build images                          |
| `make dev`             | create `.venv` with pytest/ruff                      |
| `make deploy`          | build + start + topics/schema + job reset + smoke     |
| `make redeploy`        | recreate containers with current config              |
| `make smoke`           | end-to-end check: services, job, topics, new rows    |
| `make check`           | lint + unit tests + compose validation               |
| `make test` / `lint`   | pytest / ruff                                        |
| `make check-data`      | row counts and latest timestamp per table            |
| `make schema`          | apply ClickHouse schema + views (idempotent)         |
| `make job-reset`       | cancel and resubmit the Flink job                    |
| `make query SQL='...'` | one-off ClickHouse query                             |
| `make feed TOPIC=...`  | tail Kafka messages                                  |
| `make test-topics`     | topic partitions and message counts                  |
| `make logs SERVICE=x`  | tail one service                                     |
| `make shell SERVICE=x` | shell inside a container                             |
| `make load RATE=n`     | synthetic Kafka load (`DURATION`, `TOPIC`, `SYMBOL`) |
| `make nuke`            | wipe all data and restart                            |

`make smoke` is the useful one: it fails when Kafka, Flink, or ClickHouse
silently stops moving data.

## Environment

| Variable                 | Required | Purpose                               |
|--------------------------|----------|---------------------------------------|
| `CLICKHOUSE_PASSWORD`    | yes      | ClickHouse `default` user password    |
| `GRAFANA_ADMIN_PASSWORD` | yes      | Grafana admin password                |
| `SEC_CONTACT_EMAIL`      | for SEC  | real contact, SEC requires it in UA   |
| `SYMBOLS`, `SYMBOLS_SHARD`, `CRYPTO_SYMBOLS`, `CRYPTO_SYMBOLS_SHARD`, `POLL_INTERVAL`, `POLL_BACKOFF`, `PUBLISH_TIMEOUT` | no | producer scaling knobs |
| `CLICKHOUSE_*`, `GRAFANA_PORT`, `KAFKA_PORT`, `KAFKA_PORT_2/3` | no | ports/users/db overrides |

## Layout

```
.
├── docker-compose.yml          Kafka, ClickHouse, Grafana, Flink, producers
├── docker-compose.override.yml dev hot reload / debug conveniences
├── Makefile                    setup, dev, deploy, smoke, check-data, ...
├── .env.example                config template (copy to .env)
├── init/
│   ├── kafka/create-topics.sh  topics, partitions, retention
│   └── clickhouse/01_init.sql  analytics schema
├── producers/src/              one process per data source
│   ├── equities/               Yahoo prices, indices, fundamentals, SEC EDGAR
│   ├── crypto/                 Binance websocket, Binance klines, CoinGecko
│   ├── forex/                  Frankfurter (ECB)
│   └── macro/                  BLS, US Treasury, commodities
├── flink-jobs/src/             PyFlink job, transforms, config
├── configs/grafana/            datasource + dashboard provisioning
├── dashboards/                 Market Analytics, Pipeline Health
├── tests/unit/                 transform + dead-letter tests
├── scripts/smoke.sh            end-to-end check used by make smoke
├── requirements-dev.txt        pytest, ruff
└── ruff.toml                   lint rules
```

## Known limits

- Three Kafka brokers (RF3, min ISR 2) on one Zookeeper. Zookeeper is still
  a single point, and ClickHouse is a single instance with
  `ReplacingMergeTree` rather than a replicated table. KRaft and ClickHouse
  replication are not migrated here.
- Flink checkpoints live in JobManager memory: task failures recover, a
  JobManager restart loses the job. Add a durable state backend and HA for
  anything beyond local use.
- No schema registry; producers and the transform mapping are the contract.
- Grafana installs `grafana-clickhouse-datasource` at startup, so the first
  start needs network access.
- 90-day ClickHouse TTL and 7-day Kafka retention are demo choices; change
  them in `init/clickhouse/01_init.sql` and `init/kafka/create-topics.sh`.
- Data-source terms: this stack is for local/personal use. Do not publish a
  public or Alpha Vantage-compatible API from these feeds; see
  [Scope and data terms](#scope-and-data-terms).
