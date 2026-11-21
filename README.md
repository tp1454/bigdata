# kafka-flink-clickhouse-grafana

Local streaming analytics pipeline: financial market producers publish to
Kafka, a PyFlink job parses and batches them into ClickHouse, and Grafana
reads ClickHouse.

```
producers (13) --> Kafka (3 brokers, 10 topics) --> Flink (parse + sink) --> ClickHouse (2 shards x 2 replicas) --> Grafana
                                                                              |
                                                                              +--> S3Proxy (cold TTL volume)
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

The first start creates the Kafka topics and, once all four ClickHouse nodes
are healthy, runs the `clickhouse-schema` one-shot to create the cluster
schema. On an existing volume, `make deploy` re-applies topics and schema and
resubmits the Flink job. Use `make nuke` only when the schema or broker layout changed
incompatibly; it deletes all local data. The RF3 layout uses new
`volumes/kafka-data-1..3` and a fresh Zookeeper, so an old single-broker
cluster needs `make nuke` (or a `kafka-reassign-partitions` migration); the
topic script does not rewrite existing RF1 assignments.

## Services and ports

| Service      | URL / port                    |
|--------------|-------------------------------|
| Grafana      | http://localhost:3000         |
| Flink UI     | http://localhost:8081         |
| Prometheus   | http://localhost:9090         |
| ClickHouse   | http://localhost:8123         |
| Kafka        | localhost:29092 (bootstrap; 29092-29094) |

`clickhouse` is shard 1, replica 1 and the cluster entry point: Flink and
Grafana connect to it, and its `Distributed` tables fan out across the four
ClickHouse nodes. `clickhouse-2`, `clickhouse-3` and `clickhouse-4` are
internal only. S3Proxy (cold tier) is internal too.

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

Topics get `TOPIC_PARTITIONS` partitions (default 3). Tick topics keep 7 days
in Kafka; ClickHouse keeps 90 days of tick data. The slow macro/filings/insider
tables keep 10 years and are partitioned by month; `fundamentals.filings` is
compacted.

Each public table is a `Distributed` table over a `ReplicatedReplacingMergeTree`
(`*_local`) pair on two shards. The shard key is `cityHash64(trade_id)` for
crypto trades (spreads BTC) and `cityHash64(symbol)`-style keys elsewhere, so
all rows for one symbol stay together. Parts older than 7-30 days (tick) or
1 year (slow) move to the S3Proxy-backed `cold` volume; deletion still follows
the old TTL.

## Delivery semantics

The Flink jobs are **at-least-once**: the JDBC sink batches rows and flushes
them on every checkpoint, so Kafka offsets commit only after the rows reach
ClickHouse. The connector is JVM-side (`flink-connector-jdbc` plus the
ClickHouse JDBC driver); a failed flush fails the task and the batch is
replayed. Replays are deduplicated by ClickHouse `ReplacingMergeTree` on
natural keys (`trade_id`, `symbol + ts`, ...).

Deduplication happens on merge within each shard; the clean equity view uses
`argMax(..., ingested_at)` instead of `FINAL`. Other tick reads should use
`argMax` or `FINAL` before relying on uniqueness.

Unparseable records are written to `pipeline_errors` (topic, error, truncated
raw payload) instead of being dropped silently.

A single `clickhouse-keeper` coordinates replication and distributed DDL. It is
not replicated, so it is the remaining ClickHouse single point.

## Monitoring

Prometheus scrapes `kafka-exporter` for topic offsets and consumer-group lag,
and the Flink Prometheus reporter on `jobmanager:9249` and
`taskmanager:9249` for records in/out, checkpoint duration and backpressure.
Grafana's **Pipeline Input** dashboard shows input rate, lag and processing
rate next to the existing output dashboards.

Kafka offsets commit only at Flink checkpoints, so do not read a rate from the
committed offset: it is bursty. Use consumer lag plus Flink's
`numRecordsInPerSecond` instead. Input rate comes from Kafka log-end offsets
(`kafka_topic_partition_current_offset`), which track producers directly.

Alert rules live in `configs/prometheus/alerts.yml`. They show as firing in
Prometheus, but sending notifications needs an Alertmanager, which this stack
does not run.

## Indicators and analytics

`analytics.equities_prices_clean` dedupes replays and drops impossible bars.
`analytics.equities_indicators` builds SMA(20/50/200), Bollinger bands,
simple-average RSI(14) and ATR(14) (`rsi_14_sma`, `atr_14_sma`), 1/5-bar
returns, 20-bar volatility, and a 20-bar volume average over the stored
bars (1-minute for equities, daily for indices).
It is a view, so it stays current without a write job; run `make schema` once
on an existing volume.

`analytics.equities_indicators_snapshot` holds the same series precomputed by
a refreshable materialized view every 5 minutes, one refresh per replica from
that node's local shard. Read the snapshot for dashboards; read the view when
you need the last few minutes immediately. The refresh scans each shard's full
history, so it is cheap here but not a true incremental aggregate.

## Make targets

Run `make` or `make help` for the full list.

| Target                 | What it does                                         |
|------------------------|------------------------------------------------------|
| `make setup`           | create `.env`, build images                          |
| `make dev`             | create `.venv` with pytest/ruff                      |
| `make deploy`          | build + start + topics/schema + job reset + smoke     |
| `make redeploy`        | recreate containers with current config              |
| `make smoke`           | end-to-end check: services, jobs, topics, new rows   |
| `make check`           | lint + unit tests + compose validation               |
| `make test` / `lint`   | pytest / ruff                                        |
| `make check-data`      | row counts and latest timestamp per table            |
| `make schema`          | apply schema, TTLs, tiering and indicators MV        |
| `make job-reset`       | cancel and resubmit the Flink jobs                   |
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
| `S3_SECRET_KEY`          | no       | S3Proxy/ClickHouse cold-tier secret (default `s3secret`) |
| `SEC_CONTACT_EMAIL`      | for SEC  | real contact, SEC requires it in UA   |
| `SYMBOLS`, `SYMBOLS_SHARD`, `CRYPTO_SYMBOLS`, `CRYPTO_SYMBOLS_SHARD`, `POLL_INTERVAL`, `POLL_BACKOFF`, `PUBLISH_TIMEOUT` | no | producer scaling knobs |
| `TOPIC_PARTITIONS`, `FLINK_PARALLELISM`, `TASKMANAGER_SLOTS` | no | Flink/Kafka scale knobs (keep parallelism <= partitions and slots >= 4 x parallelism) |
| `CLICKHOUSE_*`, `GRAFANA_PORT`, `PROMETHEUS_PORT`, `KAFKA_PORT`, `KAFKA_PORT_2/3` | no | ports/users/db overrides |

## Layout

```
.
├── docker-compose.yml          Kafka, ClickHouse, Grafana, Flink, producers
├── docker-compose.override.yml dev hot reload / debug conveniences
├── Makefile                    setup, dev, deploy, smoke, check-data, ...
├── .env.example                config template (copy to .env)
├── init/
│   ├── kafka/create-topics.sh  topics, partitions, retention
│   ├── clickhouse/             base schema, views, retention and tiering
│   └── clickhouse-mv/          per-node refreshable indicators MV
├── configs/clickhouse/         Keeper + cluster/storage-policy config
├── producers/src/              one process per data source
│   ├── equities/               Yahoo prices, indices, fundamentals, SEC EDGAR
│   ├── crypto/                 Binance websocket, Binance klines, CoinGecko
│   ├── forex/                  Frankfurter (ECB)
│   └── macro/                  BLS, US Treasury, commodities
├── flink-jobs/src/             PyFlink job, transforms, config
├── configs/grafana/            datasource + dashboard provisioning
├── configs/prometheus/         scrape config + alert rules
├── dashboards/                 Market Analytics, Pipeline Health, Pipeline Input
├── tests/unit/                 transform + dead-letter tests
├── scripts/smoke.sh            end-to-end check used by make smoke
├── requirements-dev.txt        pytest, ruff
└── ruff.toml                   lint rules
```

## Known limits

- Three Kafka brokers (RF3, min ISR 2) on one Zookeeper. Zookeeper and
  `clickhouse-keeper` are each a single point. ClickHouse itself is now
  2 shards x 2 replicas with `Distributed` tables; Flink and Grafana still use
  one node as the entry point, so that node's HTTP availability matters even
  though the data is replicated.
- The 2x2 ClickHouse layout replaces the old single-node volume. Existing data
  needs a copy into the `*_local` tables or a fresh `make nuke`; the SQL files
  and named volumes are not compatible with the previous single node.
- ClickHouse partitions are created by `01/03/05` on a fresh volume. An
  existing cluster uses `make schema`, which re-applies the schema files and
  the indicators MV; it cannot change a table's PARTITION BY in place.
- The refreshable indicators MV is experimental in ClickHouse 24.8 and is
  created per node by `clickhouse-schema` because `ON CLUSTER` crashes the
  24.8 DDL worker. Each replica rescans its own shard every 5 minutes; it is
  not an incremental aggregate.
- S3Proxy is one container with a local directory backend, and Keeper is one
  node. Neither is replicated; the data and object tiers are separate failure
  domains from the entry-point node, not highly available ones.
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
