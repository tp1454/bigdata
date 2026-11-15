# kafka-flink-clickhouse-grafana

Local streaming analytics pipeline: financial market producers publish to
Kafka, a PyFlink job parses and batches them into ClickHouse, and Grafana
reads ClickHouse.

```
producers (7) --> Kafka (6 topics) --> Flink (parse + sink) --> ClickHouse (analytics.*) --> Grafana (2 dashboards)
```

## Quickstart

```bash
make setup        # copies .env.example -> .env; set real passwords
make deploy       # build, start, then run the smoke check
make url          # Grafana / Flink UI / ClickHouse / Kafka endpoints
```

The first start creates the Kafka topics and the ClickHouse schema. If the
stack has run before with an older schema, wipe local data first:

```bash
make nuke         # DANGER: deletes all local data and restarts
```

## Services and ports

| Service      | URL / port                    |
|--------------|-------------------------------|
| Grafana      | http://localhost:3000         |
| Flink UI     | http://localhost:8081         |
| ClickHouse   | http://localhost:8123         |
| Kafka        | localhost:29092               |

Credentials and ports come from `.env` (see `.env.example`). Compose refuses
to start without `CLICKHOUSE_PASSWORD` and `GRAFANA_ADMIN_PASSWORD`.

## Topics and tables

| Kafka topic              | ClickHouse table                  | Producer                  |
|--------------------------|-----------------------------------|---------------------------|
| `equities.prices`        | `analytics.equities_prices`       | Yahoo Finance             |
| `crypto.trades`          | `analytics.crypto_trades`         | Binance websocket         |
| `crypto.market`          | `analytics.crypto_market_data`    | CoinGecko                 |
| `forex.rates`            | `analytics.forex_rates`           | Frankfurter (ECB)         |
| `macro.indicators`       | `analytics.macro_indicators`      | BLS, US Treasury          |
| `fundamentals.filings`   | `analytics.company_filings`       | SEC EDGAR                 |
| (parse failures)         | `analytics.pipeline_errors`       | Flink dead-letter         |

Topics get 3 partitions. Tick topics keep 7 days in Kafka; ClickHouse keeps
90 days of tick data. `fundamentals.filings` is compacted.

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

## Make targets

Run `make` or `make help` for the full list.

| Target                 | What it does                                         |
|------------------------|------------------------------------------------------|
| `make setup`           | create `.env`, build images                          |
| `make dev`             | create `.venv` with pytest/ruff                      |
| `make deploy`          | build + start + `make smoke`                         |
| `make redeploy`        | recreate containers with current config              |
| `make smoke`           | end-to-end check: services, job, topics, new rows    |
| `make check`           | lint + unit tests + compose validation               |
| `make test` / `lint`   | pytest / ruff                                        |
| `make check-data`      | row counts and latest timestamp per table            |
| `make query SQL='...'` | one-off ClickHouse query                             |
| `make feed TOPIC=...`  | tail Kafka messages                                  |
| `make test-topics`     | topic partitions and message counts                  |
| `make logs SERVICE=x`  | tail one service                                     |
| `make shell SERVICE=x` | shell inside a container                             |
| `make nuke`            | wipe all data and restart                            |

`make smoke` is the useful one: it fails when Kafka, Flink, or ClickHouse
silently stops moving data.

## Environment

| Variable                 | Required | Purpose                               |
|--------------------------|----------|---------------------------------------|
| `CLICKHOUSE_PASSWORD`    | yes      | ClickHouse `default` user password    |
| `GRAFANA_ADMIN_PASSWORD` | yes      | Grafana admin password                |
| `SEC_CONTACT_EMAIL`      | for SEC  | real contact, SEC requires it in UA   |
| `CLICKHOUSE_*`, `GRAFANA_PORT`, `KAFKA_PORT` | no | ports/users/db overrides |

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
│   ├── equities/               Yahoo Finance, SEC EDGAR
│   ├── crypto/                 Binance websocket, CoinGecko
│   ├── forex/                  Frankfurter (ECB)
│   └── macro/                  BLS, US Treasury
├── flink-jobs/src/             PyFlink job, transforms, config
├── configs/grafana/            datasource + dashboard provisioning
├── dashboards/                 Market Analytics, Pipeline Health
├── tests/unit/                 transform + dead-letter tests
├── scripts/smoke.sh            end-to-end check used by make smoke
├── requirements-dev.txt        pytest, ruff
└── ruff.toml                   lint rules
```

## Known limits

- Single Kafka broker on Zookeeper, replication factor 1: no HA. KRaft would
  remove Zookeeper; not migrated here.
- Flink checkpoints live in JobManager memory: task failures recover, a
  JobManager restart loses the job. Add a durable state backend and HA for
  anything beyond local use.
- No schema registry; producers and the transform mapping are the contract.
- Grafana installs `grafana-clickhouse-datasource` at startup, so the first
  start needs network access.
- 90-day ClickHouse TTL and 7-day Kafka retention are demo choices; change
  them in `init/clickhouse/01_init.sql` and `init/kafka/create-topics.sh`.
