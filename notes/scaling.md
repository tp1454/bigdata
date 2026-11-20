# Scaling plan

The live stack runs at ~20-25 rows/s and ~2 MB in ClickHouse. Nothing is
hardware-bound yet. The limits below are software/config limits, in the order
they will be hit.

## This branch (`fix/scaling`, from `master`)

The Kubernetes manifests live on `feat/kubernetes-multi-host`. Until that
branch merges, this branch only changes code shared by Compose and k8s. The
first slice is:

- [x] Yahoo producer: fetch only bars after the last successful poll instead of
  re-downloading the whole day for every symbol on every poll. Catch failures
  per symbol so one bad/delisted symbol does not stop the other symbols.

Next slices on this branch (stack-independent):

- [x] Add a synthetic load harness (rate-controlled producer + benchmark) so
  each change can be measured. This is a prerequisite for claiming a speedup.
  Run it with `make load RATE=1000 DURATION=60 TOPIC=crypto.trades`.
- [x] Raise or back off the Yahoo poll interval: the default is 60 s, with a
  300 s backoff when no symbol returns data.
- [x] Split `SYMBOLS` across producer pods and add a crypto symbol config:
  `SYMBOLS_SHARD`, `CRYPTO_SYMBOLS` and `CRYPTO_SYMBOLS_SHARD`. Compose passes
  them; the k8s app-config and producer Deployments still need the same keys.
  The example below runs two Yahoo shards; run the matching shard for
  fundamentals/insider/corporate too, and both Binance producers for crypto.
- [x] Remove silent producer drops: publish waits up to `PUBLISH_TIMEOUT`
  (default 5 s) for queue space, and the Binance queue is 10k. Remaining drops
  are logged.

## Next branch (`feat/flink-scale`, from `fix/scaling`)

- [x] One config for `TOPIC_PARTITIONS`, `FLINK_PARALLELISM` and
  `TASKMANAGER_SLOTS`; keep `FLINK_PARALLELISM <= TOPIC_PARTITIONS` and
  `TASKMANAGER_SLOTS >= 4 * FLINK_PARALLELISM`. `make topics` grows existing
  topics to the new count.
- [x] Split the job into four groups (equities, crypto, fundamentals, macro)
  with a separate Kafka group per job. The Compose submit loops over the
  groups and `make job-reset` cancels all four. The first run starts each new
  group at the earliest retained offset, so expect a replay; ClickHouse
  dedupes keyed rows, but `pipeline_errors` can gain duplicates.
- [x] Native ClickHouse JDBC sink (Table API): each table has a `jdbc` sink;
  the connector batches rows and flushes on checkpoints, so Kafka offsets
  commit only after the rows reach ClickHouse. Requires the
  `flink-connector-jdbc` and ClickHouse JDBC JARs in the Flink image and the
  `tables.py` pyFile. A live checkpoint/restart test is still pending.

The k8s submit watcher on `feat/kubernetes-multi-host` still submits one job;
port the same group loop when the branches merge.

The ClickHouse TTL/partition/materialized-view work is the next branch;
metrics/autoscaler is last.

## Harness

```bash
make load RATE=1000 DURATION=60 TOPIC=crypto.trades

# Two Yahoo shards instead of the single service:
docker compose stop yahoo-producer
docker compose run --rm -e SYMBOLS_SHARD=0/2 yahoo-producer
docker compose run --rm -e SYMBOLS_SHARD=1/2 yahoo-producer
```

The k8s `app-config` and producer Deployments (on `feat/kubernetes-multi-host`)
need the same `SYMBOLS_SHARD`, `CRYPTO_SYMBOLS`, `CRYPTO_SYMBOLS_SHARD`,
`POLL_INTERVAL`, `POLL_BACKOFF` and `PUBLISH_TIMEOUT` keys, plus one producer
Deployment per shard.

## Branches / order (4-5 total)

1. `feat/producers-scale` (this branch's first slice): incremental Yahoo,
   per-symbol errors, no silent drops, symbol sharding, crypto key/symbols.
2. `feat/flink-scale`: partitions/parallelism/slots in one config, split jobs
   (done); batching sink via JDBC (done).
3. `feat/clickhouse-scale`: Keeper + replicated ClickHouse + `Distributed`
   tables, shard keys, read path, TTLs, tiering. Split into cluster and
   read/retention branches if it gets too large.
4. `feat/observability-autoscaling`: Prometheus/alerts, Flink operator,
   autoscaler, savepoints, Kafka broker topology.
5. `test/scaling-harness` (or the first commit of the producers branch).

## Blockers and evidence

| Blocker | Where | Why it blocks |
|---|---|---|
| 3 partitions, 3 Flink parallelism, 4 slots | `init/kafka/create-topics.sh`, `k8s/config.sh`, `k8s/base/flink.yaml` | Source parallelism cannot exceed partitions; the values are hardcoded in three files. |
| Single ClickHouse, no sharding/replication | `k8s/base/clickhouse.yaml`, `init/clickhouse/*.sql` | Every insert and query lands on one node; no horizontal path. |
| Synchronous per-message sink | `flink-jobs/src/job_kafka_to_clickhouse.py` | One HTTP insert per message; no client batching/async; latency × sink subtasks. |
| One job unions 10 topics | `flink-jobs/src/job_kafka_to_clickhouse.py` | One slow table/topic backpressures everything; one shared consumer group. |
| Serial, unsharded producers | `producers/src/equities/yahoo_producer.py`, `producers/src/crypto/binance_ws_producer.py` | One pod per feed; full-day Yahoo fetch per poll; crypto list hardcoded. |
| Silent drops | `producers/src/base_producer.py`, `binance_ws_producer.py` | Queue full drops records; data loss at high rate. |
| Symbol key skew | `producers/src/base_producer.py`, `binance_ws_producer.py` | BTC dominates; one key cannot be split across partitions. |
| Read path `FINAL` + window views | `init/clickhouse/02_clean.sql`, `04_indicators.sql` | Dedupe/indicators recomputed per query; cost grows with history. |
| Missing TTLs/partitions | `init/clickhouse/01_init.sql`, `05_fundamentals.sql` | Several tables grow forever in one partition. |
| Static Flink/no metrics | `k8s/base/flink.yaml`, `k8s/README.md` | Manual scaling, single JobManager, no lag/backpressure alerts. |

## Acceptance

- `make test` and `make lint` pass for each slice.
- A load harness reports rows/s, Kafka lag, Flink backpressure and ClickHouse
  p95 insert latency before and after.
- No change claims a scaling win without the harness numbers.
