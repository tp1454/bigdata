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

## Next branch (`feat/clickhouse-scale`, from `feat/flink-native-sink`)

- [x] Remove `FINAL` from the equities clean view; use
  `argMax(..., ingested_at)` so the view no longer forces a full merge.
- [x] Add monthly partitions and a 10-year TTL to the slow tables; the
  `07_tiering.sql` migration applies the TTL (and cold tier) to existing
  tables.
- [x] ClickHouse Keeper + replicated ClickHouse + `Distributed` tables and
  shard keys. Compose runs 2 shards x 2 replicas behind one `analytics_cluster`
  (shard 1 keys by symbol; crypto trades shard by `trade_id`). The public table
  names are `Distributed`; `*_local` is the ReplicatedMergeTree on each node.
  One Keeper is still a single point. The old single-node volume is not
  compatible: use a fresh volume (`make nuke`) or copy data into `*_local`.
- [x] Materialized/incremental `equities_indicators` (the window view remains):
  `equities_indicators` is still the live window view;
  `equities_indicators_mv` is a refreshable materialized view (5 minutes) that
  recomputes from each node's local shard, and `equities_indicators_snapshot`
  is the `Distributed` read table. The refresh is a full rescan of each shard,
  not a true incremental aggregate; it moves cost off dashboard queries.
- [x] S3/MinIO tiering. A single S3Proxy container provides the S3 API and a
  bucket is the `cold` volume of the `tiered` storage policy, one prefix per
  ClickHouse node. (MinIO's Docker Hub images are no longer published, so the
  bucket server is the S3Proxy image; the ClickHouse disk config is standard
  S3 and can point at MinIO or real S3 unchanged.) Tick parts move after 7-30
  days, slow tables after 1 year, and existing TTL deletion still applies.
  `07_tiering.sql` applies the policy and TTL to existing tables.

The cluster schema is applied by the `clickhouse-schema` one-shot after all
four nodes are healthy; `make schema` reruns the same service. Refreshable MVs
cannot be created with `ON CLUSTER` on 24.8 (the DDL worker segfaults), so the
MV file is applied to each node separately.

The k8s branch still runs one ClickHouse and mounts these same SQL files, so it
must add Keeper + the cluster config (or keep its own schema) when the branches
merge. The k8s submit watcher also still submits one job; port the same group
loop when the branches merge.

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
3. `feat/clickhouse-scale`: read/retention, 2x2 cluster with Keeper +
   `Distributed`, materialized indicator snapshot, S3 tiering (all done
   here). A replicated Keeper and a load-balanced entry point remain.
4. `feat/observability-autoscaling`: Prometheus/alerts, Flink operator,
   autoscaler, savepoints, Kafka broker topology.
5. `test/scaling-harness` (or the first commit of the producers branch).

## Blockers and evidence

| Blocker | Where | Why it blocks |
|---|---|---|
| 3 partitions, 3 Flink parallelism, 4 slots | `init/kafka/create-topics.sh`, `k8s/config.sh`, `k8s/base/flink.yaml` | Source parallelism cannot exceed partitions; the values are hardcoded in three files. |
| Single ClickHouse Keeper / one entry node | `configs/clickhouse/keeper.xml`, `docker-compose.yml` | Replication and DDL stop if Keeper dies; Flink and Grafana still point at node 1. Data is replicated, the entry point is not. |
| Serial, unsharded producers | `producers/src/equities/yahoo_producer.py`, `producers/src/crypto/binance_ws_producer.py` | One pod per feed; full-day Yahoo fetch per poll; crypto list hardcoded. |
| Silent drops | `producers/src/base_producer.py`, `binance_ws_producer.py` | Queue full drops records; data loss at high rate. |
| Symbol key skew | `producers/src/base_producer.py`, `binance_ws_producer.py` | BTC dominates; one key cannot be split across partitions. |
| Read path window view | `init/clickhouse/04_indicators.sql` | `equities_indicators` still recomputes on every query; the 5-minute snapshot is the cheap path. |
| Full-history indicator refresh | `init/clickhouse/04_indicators.sql` | Each refresh rescans the shard's history; a real incremental aggregate (AggregatingMergeTree state) is still open. |
| Static Flink/no metrics | `k8s/base/flink.yaml`, `k8s/README.md` | Manual scaling, single JobManager, no lag/backpressure alerts. |

## Acceptance

- `make test` and `make lint` pass for each slice.
- A load harness reports rows/s, Kafka lag, Flink backpressure and ClickHouse
  p95 insert latency before and after.
- No change claims a scaling win without the harness numbers.
