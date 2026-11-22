# Java ClickHouse sink — decision, design, measurements

Status: implemented on `feat/java-crypto-sink`. All four Flink groups
(`equities`, `crypto`, `fundamentals`, `macro`) are submitted as the Java
DataStream job. The Python PyFlink job is kept in the tree for reference but is
no longer submitted.

## Why this change

The Python job's `ClickHouseSink` is a `MapFunction`: one synchronous
`client.insert(...)` per Kafka record. That is correct (the write completes
before the checkpoint barrier) but it caps throughput at inserts/s, and PyFlink
1.18 gives a Python operator no checkpoint hook to flush a buffer safely.

A Python buffer would be worse than slow: records in Python memory are not in
the checkpoint snapshot, while the Kafka source offsets are. If the barrier
passes with rows still buffered, the checkpoint completes, the offsets commit,
and a failure loses those rows. Options considered:

| Option | Result |
|---|---|
| Per-record insert (Python, current) | Correct, slow; one insert per message |
| Python buffer + timer | Loses the buffer between checkpoint and flush |
| `wait_for_async_insert=0` | ClickHouse buffers; a ClickHouse crash loses the buffer after Flink committed |
| Java `CheckpointedFunction` | Buffer flushes in `snapshotState()`, then the checkpoint completes — safe |

A plain Python consumer outside Flink could batch and commit offsets after the
insert, but inside Flink the safe hook is Java. The Java sink gives both:
checkpoint-safe at-least-once and large batches.

Producer batching already existed for crypto (200 trades / 1 s message) and for
multi-row messages (statement line items). It does not help single-row topics
(equities, forex, macro, actions, insider, SEC filings-per-filing).

## What was built

- `flink-jobs/java/src/main/java/com/example/`
  - `KafkaToClickHouse` — builds one Kafka source per topic, unions the parsed
    rows, attaches the sink; `FLINK_GROUP` selects the group.
  - `Groups` — the four topic groups; job name and consumer group follow the
    Python convention (`Kafka_to_ClickHouse_<group>`, `flink_consumer_group_<group>`).
  - `MessageParser` — all 10 topics; filings expand one row per filing,
    statements expand line items, insider keeps nullable `value`, parse errors
    go to `pipeline_errors`.
  - `ClickHouseSink` — buffers up to **2,000 rows or 1 s**, one JDBC batch per
    table, flushes in `snapshotState()`, 11 target tables.
  - `ClickHouseRow` — `(table, values...)`.
  - `src/test/.../MessageParserTest` — fixtures for every topic and the error
    path; the tests run in the image build.
- `flink-jobs/Dockerfile` — multi-stage Maven build; `flink-sink.jar` in
  `/opt/flink/usrlib`.
- `docker-compose.yml` — `flink-submit` submits the Java jar for each group
  from the existing `TOPIC_GROUPS` list; the job names and Grafana/Prometheus
  labels are unchanged. TaskManager: **6g process / 1g task off-heap**.
- `SEC_CIKS` support for the producer (used by the stress test) and the
  equity/CIK scale knobs are not part of the Java commit; see the stress notes.

Rollout fixes found on the way: the connector must be the distribution's
shaded Kafka connector (`OffsetResetStrategy`), Jackson `core`/`databind` must
both be 2.17.2, `httpclient5` is required by `clickhouse-jdbc`, the insider
parser must accept empty `transaction`/`ownership` like Python, `transaction`
must be backtick-quoted for the JDBC parser, and the batch must stay at 2,000
because a 10,000-row batch hit ClickHouse's 2.7 GiB query limit.

## Measured results

Conditions: single host, 4 Flink jobs at parallelism 3 (12 slots), 2 shards ×
2 replicas ClickHouse (3 GiB each), TaskManager 6 GiB, batch 2,000/1 s,
checkpoint 60 s / timeout 30 min. The stress feed was 51 crypto symbols,
30 equity symbols, 10 SEC CIKs. Synthetic loads: 600,000 crypto trades
(10,000/s for 60 s, 200-trade messages) and 12,000 equities messages
(200/s for 60 s, one row each). These are demo measurements, not a benchmark.

**Python sink (no sink-level batching, `integration/scaling`), after the load**

| Group | Lag | Observed rate |
|---|---|---|
| crypto | 4,298 messages | 8.6 msg/s, 1000 ms/s backpressure, ~0.9–1.7k rows/s |
| equities | 23,470 messages (incl. the 11.7k Yahoo burst) | 13.3 rows/s |
| fundamentals | no committed group to report | 22.2 msg/s, 1000 ms/s backpressure |
| macro | 4 messages | low-rate |

ClickHouse inserts in that mode: ~111 ms average per insert, one insert per
message. The equities and crypto backlogs did not drain under the load, and the
crypto/fundamentals checkpoints could not complete.

**Java sink (batching), after replaying the same backlogs**

| Group | Lag after replay | Notes |
|---|---|---|
| crypto | 33 (later ~51 with live input) | 8.9k rows/s during the 600k replay; 2,836 rows/insert; ~13 ms/insert |
| equities | 4 | 23k backlog drained in minutes |
| fundamentals | 0 | 51,332 statement rows written; filings/actions/insider also populated |
| macro | 0–8 | forex 4 |

Target tables after the migration: equities 33,499; crypto_trades 1,757,074;
forex 1,357; macro 157; filings 989; actions 1,055; fundamentals 51,332;
insider 3,376. `pipeline_errors`: 1,909 historical insider rows from before
the parity fix; no new errors after the fixed jobs started.

Current steady state (quiet feed): ~6 rows/s, ~6 ms per insert, 2,662 inserts
in 10 minutes for 15,895 rows — the 1 s flush keeps batches small when the
feed is slow. The 2,000-row batch only fills under load.

Lag is a **committed-offset** metric and moves in steps at checkpoints. The
Java sink flushes at the barrier, so checkpoints complete under load; the
Python job's per-message inserts stalled the barrier and the lag stayed.

## Tuning and operation

| Knob | Default | Effect |
|---|---|---|
| `CLICKHOUSE_BATCH_SIZE` | 2000 | rows per batch; raise for throughput, lower if ClickHouse hits its memory limit |
| `CLICKHOUSE_FLUSH_MS` | 1000 | maximum time a row waits in the sink |
| `CHECKPOINT_INTERVAL_MS` | 60000 | offset commit cadence / at-least-once replay window |
| `CHECKPOINT_TIMEOUT_MS` | 1800000 | must survive the first big backlog on a cold start |
| `FLINK_PARALLELISM` | 3 | bounded by the topic partitions and key skew |
| `TASKMANAGER_SLOTS` | 12 | 4 groups × parallelism |
| `taskmanager.memory.process.size` | 6g | Python/Beam and Kafka clients need direct memory |
| `taskmanager.memory.task.off-heap.size` | 1g | JVM direct limit; too low → `Direct buffer memory` |
| ClickHouse `mem_limit` | 3g | the server query limit is ~2.7 GiB; the batch must stay below it |

Verify with: per-table row counts, `kafka-consumer-groups` lag (checkpoint
steps), `flink_taskmanager_job_task_backPressuredTimeMsPerSecond`, ClickHouse
`system.query_log` insert duration, and `pipeline_errors`. The Java parser tests
run in `docker compose build`.

## Known limits / follow-ups

- **At-least-once, not exactly-once.** Duplicates on replay are absorbed by
  `ReplacingMergeTree`; `pipeline_errors` is a plain MergeTree and can duplicate.
- **Checkpoint state still lives in JobManager memory** (the existing
  `shortcut:`). A JobManager restart loses checkpoints; add a durable state
  backend before relying on recovery.
- **The ClickHouse password is serialized into the job graph** (passed to the
  sink constructor). The TaskManager already has `CLICKHOUSE_PASSWORD`; read it
  in `open()` to remove the secret from the serialized job.
- **No reconnect.** One JDBC connection per subtask; a dropped connection fails
  the task and replays.
- **Batch/CH memory coupling.** Raising the batch above ~2,000 on 3 GiB nodes
  can hit `MEMORY_LIMIT_EXCEEDED`; scale ClickHouse memory first.
- **1 s flush latency** for low-rate topics; acceptable for this pipeline.
- **Cleanup pending:** the Python job, transforms, tests and the
  `apache-flink`/`clickhouse-connect` requirements are still in the image and
  the group list is still read from `config.py`; `Groups.java` duplicates it.
  Remove them after the Java path has run for a while.

## Rollback

The last Python-only state is `integration/scaling`. Revert the Java submit
lines in `flink-submit` (restore the Python job and `--pyFiles`) and rebuild;
job names, consumer groups and tables are unchanged, so no data migration is
needed on rollback.
