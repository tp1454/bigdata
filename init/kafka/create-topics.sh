#!/usr/bin/env bash
set -euo pipefail

BROKER="${KAFKA_BROKER:-kafka-1:29092,kafka-2:29092,kafka-3:29092}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
REPLICATION_FACTOR=3
TOPICS=(equities.prices crypto.trades crypto.ohlcv crypto.market forex.rates macro.indicators fundamentals.filings fundamentals.actions fundamentals.statements fundamentals.insider)

until kafka-topics --bootstrap-server "$BROKER" --list >/dev/null 2>&1; do
  echo "Waiting for Kafka at $BROKER..."
  sleep 2
done

for topic in "${TOPICS[@]}"; do
  kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists \
    --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$REPLICATION_FACTOR"

  # Existing topics keep their replication factor; a live RF1 cluster
  # needs kafka-reassign-partitions or a fresh Kafka/Zookeeper volume.
  # Older topics may also have one partition; grow them in place.
  current=$(kafka-topics --bootstrap-server "$BROKER" --describe --topic "$topic" 2>/dev/null \
    | head -1 | grep -o 'PartitionCount: [0-9]*' | awk '{print $2}' || true)
  if [[ -n "$current" && "$current" -lt "$PARTITIONS" ]]; then
    kafka-topics --bootstrap-server "$BROKER" --alter --topic "$topic" --partitions "$PARTITIONS"
  fi
  echo "Topic $topic ready"
done

# High-frequency tick data: keep a bounded local window (7 days).
for topic in equities.prices crypto.trades crypto.ohlcv crypto.market forex.rates; do
  kafka-configs --bootstrap-server "$BROKER" --alter --entity-type topics \
    --entity-name "$topic" --add-config retention.ms=604800000,min.insync.replicas=2
done

# Filings are keyed snapshots: compact old versions, keep 30 days.
kafka-configs --bootstrap-server "$BROKER" --alter --entity-type topics \
  --entity-name fundamentals.filings --add-config 'cleanup.policy=compact,retention.ms=2592000000,min.insync.replicas=2'

# Corporate actions, statements and insider rows are keyed events, not
# snapshots: keep 30 days so a replay can rebuild recent history.
for topic in fundamentals.actions fundamentals.statements fundamentals.insider; do
  kafka-configs --bootstrap-server "$BROKER" --alter --entity-type topics \
    --entity-name "$topic" --add-config retention.ms=2592000000,min.insync.replicas=2
done

echo "Topic configuration applied"
