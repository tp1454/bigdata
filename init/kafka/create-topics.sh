#!/usr/bin/env bash
set -euo pipefail

BROKER="${KAFKA_BROKER:-kafka:29092}"
PARTITIONS=3
TOPICS=(equities.prices crypto.trades crypto.market forex.rates macro.indicators fundamentals.filings)

until kafka-topics --bootstrap-server "$BROKER" --list >/dev/null 2>&1; do
  echo "Waiting for Kafka at $BROKER..."
  sleep 2
done

for topic in "${TOPICS[@]}"; do
  kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists \
    --topic "$topic" --partitions "$PARTITIONS" --replication-factor 1

  # Topics created by older versions have one partition; grow them in place.
  current=$(kafka-topics --bootstrap-server "$BROKER" --describe --topic "$topic" 2>/dev/null \
    | head -1 | grep -o 'PartitionCount: [0-9]*' | awk '{print $2}' || true)
  if [[ -n "$current" && "$current" -lt "$PARTITIONS" ]]; then
    kafka-topics --bootstrap-server "$BROKER" --alter --topic "$topic" --partitions "$PARTITIONS"
  fi
  echo "Topic $topic ready"
done

# High-frequency tick data: keep a bounded local window (7 days).
for topic in equities.prices crypto.trades crypto.market forex.rates; do
  kafka-configs --bootstrap-server "$BROKER" --alter --entity-type topics \
    --entity-name "$topic" --add-config retention.ms=604800000
done

# Filings are keyed snapshots: compact old versions, keep 30 days.
kafka-configs --bootstrap-server "$BROKER" --alter --entity-type topics \
  --entity-name fundamentals.filings --add-config 'cleanup.policy=compact,retention.ms=2592000000'

echo "Topic configuration applied"
