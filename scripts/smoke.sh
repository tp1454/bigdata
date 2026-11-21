#!/usr/bin/env bash
# End-to-end check: the stack is up, the Flink job runs, and rows arrive.
set -euo pipefail
cd "$(dirname "$0")/.."

TOPICS="equities.prices crypto.trades crypto.ohlcv crypto.market forex.rates macro.indicators fundamentals.filings fundamentals.actions fundamentals.statements fundamentals.insider"
TABLES="equities_prices crypto_trades crypto_ohlcv crypto_market_data forex_rates macro_indicators company_filings corporate_actions fundamentals insider_transactions pipeline_errors"
DEADLINE_SECONDS=180

wait_until() {
    local what="$1"; shift
    local deadline=$((SECONDS + DEADLINE_SECONDS))
    until "$@" >/dev/null 2>&1; do
        if (( SECONDS >= deadline )); then
            echo "FAIL: timed out waiting for $what" >&2
            docker compose ps >&2
            exit 1
        fi
        sleep 5
    done
    echo "ok: $what"
}

ch_query() {
    docker compose exec -T clickhouse clickhouse-client --query "$1" | tr -d '\r'
}

count() {
    ch_query "SELECT count() FROM analytics.$1"
}

wait_until "kafka" docker compose exec -T kafka-1 \
    kafka-broker-api-versions --bootstrap-server localhost:29092
wait_until "clickhouse" docker compose exec -T clickhouse \
    wget -q --spider http://localhost:8123/ping
wait_until "4 flink jobs RUNNING" bash -c '
    for g in equities crypto fundamentals macro; do
        docker compose exec -T jobmanager flink list -r 2>/dev/null \
            | grep -q "Kafka_to_ClickHouse_$g" || exit 1
    done'

topics=$(docker compose exec -T kafka-1 kafka-topics --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --list | tr -d '\r')
for topic in $TOPICS; do
    if ! grep -qx "$topic" <<<"$topics"; then
        echo "FAIL: missing topic $topic" >&2
        exit 1
    fi
done
echo "ok: ${TOPICS// /, }"

cluster_hosts=$(ch_query "SELECT count() FROM system.clusters WHERE cluster = 'analytics_cluster'")
if (( cluster_hosts < 4 )); then
    echo "FAIL: analytics_cluster has $cluster_hosts hosts, expected 4" >&2
    exit 1
fi
echo "ok: clickhouse cluster ($cluster_hosts hosts)"

for table in $TABLES; do
    if ! ch_query "SELECT count() FROM analytics.$table" >/dev/null 2>&1; then
        echo "FAIL: missing or unreadable table analytics.$table" >&2
        exit 1
    fi
done

total_rows() {
    local sum=0
    for table in $TABLES; do
        [ "$table" = "pipeline_errors" ] && continue
        sum=$((sum + $(count "$table")))
    done
    echo "$sum"
}

before=$(total_rows)
echo "waiting 30s for new rows..."
sleep 30
after=$(total_rows)
if (( after <= before )); then
    echo "FAIL: no new rows in 30s (all tables)" >&2
    exit 1
fi
echo "ok: $before -> $after rows"
for table in $TABLES; do
    printf '  %-22s %12s rows\n' "$table" "$(count "$table")"
done
echo "smoke: PASS"
