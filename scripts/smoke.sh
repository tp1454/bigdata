#!/usr/bin/env bash
# End-to-end check: the stack is up, the Flink job runs, and rows arrive.
set -euo pipefail
cd "$(dirname "$0")/.."

TOPICS="equities.prices crypto.trades crypto.market forex.rates macro.indicators fundamentals.filings"
TABLES="equities_prices crypto_trades crypto_market_data forex_rates macro_indicators company_filings pipeline_errors"
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

wait_until "kafka" docker compose exec -T kafka \
    kafka-broker-api-versions --bootstrap-server localhost:29092
wait_until "clickhouse" docker compose exec -T clickhouse \
    wget -q --spider http://localhost:8123/ping
wait_until "flink job RUNNING" bash -c \
    'docker compose exec -T jobmanager flink list -r 2>/dev/null | grep -q RUNNING'

topics=$(docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list | tr -d '\r')
for topic in $TOPICS; do
    if ! grep -qx "$topic" <<<"$topics"; then
        echo "FAIL: missing topic $topic" >&2
        exit 1
    fi
done
echo "ok: ${TOPICS// /, }"

before=$(( $(count crypto_trades) + $(count equities_prices) ))
echo "waiting 30s for new rows..."
sleep 30
after=$(( $(count crypto_trades) + $(count equities_prices) ))
if (( after <= before )); then
    echo "FAIL: no new rows in 30s (crypto_trades + equities_prices)" >&2
    exit 1
fi
echo "ok: $before -> $after rows"

for table in $TABLES; do
    printf '  %-22s %12s rows\n' "$table" "$(count "$table")"
done
echo "smoke: PASS"
