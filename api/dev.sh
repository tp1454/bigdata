#!/usr/bin/env bash
# Create, start and check the API for development.
#
#   ./api/dev.sh up       build + start clickhouse/api/mcp, seed demo rows, run checks
#   ./api/dev.sh check    run the HTTP/MCP checks against a running stack
#   ./api/dev.sh seed     insert demo rows for symbols that have none
#   ./api/dev.sh reload   restart the MCP bridge after API route changes
#   ./api/dev.sh down     stop api, mcp and clickhouse (keeps data)
#   ./api/dev.sh logs     follow api and mcp logs
#   ./api/dev.sh sql "SELECT ..."   run a ClickHouse query
#   ./api/dev.sh test     run the unit tests (no Docker needed)
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose"
API_URL="${API_URL:-http://localhost:4000}"
MCP_URL="${MCP_URL:-http://localhost:3001}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-clickhouse}"

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
  CH_PASSWORD="${CLICKHOUSE_PASSWORD:-clickhouse}"
fi

fail() { echo "FAIL: $*" >&2; exit 1; }

ch() {
  $COMPOSE exec -T clickhouse clickhouse-client --password "$CH_PASSWORD" --query "$1"
}

wait_http() {
  local name=$1 url=$2
  for _ in $(seq 1 90); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$url" 2>/dev/null || true)
    if [ -n "$code" ] && [ "$code" != "000" ]; then
      echo "ok: $name is answering"
      return
    fi
    sleep 2
  done
  fail "timed out waiting for $name at $url"
}

seed_rows() {
  local table=$1 column=$2 value=$3 sql=$4
  local count
  count=$(ch "SELECT count() FROM analytics.$table WHERE $column = '$value'")
  if [ "$count" = "0" ]; then
    ch "$sql"
    echo "seeded $table ($value)"
  else
    echo "skip $table ($value): $count rows"
  fi
}

seed() {
  seed_rows equities_prices symbol AAPL \
    "INSERT INTO analytics.equities_prices (symbol, ts, open, high, low, close, volume) VALUES ('AAPL', now64(3), 190.0, 191.5, 189.2, 190.8, 1200), ('AAPL', now64(3) - INTERVAL 1 MINUTE, 189.8, 190.4, 189.0, 190.0, 800)"
  seed_rows crypto_trades symbol BTCUSDT \
    "INSERT INTO analytics.crypto_trades (symbol, timestamp, price, quantity, trade_id, source) VALUES ('BTCUSDT', now64(3), 68000.5, 0.01, 1001, 'binance')"
  seed_rows forex_rates pair EURUSD \
    "INSERT INTO analytics.forex_rates (pair, timestamp, rate, source) VALUES ('EURUSD', now64(3), 1.0876, 'frankfurter')"
}

check() {
  local body status

  body=$(curl -sf "$API_URL/api/crypto/latest?symbol=BTCUSDT") || fail "crypto latest"
  echo "$body" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["data"], d; print("ok: crypto latest")'

  body=$(curl -sf "$API_URL/api/equities/daily?symbol=AAPL&limit=2") || fail "equities daily"
  echo "$body" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["data"], d; print("ok: equities daily")'

  body=$(curl -sf "$API_URL/api/forex/latest?pair=EURUSD") || fail "forex latest"
  echo "$body" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["data"], d; print("ok: forex latest")'

  status=$(curl -s -o /dev/null -w '%{http_code}' "$API_URL/api/crypto/latest?symbol=")
  [ "$status" = "400" ] || fail "expected 400 for empty symbol, got $status"
  echo "ok: validation -> 400"

  status=$(curl -s -o /dev/null -w '%{http_code}' "$API_URL/nope")
  [ "$status" = "404" ] || fail "expected 404 for unknown path, got $status"
  echo "ok: unknown path -> 404"

  local sid tools
  sid=$(curl -s -D - -o /dev/null -X POST "$MCP_URL/mcp" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json, text/event-stream' \
    --data '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"dev-sh","version":"0"}}}' \
    | awk -F': ' 'tolower($1)=="mcp-session-id"{print $2}' | tr -d '\r')
  [ -n "$sid" ] || fail "MCP initialize returned no session id"

  curl -s -X POST "$MCP_URL/mcp" -H "Mcp-Session-Id: $sid" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json, text/event-stream' \
    --data '{"jsonrpc":"2.0","method":"notifications/initialized"}' -o /dev/null

  tools=$(curl -s -X POST "$MCP_URL/mcp" -H "Mcp-Session-Id: $sid" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json, text/event-stream' \
    --data '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
    | python3 -c 'import json,sys; print(",".join(t["name"] for t in json.load(sys.stdin)["result"]["tools"]))')

  case ",$tools," in
    *",crypto-latest,"*",equities-daily,"*) ;;
    *) fail "unexpected MCP tools: $tools" ;;
  esac
  echo "ok: MCP tools: $tools"
}

case "${1:-up}" in
  up)
    $COMPOSE up -d --build clickhouse api mcp
    wait_http api "$API_URL/health"
    wait_http mcp "$MCP_URL/mcp"
    seed
    check
    echo "API:  $API_URL (docs: $API_URL/openapi)"
    echo "MCP:  $MCP_URL/mcp"
    ;;
  check)
    wait_http api "$API_URL/health"
    check
    ;;
  seed)
    seed
    ;;
  reload)
    $COMPOSE restart api mcp
    wait_http api "$API_URL/health"
    wait_http mcp "$MCP_URL/mcp"
    echo "reloaded api and mcp"
    ;;
  down)
    $COMPOSE stop api mcp clickhouse
    ;;
  logs)
    $COMPOSE logs -f api mcp
    ;;
  sql)
    shift
    [ $# -gt 0 ] || fail 'usage: ./api/dev.sh sql "SELECT ..."'
    ch "$*"
    ;;
  test)
    (cd api && bun test)
    ;;
  help | -h | --help)
    sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
    ;;
  *)
    fail "unknown command: $1 (try: up, check, seed, reload, down, logs, sql, test)"
    ;;
esac
