.DEFAULT_GOAL := help
SHELL := /bin/bash

COMPOSE := docker compose
PYTEST := $(if $(wildcard .venv/bin/pytest),.venv/bin/pytest,python3 -m pytest)
RUFF := $(if $(wildcard .venv/bin/ruff),.venv/bin/ruff,ruff)

KAFKA_TOPICS := equities.prices crypto.trades crypto.ohlcv crypto.market forex.rates macro.indicators fundamentals.filings fundamentals.actions fundamentals.statements fundamentals.insider
# table:time-column pairs for freshness checks
TABLES := equities_prices:ts crypto_trades:timestamp crypto_ohlcv:timestamp crypto_market_data:timestamp forex_rates:timestamp macro_indicators:timestamp company_filings:filing_date corporate_actions:action_date fundamentals:end_date insider_transactions:transaction_date pipeline_errors:ts

.PHONY: help setup dev up build deploy redeploy down logs ps shell topics feed \
        test-topics check-data schema job-reset smoke test lint fmt check nuke url query

help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Copy .env.example to .env (if missing) and build images
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example - set real passwords")
	$(COMPOSE) build

dev: ## Create .venv and install test/lint tooling
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements-dev.txt

up: ## Start the stack (BUILD=1 to rebuild images)
	@test -f .env || (echo "missing .env; run 'make setup'"; exit 1)
	$(COMPOSE) up -d $(if $(BUILD),--build)

deploy: ## Rebuild, start and verify the pipeline (SERVICE=x for one service)
	@test -f .env || (echo "missing .env; run 'make setup'"; exit 1)
	$(COMPOSE) up -d --build $(SERVICE)
	@if [ -z "$(SERVICE)" ]; then \
		set -e; \
		$(MAKE) topics; \
		$(MAKE) schema; \
		$(MAKE) job-reset; \
		$(MAKE) smoke; \
	fi

redeploy: ## Recreate containers with current config, no image rebuild
	@test -f .env || (echo "missing .env; run 'make setup'"; exit 1)
	$(COMPOSE) up -d --force-recreate $(SERVICE)

down: ## Stop the stack (keeps data)
	$(COMPOSE) down

logs: ## Tail logs (SERVICE=x for one service)
	$(COMPOSE) logs -f $(SERVICE)

ps: ## Show container status
	$(COMPOSE) ps

shell: ## Open a shell (SERVICE=x)
	@test -n "$(SERVICE)" || (echo "usage: make shell SERVICE=<service>"; exit 1)
	$(COMPOSE) exec $(SERVICE) bash

topics: ## Re-run topic creation/configuration
	$(COMPOSE) exec -T kafka bash /etc/kafka/create-topics.sh

feed: ## Tail Kafka messages (TOPIC=x for one topic)
	@$(COMPOSE) exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 $(if $(TOPIC),--topic $(TOPIC),--include '.*')

test-topics: ## Show topics, partitions and message counts
	@echo "=== Existing Topics ==="
	@$(COMPOSE) exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list
	@echo ""
	@echo "=== Topic Stats (Partitions | Messages) ==="
	@for t in $(KAFKA_TOPICS); do \
		partitions=$$($(COMPOSE) exec -T kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic $$t 2>/dev/null | head -1 | grep -o 'PartitionCount: [0-9]*' | awk '{print $$2}'); \
		messages=$$($(COMPOSE) exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic $$t 2>/dev/null | awk -F: '{sum+=$$3} END{print sum+0}'); \
		printf "%-25s %3s partitions | %s messages\n" "$$t" "$$partitions" "$$messages"; \
	done

check-data: ## Row counts and latest timestamp per table
	@printf '%-22s %10s  %s\n' TABLE ROWS LATEST
	@for entry in $(TABLES); do \
		t=$${entry%%:*}; c=$${entry##*:}; \
		out=$$($(COMPOSE) exec -T clickhouse clickhouse-client --query "SELECT count(), toString(max($$c)) FROM analytics.$$t" 2>/dev/null | tr '\t' ' '); \
		printf '%-22s %s\n' "$$t" "$$out"; \
	done

schema: ## Apply ClickHouse schema and views (idempotent)
	@for _ in $$(seq 1 60); do \
		$(COMPOSE) exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1 && break; \
		sleep 2; \
	done; \
	engine=$$($(COMPOSE) exec -T clickhouse clickhouse-client --query "SELECT engine FROM system.tables WHERE database='analytics' AND name='equities_prices'" 2>/dev/null); \
	if [ -n "$$engine" ] && [ "$$engine" != "ReplacingMergeTree" ]; then \
		echo "FAIL: analytics.equities_prices engine is $$engine; run 'make nuke' or migrate" >&2; \
		exit 1; \
	fi; \
	for f in init/clickhouse/0[2-9]_*.sql; do \
		$(COMPOSE) exec -T clickhouse clickhouse-client < $$f || exit 1; \
	done

job-reset: ## Cancel and resubmit the Flink job (after code or topic changes)
	@job=""; \
	for _ in $$(seq 1 60); do \
		job=$$($(COMPOSE) exec -T jobmanager flink list -r -m jobmanager:8081 2>/dev/null | grep -oE '[0-9a-f]{32}' | head -1); \
		[ -n "$$job" ] && break; \
		sleep 2; \
	done; \
	if [ -n "$$job" ]; then \
		$(COMPOSE) exec -T jobmanager flink cancel -m jobmanager:8081 $$job; \
		for _ in $$(seq 1 30); do \
			$(COMPOSE) exec -T jobmanager flink list -r -m jobmanager:8081 2>/dev/null | grep -q "$$job" || break; \
			sleep 1; \
		done; \
	fi; \
	$(COMPOSE) run --rm flink-submit

query: ## Run a ClickHouse query (SQL='SELECT ...')
	@test -n "$(SQL)" || (echo "usage: make query SQL='SELECT ...'"; exit 1)
	@$(COMPOSE) exec -T clickhouse clickhouse-client --query "$(SQL)"

smoke: ## Verify the running stack end to end
	./scripts/smoke.sh

url: ## Print service URLs
	@if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	echo "Grafana:    http://localhost:$${GRAFANA_PORT:-3000}"; \
	echo "Flink UI:   http://localhost:8081"; \
	echo "ClickHouse: http://localhost:$${CLICKHOUSE_HTTP_PORT:-8123} (user $${CLICKHOUSE_USER:-default})"; \
	echo "Kafka:      localhost:$${KAFKA_PORT:-29092}"

test: ## Run unit tests
	$(PYTEST)

lint: ## Lint Python
	$(RUFF) check producers flink-jobs tests

fmt: ## Format Python
	$(RUFF) format producers flink-jobs tests

check: lint test ## Lint, unit tests and compose validation
	@if [ -f .env ]; then $(COMPOSE) config -q && echo "ok: compose config"; else echo "skip: compose config (no .env)"; fi

nuke: ## DANGER: wipe all data and restart
	$(COMPOSE) down -v
	docker run --rm -v "$(CURDIR)/volumes:/v" alpine sh -c 'rm -rf /v/* && mkdir -p /v/zookeeper-data /v/zookeeper-log /v/kafka-data /v/grafana-data && chown -R 1000:1000 /v/zookeeper-data /v/zookeeper-log /v/kafka-data && chown -R 472:472 /v/grafana-data'
	$(COMPOSE) up -d
	@echo "started; run 'make smoke' to verify"
