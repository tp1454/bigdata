.DEFAULT_GOAL := help
SHELL := /bin/bash

COMPOSE := docker compose
PYTEST := $(if $(wildcard .venv/bin/pytest),.venv/bin/pytest,python3 -m pytest)
RUFF := $(if $(wildcard .venv/bin/ruff),.venv/bin/ruff,ruff)

KAFKA_BROKERS := kafka-1:29092,kafka-2:29092,kafka-3:29092
KAFKA_TOPICS := equities.prices crypto.trades crypto.ohlcv crypto.market forex.rates macro.indicators fundamentals.filings fundamentals.actions fundamentals.statements fundamentals.insider options.metrics institutional.holdings reference.symbols
# table:time-column pairs for freshness checks
TABLES := equities_prices:ts crypto_trades:timestamp crypto_ohlcv:timestamp crypto_market_data:timestamp forex_rates:timestamp macro_indicators:timestamp company_filings:filing_date corporate_actions:action_date fundamentals:end_date insider_transactions:transaction_date options_metrics:timestamp institutional_holdings:period symbol_catalog:ingested_at pipeline_errors:ts

.PHONY: help setup dev up build deploy redeploy down logs ps shell topics feed \
        load test-topics check-data schema job-reset smoke test lint fmt check nuke url query

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
		$(COMPOSE) wait clickhouse-schema; \
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
	$(COMPOSE) exec -T kafka-1 bash /etc/kafka/create-topics.sh

feed: ## Tail Kafka messages (TOPIC=x for one topic)
	@$(COMPOSE) exec -T kafka-1 kafka-console-consumer --bootstrap-server $(KAFKA_BROKERS) $(if $(TOPIC),--topic $(TOPIC),--include '.*')

load: ## Synthetic Kafka load (RATE=500 DURATION=60 TOPIC=crypto.trades)
	$(COMPOSE) --profile load run --rm loadgen python -m src.loadgen \
		--rate $(or $(RATE),500) --duration $(or $(DURATION),60) \
		--topic $(or $(TOPIC),crypto.trades) \
		$(if $(SYMBOL),--symbol $(SYMBOL))

test-topics: ## Show topics, partitions and message counts
	@echo "=== Existing Topics ==="
	@$(COMPOSE) exec -T kafka-1 kafka-topics --bootstrap-server $(KAFKA_BROKERS) --list
	@echo ""
	@echo "=== Topic Stats (Partitions | Messages) ==="
	@for t in $(KAFKA_TOPICS); do \
		partitions=$$($(COMPOSE) exec -T kafka-1 kafka-topics --bootstrap-server $(KAFKA_BROKERS) --describe --topic $$t 2>/dev/null | head -1 | grep -o 'PartitionCount: [0-9]*' | awk '{print $$2}'); \
		messages=$$($(COMPOSE) exec -T kafka-1 kafka-get-offsets --bootstrap-server $(KAFKA_BROKERS) --topic $$t 2>/dev/null | awk -F: '{sum+=$$3} END{print sum+0}'); \
		printf "%-25s %3s partitions | %s messages\n" "$$t" "$$partitions" "$$messages"; \
	done

check-data: ## Row counts and latest timestamp per table
	@printf '%-22s %10s  %s\n' TABLE ROWS LATEST
	@for entry in $(TABLES); do \
		t=$${entry%%:*}; c=$${entry##*:}; \
		out=$$($(COMPOSE) exec -T clickhouse clickhouse-client --query "SELECT count(), toString(max($$c)) FROM analytics.$$t" 2>/dev/null | tr '\t' ' '); \
		printf '%-22s %s\n' "$$t" "$$out"; \
	done

schema: ## Apply schema, retention, tiering and the indicators MV
	@engine=$$($(COMPOSE) exec -T clickhouse clickhouse-client --query "SELECT engine FROM system.tables WHERE database='analytics' AND name='equities_prices'" 2>/dev/null || true); \
	if [ -n "$$engine" ] && [ "$$engine" != "Distributed" ]; then \
		echo "FAIL: analytics.equities_prices engine is $$engine; the cluster needs a fresh volume (make nuke)" >&2; exit 1; \
	fi
	@$(COMPOSE) wait clickhouse-schema 2>/dev/null || true
	$(COMPOSE) run --rm clickhouse-schema

job-reset: ## Cancel and resubmit the Flink jobs (after code or topic changes)
	@ids=""; \
	for _ in $$(seq 1 60); do \
		ids=$$($(COMPOSE) exec -T jobmanager flink list -m jobmanager:8081 2>/dev/null | grep 'Kafka_to_ClickHouse' | grep -oE '[0-9a-f]{32}' | tr '\n' ' '); \
		[ -n "$$ids" ] && break; \
		sleep 2; \
	done; \
	for id in $$ids; do \
		$(COMPOSE) exec -T jobmanager flink cancel -m jobmanager:8081 $$id; \
	done; \
	for _ in $$(seq 1 30); do \
		$(COMPOSE) exec -T jobmanager flink list -m jobmanager:8081 2>/dev/null | grep -q 'Kafka_to_ClickHouse' || break; \
		sleep 1; \
	done; \
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
	echo "Prometheus: http://localhost:$${PROMETHEUS_PORT:-9090}"; \
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
	docker run --rm -v "$(CURDIR)/volumes:/v" alpine sh -c 'rm -rf /v/* && mkdir -p /v/zookeeper-data /v/zookeeper-log /v/kafka-data-1 /v/kafka-data-2 /v/kafka-data-3 /v/grafana-data && chown -R 1000:1000 /v/zookeeper-data /v/zookeeper-log /v/kafka-data-1 /v/kafka-data-2 /v/kafka-data-3 && chown -R 472:472 /v/grafana-data'
	$(COMPOSE) up -d
	@echo "started; run 'make smoke' to verify"
