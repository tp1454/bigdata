"""Runtime configuration. Everything comes from the environment."""

import os


def _required(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is required (set it in .env / compose)")
    return value


# ==========================================
# KAFKA
# ==========================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-1:29092,kafka-2:29092,kafka-3:29092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "flink_consumer_group")

# Same order as init/kafka/create-topics.sh
KAFKA_TOPICS = [
    "equities.prices",
    "crypto.trades",
    "crypto.ohlcv",
    "crypto.market",
    "forex.rates",
    "macro.indicators",
    "fundamentals.filings",
    "fundamentals.actions",
    "fundamentals.statements",
    "fundamentals.insider",
]

TOPIC_GROUPS = {
    "equities": ["equities.prices"],
    "crypto": ["crypto.trades", "crypto.ohlcv", "crypto.market"],
    "fundamentals": [
        "fundamentals.filings",
        "fundamentals.actions",
        "fundamentals.statements",
        "fundamentals.insider",
    ],
    "macro": ["forex.rates", "macro.indicators"],
}


def _group():
    group = os.getenv("FLINK_GROUP", "").strip()
    if group and group not in TOPIC_GROUPS:
        raise RuntimeError(f"unknown FLINK_GROUP {group!r}")
    return group


def topics_for_group():
    group = _group()
    return list(TOPIC_GROUPS[group]) if group else list(KAFKA_TOPICS)


def job_name():
    group = _group()
    if not group:
        return "Kafka_to_ClickHouse_Pipeline"
    return f"Kafka_to_ClickHouse_{group}"


def job_group_id():
    group = _group()
    return f"{KAFKA_GROUP_ID}_{group}" if group else KAFKA_GROUP_ID

# ==========================================
# CLICKHOUSE
# ==========================================
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = _required("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "analytics")

# ==========================================
# PIPELINE
# ==========================================
CHECKPOINT_INTERVAL_MS = int(os.getenv("CHECKPOINT_INTERVAL_MS", "60000"))


def parallelism():
    value = int(os.getenv("FLINK_PARALLELISM", "1"))
    if value < 1:
        raise RuntimeError("FLINK_PARALLELISM must be at least 1")
    return value
