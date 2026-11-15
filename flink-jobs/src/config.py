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
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "flink_consumer_group")

# Same order as init/kafka/create-topics.sh
KAFKA_TOPICS = [
    "equities.prices",
    "crypto.trades",
    "crypto.market",
    "forex.rates",
    "macro.indicators",
    "fundamentals.filings",
]

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
