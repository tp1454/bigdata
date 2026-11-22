"""Turn raw Binance trade messages into per-symbol batches."""

# One message per symbol per second; the row cap keeps it under Kafka's
# default 1 MiB message size.
# shortcut: ~8k trades per message, raise this if the broker limit changes.
MAX_BATCH_ROWS = 8000
FLUSH_SECONDS = 1.0


def flush_due(size, elapsed):
    return size >= MAX_BATCH_ROWS or elapsed >= FLUSH_SECONDS


def trade_row(msg):
    return {
        "symbol": msg["s"],
        "timestamp": msg["T"],
        "price": float(msg["p"]),
        "quantity": float(msg["q"]),
        "trade_id": msg["a"],
    }


def grouped_trades(batch):
    """Group raw trades by symbol so each Kafka key stays a symbol."""
    groups = {}
    for msg in batch:
        groups.setdefault(msg["s"], []).append(trade_row(msg))
    return groups
