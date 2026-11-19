"""Turn raw Binance trade messages into per-symbol batches."""


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
