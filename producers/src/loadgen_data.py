"""Pure synthetic payloads for the load harness."""

from datetime import timedelta


def event_time(base_ts, sent, rate):
    """Return the wall-clock time for the sent-th record at the target rate."""
    return base_ts + timedelta(seconds=sent / rate)


def event_time_ms(start_ms, sent, rate):
    """Return the epoch-ms time for the sent-th trade batch at the target rate."""
    return start_ms + int(sent * 1000 / rate)


def equity_row(symbol, ts, price, volume=100):
    return {
        "symbol": symbol,
        "ts": ts.isoformat(),
        "open": price,
        "high": price * 1.001,
        "low": price * 0.999,
        "close": price,
        "volume": volume,
    }


def trade_batch(symbol, first_id, count, ts_ms, price):
    return {
        "source": "binance",
        "trades": [
            {
                "symbol": symbol,
                "timestamp": ts_ms + i,
                "price": price,
                "quantity": 0.01,
                "trade_id": first_id + i,
            }
            for i in range(count)
        ],
    }
