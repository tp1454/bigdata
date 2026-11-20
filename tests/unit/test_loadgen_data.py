import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "producers" / "src"))

from loadgen_data import equity_row, event_time, event_time_ms, trade_batch


def test_trade_batch_has_unique_ids():
    batch = trade_batch("BTCUSDT", 10, 3, 1000, 2.0)
    assert batch["source"] == "binance"
    assert [t["trade_id"] for t in batch["trades"]] == [10, 11, 12]
    assert all(t["symbol"] == "BTCUSDT" for t in batch["trades"])


def test_equity_row_matches_the_transform_shape():
    row = equity_row("AAPL", datetime(2026, 1, 1), 10.0)
    assert row["symbol"] == "AAPL"
    assert row["ts"] == "2026-01-01T00:00:00"
    assert row["high"] > row["close"] > row["low"]
    assert row["volume"] == 100


def test_event_times_follow_the_target_rate():
    base = datetime(2026, 1, 1)
    assert event_time(base, 500, 500) == base + timedelta(seconds=1)
    assert event_time_ms(1000, 500, 500) == 2000
