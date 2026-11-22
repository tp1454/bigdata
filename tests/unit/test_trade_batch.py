import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "producers" / "src"))

from crypto.trade_batch import FLUSH_SECONDS, MAX_BATCH_ROWS, flush_due, grouped_trades


def test_flush_due_on_size_or_time():
    assert not flush_due(1, 0.1)
    assert flush_due(MAX_BATCH_ROWS, 0.1)
    assert flush_due(1, FLUSH_SECONDS)


def test_grouped_trades_keeps_each_symbol_under_its_key():
    batch = [
        {"s": "BTCUSDT", "T": 1, "p": "1", "q": "2", "a": 11},
        {"s": "ETHUSDT", "T": 2, "p": "3", "q": "4", "a": 12},
        {"s": "BTCUSDT", "T": 3, "p": "5", "q": "6", "a": 13},
    ]
    groups = grouped_trades(batch)
    assert set(groups) == {"BTCUSDT", "ETHUSDT"}
    assert [t["trade_id"] for t in groups["BTCUSDT"]] == [11, 13]
    assert groups["ETHUSDT"][0]["trade_id"] == 12
