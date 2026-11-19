import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "producers" / "src"))

from equities.yahoo_bars import bar_volume, should_publish


def test_should_publish_skips_old_bars_but_keeps_newest():
    older = datetime(2026, 1, 1, 9, 30)
    last = datetime(2026, 1, 1, 10, 0)
    newest = datetime(2026, 1, 1, 10, 1)
    assert not should_publish(older, last, newest)
    assert should_publish(last, last, last)
    assert should_publish(newest, last, newest)
    assert should_publish(older, None, newest)


def test_bar_volume_turns_nan_into_zero():
    assert bar_volume(float("nan")) == 0
    assert bar_volume(12.0) == 12
