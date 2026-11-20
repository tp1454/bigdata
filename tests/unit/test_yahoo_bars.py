import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "producers" / "src"))

from equities.yahoo_bars import (
    bar_volume,
    history_kwargs,
    next_sleep,
    should_publish,
)


def test_next_sleep_uses_the_idle_backoff():
    assert next_sleep(True, 60, 300) == 60
    assert next_sleep(False, 60, 300) == 300


def test_history_kwargs_uses_a_small_window_after_the_first_poll():
    assert history_kwargs(None) == {"period": "1d"}
    last = datetime(2026, 1, 1, 10, 0)
    assert history_kwargs(last) == {"start": last - timedelta(minutes=1)}


def test_should_publish_republishes_the_last_bar_and_newer():
    older = datetime(2026, 1, 1, 9, 30)
    last = datetime(2026, 1, 1, 10, 0)
    newest = datetime(2026, 1, 1, 10, 1)
    assert not should_publish(older, last)
    # The completed bar is re-sent so its final OHLC/volume replace the partial values.
    assert should_publish(last, last)
    assert should_publish(newest, last)
    assert should_publish(older, None)


def test_bar_volume_turns_nan_into_zero():
    assert bar_volume(float("nan")) == 0
    assert bar_volume(12.0) == 12
