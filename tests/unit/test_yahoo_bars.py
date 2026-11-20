import sys
from datetime import datetime
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
    assert history_kwargs(last) == {"start": last}


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
