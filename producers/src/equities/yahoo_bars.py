"""Pure helpers for the Yahoo 1m bar producer."""

from datetime import timedelta


def should_publish(ts, last):
    """Publish the last bar again plus anything newer.

    Re-sending `last` lets a bar published while still forming be corrected
    once it is complete.
    """
    return last is None or ts >= last


def history_kwargs(last):
    """Fetch one day first, then from one bar before `last`.

    The extra bar guarantees `last` is in the window so it can be corrected,
    even if Yahoo treats `start` as exclusive.
    """
    if last is None:
        return {"period": "1d"}
    return {"start": last - timedelta(minutes=1)}


def next_sleep(got_data, interval, backoff):
    """Wait for the poll interval after data, otherwise for the idle backoff."""
    return interval if got_data else backoff


def bar_volume(value):
    """Yahoo uses NaN for missing volume; store 0 instead."""
    return 0 if value != value else int(value)
