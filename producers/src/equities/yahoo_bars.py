"""Pure helpers for the Yahoo 1m bar producer."""


def should_publish(ts, last, newest):
    """Publish new bars, plus the newest bar because it keeps moving."""
    return last is None or ts > last or ts == newest


def history_kwargs(last):
    """Fetch one day first, then only bars after the last successful poll."""
    return {"period": "1d"} if last is None else {"start": last}


def next_sleep(got_data, interval, backoff):
    """Wait for the poll interval after data, otherwise for the idle backoff."""
    return interval if got_data else backoff


def bar_volume(value):
    """Yahoo uses NaN for missing volume; store 0 instead."""
    return 0 if value != value else int(value)
