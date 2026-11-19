"""Pure helpers for the Yahoo 1m bar producer."""


def should_publish(ts, last, newest):
    """Publish new bars, plus the newest bar because it keeps moving."""
    return last is None or ts > last or ts == newest


def bar_volume(value):
    """Yahoo uses NaN for missing volume; store 0 instead."""
    return 0 if value != value else int(value)
