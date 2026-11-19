"""Numeric conversion shared by the producers."""


def number(value):
    """Return a float, or None for missing or non-numeric values."""
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return None if value != value else value
