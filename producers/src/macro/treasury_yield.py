"""Parse the US Treasury daily par yield curve CSV."""

import csv
import io
from datetime import datetime

# CSV column -> indicator published to the macro topic.
MATURITIES = {
    "3 Mo": "TREASURY_YIELD_3M",
    "2 Yr": "TREASURY_YIELD_2Y",
    "10 Yr": "TREASURY_YIELD_10Y",
    "30 Yr": "TREASURY_YIELD_30Y",
}


def parse_yield_csv(text):
    """Return (ISO date, [(indicator, value), ...]) for the newest row.

    Treasury dates are MM/DD/YYYY and missing maturities are empty strings.
    Returns (None, []) when no usable row exists.
    """
    rows = [row for row in csv.DictReader(io.StringIO(text)) if row.get("Date")]
    if not rows:
        return None, []

    newest = max(rows, key=lambda row: datetime.strptime(row["Date"], "%m/%d/%Y"))
    date_iso = datetime.strptime(newest["Date"], "%m/%d/%Y").date().isoformat()
    points = []
    for column, indicator in MATURITIES.items():
        raw = (newest.get(column) or "").strip()
        if raw:
            points.append((indicator, float(raw)))
    return date_iso, points
