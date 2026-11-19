import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "producers" / "src"))

from macro.treasury_yield import parse_yield_csv

SAMPLE = """Date,"1 Mo","2 Mo","3 Mo","6 Mo","1 Yr","2 Yr","10 Yr","30 Yr"
11/18/2026,4.10,4.27,4.34,4.43,4.62,4.80,5.07,5.41
11/17/2026,4.09,4.26,4.33,4.41,4.60,4.80,5.10,5.46
"""


def test_parse_picks_newest_row_and_requested_maturities():
    date_iso, points = parse_yield_csv(SAMPLE)
    assert date_iso == "2026-11-18"
    assert dict(points) == {
        "TREASURY_YIELD_3M": 4.34,
        "TREASURY_YIELD_2Y": 4.80,
        "TREASURY_YIELD_10Y": 5.07,
        "TREASURY_YIELD_30Y": 5.41,
    }


def test_parse_skips_blank_maturity_and_empty_input():
    date_iso, points = parse_yield_csv('Date,"3 Mo","10 Yr"\n11/18/2026,4.34,\n')
    assert date_iso == "2026-11-18"
    assert points == [("TREASURY_YIELD_3M", 4.34)]
    assert parse_yield_csv("") == (None, [])
