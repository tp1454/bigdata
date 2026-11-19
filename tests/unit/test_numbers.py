import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "producers" / "src"))

from numeric import number


def test_number_converts_and_preserves_missing():
    assert number("1.5") == 1.5
    assert number(None) is None
    assert number(float("nan")) is None
    assert number("nope") is None
