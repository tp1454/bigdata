import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "flink-jobs" / "src"))

from tables import TABLES
from transforms import ERROR_COLUMNS, ERROR_TABLE, TOPICS


def test_tables_match_the_transforms():
    assert set(TABLES) == {
        table_name for table_name, _, _ in TOPICS.values()
    } | {ERROR_TABLE}
    for table_name, columns, _ in TOPICS.values():
        assert TABLES[table_name]["columns"] == columns
    assert TABLES[ERROR_TABLE]["columns"] == ERROR_COLUMNS


def test_types_are_known_and_aligned():
    allowed = {"STRING", "TIMESTAMP(3)", "DOUBLE", "BIGINT", "DATE"}
    for spec in TABLES.values():
        assert len(spec["columns"]) == len(spec["types"])
        assert set(spec["types"]) <= allowed
