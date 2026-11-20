import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[2] / "producers" / "src"))

from sharding import parse_shard, shard_symbols, symbols_from_env


def test_parse_shard_defaults_to_one_shard():
    assert parse_shard("") == (0, 1)
    assert parse_shard("1/3") == (1, 3)


def test_parse_shard_rejects_bad_values():
    for value in ("2/2", "-1/2", "x/2", "1/2/3"):
        with pytest.raises(ValueError):
            parse_shard(value)


def test_shard_symbols_splits_without_overlap():
    symbols = ["A", "B", "C", "D", "E"]
    assert shard_symbols(symbols, 0, 2) == ["A", "C", "E"]
    assert shard_symbols(symbols, 1, 2) == ["B", "D"]
    assert shard_symbols(symbols, 0, 1) == symbols


def test_symbols_from_env_shards_and_rejects_an_empty_shard(monkeypatch):
    monkeypatch.setenv("TEST_SYMBOLS", "A,B,C")
    monkeypatch.setenv("TEST_SHARD", "1/2")
    assert symbols_from_env("TEST_SYMBOLS", "x", "TEST_SHARD") == ["B"]

    monkeypatch.setenv("TEST_SYMBOLS", "A")
    monkeypatch.setenv("TEST_SHARD", "1/2")
    with pytest.raises(ValueError):
        symbols_from_env("TEST_SYMBOLS", "x", "TEST_SHARD")
