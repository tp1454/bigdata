import json
from datetime import date, datetime, timezone

import pytest

from transforms import _ms_to_dt, _to_date, parse_kafka_message


def one(topic, payload):
    return list(parse_kafka_message(topic, json.dumps(payload)))


def test_equities_prices():
    rows = one("equities.prices", {
        "symbol": "AAPL", "ts": "2026-11-14T15:30:00+00:00",
        "open": 1.5, "high": 2.0, "low": 1.0, "close": 1.75, "volume": 100,
    })
    assert rows == [("equities_prices", (
        "AAPL", datetime(2026, 11, 14, 15, 30, tzinfo=timezone.utc),
        1.5, 2.0, 1.0, 1.75, 100,
    ))]


def test_crypto_trades_ms_epoch():
    rows = one("crypto.trades", {
        "symbol": "BTCUSDT", "timestamp": 1_700_000_000_123,
        "price": "42.5", "quantity": 0.1, "trade_id": "7", "source": "binance",
    })
    table, row = rows[0]
    assert table == "crypto_trades"
    assert row[1] == datetime.fromtimestamp(1_700_000_000.123, tz=timezone.utc)
    assert row[2:] == (42.5, 0.1, 7, "binance")


def test_crypto_market_keeps_missing_optionals_as_none():
    rows = one("crypto.market", {
        "symbol": "bitcoin", "timestamp": 1_700_000_000_000,
        "price": 100, "source": "coingecko",
    })
    assert rows[0][1][3:5] == (None, None)


def test_forex_rates():
    rows = one("forex.rates", {
        "pair": "EURUSD", "timestamp": 1_700_000_000_000,
        "rate": 1.08, "source": "frankfurter",
    })
    assert rows == [("forex_rates", (
        "EURUSD", datetime.fromtimestamp(1_700_000_000, tz=timezone.utc),
        1.08, "frankfurter",
    ))]


def test_macro_uses_date_only():
    rows = one("macro.indicators", {
        "indicator": "CPI", "timestamp": "2026-10-01",
        "value": 3.2, "source": "bls",
    })
    assert rows[0][1][1] == date(2026, 10, 1)


def test_sec_flattens_one_message_into_many_rows_and_skips_incomplete():
    rows = one("fundamentals.filings", {
        "cik": "0000320193", "entity_name": "Apple", "sic": "3571",
        "source": "sec_edgar",
        "filings": {
            "accessionNumber": ["a-1", "a-2", "a-3"],
            "filingDate": ["2026-01-02", "2026-01-03", "2026-01-04"],
            "form": ["10-K", "8-K", None],
            "primaryDocument": ["d1.htm", "d2.htm", "d3.htm"],
        },
    })
    assert [r[1][3] for r in rows] == ["a-1", "a-2"]
    assert all(r[0] == "company_filings" for r in rows)
    assert rows[1][1][4] == date(2026, 1, 3)


@pytest.mark.parametrize("raw", ["not json", "{}", '{"symbol": "X"}'])
def test_bad_records_are_dead_lettered(raw):
    rows = list(parse_kafka_message("equities.prices", raw))
    assert len(rows) == 1
    table, (topic, error, stored) = rows[0]
    assert table == "pipeline_errors"
    assert topic == "equities.prices"
    assert error
    assert stored == raw


def test_unknown_topic_is_dead_lettered():
    assert list(parse_kafka_message("nope", "{}")) == [
        ("pipeline_errors", ("nope", "unknown topic", "{}")),
    ]


def test_dead_letter_payload_is_truncated():
    rows = list(parse_kafka_message("equities.prices", "x" * 1000))
    assert len(rows[0][1][2]) == 500


def test_time_helpers():
    assert _ms_to_dt(None) is None
    assert _to_date(None) is None
    assert _ms_to_dt(0) is None
    assert _to_date("2026-01-02T03:04:05Z") == date(2026, 1, 2)
