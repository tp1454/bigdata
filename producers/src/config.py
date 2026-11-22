"""Shared producer configuration: broker, topics and symbols.

The broker is environment-driven; topic names are part of the contract with
init/kafka/create-topics.sh and stay in code.
"""

import os

from dotenv import load_dotenv

from .sharding import symbols_from_env

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092,localhost:29093,localhost:29094")


def equity_symbols():
    return symbols_from_env("SYMBOLS", "AAPL,MSFT,GOOG", "SYMBOLS_SHARD")


def crypto_symbols():
    return symbols_from_env(
        "CRYPTO_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT", "CRYPTO_SYMBOLS_SHARD"
    )

TOPIC_EQUITIES = "equities.prices"
TOPIC_CRYPTO = "crypto.trades"
TOPIC_CRYPTO_OHLCV = "crypto.ohlcv"
TOPIC_CRYPTO_MARKET = "crypto.market"
TOPIC_FOREX = "forex.rates"
TOPIC_MACRO = "macro.indicators"
TOPIC_FUNDAMENTALS = "fundamentals.filings"
TOPIC_ACTIONS = "fundamentals.actions"
TOPIC_STATEMENTS = "fundamentals.statements"
TOPIC_INSIDER = "fundamentals.insider"
TOPIC_OPTIONS = "options.metrics"
TOPIC_HOLDINGS = "institutional.holdings"
TOPIC_SYMBOLS = "reference.symbols"
