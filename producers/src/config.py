"""Shared Kafka configuration: broker and topic names.

The broker is environment-driven; topic names are part of the contract with
init/kafka/create-topics.sh and stay in code.
"""

import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")

TOPIC_EQUITIES = "equities.prices"
TOPIC_CRYPTO = "crypto.trades"
TOPIC_CRYPTO_MARKET = "crypto.market"
TOPIC_FOREX = "forex.rates"
TOPIC_MACRO = "macro.indicators"
TOPIC_FUNDAMENTALS = "fundamentals.filings"
