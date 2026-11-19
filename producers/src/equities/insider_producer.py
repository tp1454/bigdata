"""Publish insider transactions into the insider topic."""

import logging

import yfinance as yf

from .. import config
from ..base_producer import BaseProducer
from ..numeric import number

log = logging.getLogger(__name__)

SYMBOLS = config.SYMBOLS
INTERVAL_SECONDS = 86400  # daily


class InsiderProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_INSIDER)

    def _fetch(self):
        for symbol in SYMBOLS:
            try:
                frame = yf.Ticker(symbol).insider_transactions
                if frame is None or frame.empty:
                    continue
                for _, row in frame.iterrows():
                    start = row.get("Start Date")
                    if start is None or start != start:  # NaT
                        continue
                    insider = row.get("Insider") or ""
                    if not insider:
                        continue
                    self.publish(f"{symbol}:{start.date().isoformat()}:{insider}", {
                        "symbol": symbol,
                        "insider": str(insider),
                        "position": str(row.get("Position") or ""),
                        "transaction": str(row.get("Transaction") or row.get("Text") or ""),
                        "transaction_date": start.date().isoformat(),
                        "shares": number(row.get("Shares")) or 0.0,
                        "value": number(row.get("Value")),
                        "ownership": str(row.get("Ownership") or ""),
                        "source": "yahoo",
                    })
            except Exception as e:
                log.error("Insider fetch failed for %s: %s", symbol, e)
        return True

    def run(self):
        self.poll_forever(self._fetch, INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    InsiderProducer().run()
