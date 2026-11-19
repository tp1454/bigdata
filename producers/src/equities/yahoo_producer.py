"""Poll Yahoo Finance 1m bars into the equities topic."""

import logging
import os
import time

import yfinance as yf

from .. import config
from ..base_producer import BaseProducer

log = logging.getLogger(__name__)

SYMBOLS = config.SYMBOLS
INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL", "15"))


class YahooProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_EQUITIES)
        self.last_ts = {}

    def run(self):
        while True:
            for symbol in SYMBOLS:
                history = yf.Ticker(symbol).history(period="1d", interval="1m")
                if history.empty:
                    continue
                last = self.last_ts.get(symbol)
                newest = history.index[-1]
                for ts, row in history.iterrows():
                    # Skip bars already sent; the newest bar keeps moving.
                    if last is not None and ts <= last and ts != newest:
                        continue
                    # numpy scalars aren't JSON-serializable
                    self.publish(symbol, {
                        "symbol": symbol,
                        "ts": ts.isoformat(),
                        "open": float(row["Open"]),
                        "high": float(row["High"]),
                        "low": float(row["Low"]),
                        "close": float(row["Close"]),
                        "volume": int(row["Volume"]),
                    })
                self.last_ts[symbol] = newest
            self.flush()
            time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    YahooProducer().run()
