"""Publish major index daily bars into the equities topic."""

import logging
import time

import yfinance as yf

from .. import config
from ..base_producer import BaseProducer

log = logging.getLogger(__name__)

INTERVAL_SECONDS = 3600  # daily bars; re-check hourly for the close

SYMBOLS = ["^GSPC", "^IXIC", "^DJI", "^VIX", "^RUT"]


class IndexProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_EQUITIES)

    def run(self):
        while True:
            for ticker in SYMBOLS:
                try:
                    history = yf.Ticker(ticker).history(period="5d", interval="1d")
                    if history.empty:
                        log.warning("No index history for %s", ticker)
                        continue
                    bar = history.iloc[-1]
                    prices = [bar[c] for c in ("Open", "High", "Low", "Close")]
                    if any(value != value for value in prices):
                        log.warning("Skipping incomplete bar for %s", ticker)
                        continue
                    volume = bar["Volume"]
                    if volume != volume:  # NaN on some indices
                        volume = 0
                    self.publish(ticker, {
                        "symbol": ticker,
                        "ts": history.index[-1].isoformat(),
                        "open": float(bar["Open"]),
                        "high": float(bar["High"]),
                        "low": float(bar["Low"]),
                        "close": float(bar["Close"]),
                        "volume": int(volume),
                    })
                except Exception as e:
                    log.error("Index fetch failed for %s: %s", ticker, e)
            self.flush()
            time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    IndexProducer().run()
