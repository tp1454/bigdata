"""Poll Yahoo Finance 1m bars into the equities topic."""

import logging
import os
import time

import yfinance as yf

from .. import config
from ..base_producer import BaseProducer
from .yahoo_bars import bar_volume, history_kwargs, next_sleep, should_publish

log = logging.getLogger(__name__)

SYMBOLS = config.equity_symbols()
INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL", "60"))
IDLE_BACKOFF_SECONDS = int(os.getenv("POLL_BACKOFF", "300"))


class YahooProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_EQUITIES)
        self.last_ts = {}

    def run(self):
        while True:
            got_data = False
            for symbol in SYMBOLS:
                try:
                    last = self.last_ts.get(symbol)
                    history = yf.Ticker(symbol).history(
                        interval="1m", **history_kwargs(last)
                    )
                    if history.empty:
                        continue
                    got_data = True
                    newest = history.index[-1]
                    for ts, row in history.iterrows():
                        # Skip bars already sent; the newest bar keeps moving.
                        if not should_publish(ts, last, newest):
                            continue
                        # numpy scalars aren't JSON-serializable
                        self.publish(symbol, {
                            "symbol": symbol,
                            "ts": ts.isoformat(),
                            "open": float(row["Open"]),
                            "high": float(row["High"]),
                            "low": float(row["Low"]),
                            "close": float(row["Close"]),
                            "volume": bar_volume(row["Volume"]),
                        })
                    self.last_ts[symbol] = newest
                except Exception as e:
                    log.error("Yahoo fetch failed for %s: %s", symbol, e)
            self.flush()
            if not got_data:
                log.warning("no Yahoo data; backing off %ss",
                            IDLE_BACKOFF_SECONDS)
            time.sleep(next_sleep(got_data, INTERVAL_SECONDS, IDLE_BACKOFF_SECONDS))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    YahooProducer().run()
