"""Publish option-chain aggregates (put/call ratios) from Yahoo Finance."""

import logging
import os
import time

import yfinance as yf

from .. import config
from ..base_producer import BaseProducer

log = logging.getLogger(__name__)

SYMBOLS = config.equity_symbols()
MAX_EXPIRIES = int(os.getenv("OPTIONS_MAX_EXPIRIES", "8"))
INTERVAL_SECONDS = int(os.getenv("OPTIONS_INTERVAL", "3600"))
RETRY_SECONDS = 300


def _sum(frame, column):
    if frame is None or frame.empty or column not in frame:
        return 0.0
    return float(frame[column].fillna(0).sum())


def _ratio(numerator, denominator):
    return numerator / denominator if denominator else None


class OptionsProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_OPTIONS)

    def _fetch(self):
        got_data = False
        for symbol in SYMBOLS:
            try:
                ticker = yf.Ticker(symbol)
                expiries = list(ticker.options or [])[:MAX_EXPIRIES]
                if not expiries:
                    log.warning("No option expiries for %s", symbol)
                    continue
                history = ticker.history(period="5d", interval="1d")
                if history.empty:
                    log.warning("No underlying price for %s", symbol)
                    continue
                price = float(history["Close"].iloc[-1])
                if price != price or price <= 0:
                    log.warning("Skipping invalid price for %s", symbol)
                    continue

                for expiry in expiries:
                    try:
                        chain = ticker.option_chain(expiry)
                        call_volume = _sum(chain.calls, "volume")
                        put_volume = _sum(chain.puts, "volume")
                        call_oi = _sum(chain.calls, "openInterest")
                        put_oi = _sum(chain.puts, "openInterest")
                        self.publish(f"{symbol}:{expiry}", {
                            "symbol": symbol,
                            "expiry": expiry,
                            "timestamp": int(time.time() * 1000),
                            "underlying_price": price,
                            "call_volume": call_volume,
                            "put_volume": put_volume,
                            "call_oi": call_oi,
                            "put_oi": put_oi,
                            "put_call_volume_ratio": _ratio(put_volume, call_volume),
                            "put_call_oi_ratio": _ratio(put_oi, call_oi),
                            "volume_oi_ratio": _ratio(call_volume + put_volume, call_oi + put_oi),
                            "source": "yahoo",
                        })
                        got_data = True
                    except Exception as e:
                        log.error("Options chain failed for %s %s: %s", symbol, expiry, e)
            except Exception as e:
                log.error("Options fetch failed for %s: %s", symbol, e)
        return got_data

    def run(self):
        self.poll_forever(self._fetch, INTERVAL_SECONDS, RETRY_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    OptionsProducer().run()
