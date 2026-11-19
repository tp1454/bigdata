"""Publish Binance daily klines into the crypto OHLCV topic."""

import logging
import time

import requests

from .. import config
from ..base_producer import BaseProducer

log = logging.getLogger(__name__)

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
INTERVAL = "1d"
INTERVAL_SECONDS = 3600  # daily bars; re-read recent history hourly
LIMIT = 5


class BinanceOHLCVProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_CRYPTO_OHLCV)

    def run(self):
        while True:
            for symbol in SYMBOLS:
                try:
                    resp = requests.get(
                        "https://api.binance.com/api/v3/klines",
                        params={"symbol": symbol, "interval": INTERVAL, "limit": LIMIT},
                        timeout=15,
                    )
                    resp.raise_for_status()
                    now_ms = int(time.time() * 1000)
                    for kline in resp.json():
                        # Skip the in-progress bar: its close and volume still move.
                        if kline[6] >= now_ms:
                            continue
                        self.publish(symbol, {
                            "symbol": symbol,
                            "interval": INTERVAL,
                            "timestamp": kline[0],
                            "open": float(kline[1]),
                            "high": float(kline[2]),
                            "low": float(kline[3]),
                            "close": float(kline[4]),
                            "volume": float(kline[5]),
                            "source": "binance",
                        })
                except Exception as e:
                    log.error("Binance klines failed for %s: %s", symbol, e)
            self.flush()
            time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    BinanceOHLCVProducer().run()
