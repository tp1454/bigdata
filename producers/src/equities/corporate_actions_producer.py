"""Publish dividends and splits into the corporate actions topic."""

import logging
import time

import yfinance as yf

from .. import config
from ..base_producer import BaseProducer

log = logging.getLogger(__name__)

SYMBOLS = config.SYMBOLS
INTERVAL_SECONDS = 86400  # daily


class CorporateActionsProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_ACTIONS)

    def run(self):
        while True:
            for symbol in SYMBOLS:
                try:
                    history = yf.Ticker(symbol).history(period="5y", actions=True)
                    for ts, row in history.iterrows():
                        actions = (
                            ("dividend", row.get("Dividends", 0)),
                            ("split", row.get("Stock Splits", 0)),
                        )
                        for action, value in actions:
                            if value:
                                self.publish(symbol, {
                                    "symbol": symbol,
                                    "action": action,
                                    "action_date": ts.date().isoformat(),
                                    "value": float(value),
                                    "source": "yahoo",
                                })
                except Exception as e:
                    log.error("Corporate actions fetch failed for %s: %s", symbol, e)
            self.flush()
            time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    CorporateActionsProducer().run()
