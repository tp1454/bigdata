"""Fetch commodity futures closes from Yahoo Finance into the macro topic."""

import logging

import yfinance as yf

from .. import config
from ..base_producer import BaseProducer

log = logging.getLogger(__name__)

INTERVAL_SECONDS = 86400  # commodities settle once a day

SERIES = {
    "CL=F": "COMMODITY_WTI",
    "BZ=F": "COMMODITY_BRENT",
    "NG=F": "COMMODITY_NATURAL_GAS",
    "GC=F": "COMMODITY_GOLD",
    "SI=F": "COMMODITY_SILVER",
    "HG=F": "COMMODITY_COPPER",
    "ZW=F": "COMMODITY_WHEAT",
    "ZC=F": "COMMODITY_CORN",
    "CT=F": "COMMODITY_COTTON",
    "SB=F": "COMMODITY_SUGAR",
    "KC=F": "COMMODITY_COFFEE",
}


class CommoditiesProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_MACRO)

    def _fetch(self):
        for ticker, indicator in SERIES.items():
            try:
                history = yf.Ticker(ticker).history(
                    period="5d", interval="1d", auto_adjust=False
                )
                if history.empty:
                    log.warning("No price history for %s", ticker)
                    continue
                close = float(history["Close"].iloc[-1])
                if close != close:  # NaN on an incomplete/expired contract
                    log.warning("Skipping NaN close for %s", ticker)
                    continue
                self.publish(indicator, {
                    "indicator": indicator,
                    "timestamp": history.index[-1].date().isoformat(),
                    "value": close,
                    "source": "yahoo",
                })
            except Exception as e:
                log.error("Commodity fetch failed for %s: %s", ticker, e)
        return True

    def run(self):
        self.poll_forever(self._fetch, INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    CommoditiesProducer().run()
