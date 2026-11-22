"""Publish the Nasdaq Trader symbol catalogue joined to SEC CIKs."""

import logging

import requests

from .. import config
from ..base_producer import BaseProducer
from .nasdaq_symbols import parse_symbol_directory
from .sec_client import SECClient

log = logging.getLogger(__name__)

DIRECTORIES = {
    "nasdaq": "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
    "other": "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
}
SEC_TICKERS = "https://www.sec.gov/files/company_tickers.json"
INTERVAL_SECONDS = 86400
RETRY_SECONDS = 3600


class SymbolCatalogProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_SYMBOLS)
        self.client = SECClient()

    def _cik_map(self):
        data = self.client.json(SEC_TICKERS)
        return {
            entry["ticker"].upper(): entry["cik_str"]
            for entry in data.values()
            if entry.get("ticker") and entry.get("cik_str")
        }

    def _fetch(self):
        ok = True
        try:
            ciks = self._cik_map()
        except Exception as e:
            ok = False
            ciks = {}
            log.error("SEC ticker map failed, publishing without CIKs: %s", e)

        for source, url in DIRECTORIES.items():
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                for row in parse_symbol_directory(resp.text, source):
                    row["cik"] = ciks.get(row["symbol"].upper())
                    row["source"] = "nasdaq_trader"
                    self.publish(f"{row['symbol']}:{row['exchange']}", row)
            except Exception as e:
                ok = False
                log.error("Symbol directory %s failed: %s", source, e)
        return ok

    def run(self):
        self.poll_forever(self._fetch, INTERVAL_SECONDS, RETRY_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    SymbolCatalogProducer().run()
