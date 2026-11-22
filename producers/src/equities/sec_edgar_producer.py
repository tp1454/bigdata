"""Fetch SEC EDGAR company filings with proper rate limiting."""

import logging
import time

from .. import config
from ..base_producer import BaseProducer
from .sec_client import SECClient

log = logging.getLogger(__name__)

CIKS = ["0000320193"]  # Apple


class SECEdgarProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_FUNDAMENTALS)
        self.client = SECClient()

    def run(self):
        while True:
            for cik in CIKS:
                try:
                    url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
                    data = self.client.json(url)
                    self.publish(cik, {
                        "cik": cik,
                        "entity_name": data.get("name"),
                        "sic": data.get("sic"),
                        "filings": data.get("filings", {}).get("recent", {}),
                        "source": "sec_edgar",
                    })
                except Exception as e:
                    log.error("SEC fetch failed for CIK %s: %s", cik, e)
            self.flush()
            time.sleep(3600)  # re-check hourly


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    SECEdgarProducer().run()