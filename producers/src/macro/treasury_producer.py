"""Fetch US Treasury average interest rates into the macro topic."""

import logging
import time

import requests

from .. import config
from ..base_producer import BaseProducer

log = logging.getLogger(__name__)

INTERVAL_SECONDS = 86400


class TreasuryProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_MACRO)

    def run(self):
        while True:
            try:
                # Average interest rates on Treasury securities.
                # shortcut: not the par yield curve; switch feeds if a true
                # yield curve is needed.
                url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates"
                params = {
                    "sort": "-record_date",
                    "page[size]": "1",
                }
                resp = requests.get(url, params=params, timeout=15)
                resp.raise_for_status()

                for row in resp.json()["data"]:
                    self.publish("TREASURY_AVG_INTEREST_RATE", {
                        "indicator": "TREASURY_AVG_INTEREST_RATE",
                        "timestamp": row["record_date"],
                        "value": float(row["avg_interest_rate_amt"]),
                        "source": "treasury",
                    })
                self.flush()
            except Exception as e:
                log.error("Treasury fetch failed: %s", e)
            time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    TreasuryProducer().run()