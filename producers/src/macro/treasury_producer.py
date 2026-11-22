"""Fetch the US Treasury daily par yield curve into the macro topic."""

import logging
from datetime import datetime, timezone

import requests

from .. import config
from ..base_producer import BaseProducer
from .treasury_yield import parse_yield_csv

log = logging.getLogger(__name__)

INTERVAL_SECONDS = 86400  # daily
RETRY_SECONDS = 300       # retry a failed fetch sooner
YIELD_CSV_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/"
    "interest-rates/daily-treasury-rates.csv/{year}/all"
    "?type=daily_treasury_yield_curve&field_tdr_date_value={year}&page&_format=csv"
)


class TreasuryProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_MACRO)

    def _fetch(self):
        year = datetime.now(timezone.utc).year
        try:
            resp = requests.get(YIELD_CSV_URL.format(year=year), timeout=60)
            resp.raise_for_status()
            date_iso, points = parse_yield_csv(resp.text)
            if not points:
                log.warning("Treasury yield CSV had no rows for %s", year)
                return False
            for indicator, value in points:
                self.publish(indicator, {
                    "indicator": indicator,
                    "timestamp": date_iso,
                    "value": value,
                    "source": "treasury",
                })
            return True
        except Exception as e:
            log.error("Treasury yield fetch failed: %s", e)
            return False

    def run(self):
        self.poll_forever(self._fetch, INTERVAL_SECONDS, RETRY_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    TreasuryProducer().run()
