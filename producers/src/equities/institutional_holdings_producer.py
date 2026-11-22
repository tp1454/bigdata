"""Publish SEC 13F institutional holdings."""

import logging
import os

from .. import config
from ..base_producer import BaseProducer
from .sec_13f import parse_cover, parse_information_table
from .sec_client import SECClient

log = logging.getLogger(__name__)

CIKS = [c.strip().zfill(10) for c in
        os.getenv("INSTITUTIONAL_CIKS", "0001067983").split(",") if c.strip()]
MAX_FILINGS = 4
CHUNK_ROWS = 1000
INTERVAL_SECONDS = 86400
RETRY_SECONDS = 3600
SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"
ARCHIVES = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/"


class InstitutionalHoldingsProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_HOLDINGS)
        email = os.getenv("SEC_CONTACT_EMAIL", "")
        if not email:
            raise RuntimeError(
                "SEC_CONTACT_EMAIL is required: SEC requires a real contact "
                "email in the User-Agent"
            )
        self.client = SECClient(email)
        self.seen = set()

    def _files(self, cik, accession):
        base = ARCHIVES.format(cik=int(cik), accession=accession.replace("-", ""))
        listing = self.client.json(base + "index.json")
        names = [item["name"] for item in listing["directory"]["item"]]
        return base, names

    def _read_filing(self, cik, accession):
        base, names = self._files(cik, accession)
        cover_name = next((n for n in names if n.endswith("primary_doc.xml")), None)
        if cover_name is None:
            raise ValueError("no primary_doc.xml")
        period, filer_name = parse_cover(self.client.text(base + cover_name))
        if not period:
            raise ValueError("no periodOfReport")

        for name in names:
            if name == cover_name or not name.endswith(".xml"):
                continue
            try:
                rows = parse_information_table(self.client.text(base + name))
            except Exception:
                continue
            if rows is not None:
                return period, filer_name, rows
        raise ValueError("no informationTable XML")

    def _publish(self, cik, form, filing_date, accession):
        period, filer_name, rows = self._read_filing(cik, accession)
        for start in range(0, len(rows), CHUNK_ROWS):
            self.publish(f"{cik}:{accession}:{start // CHUNK_ROWS}", {
                "filer_cik": cik,
                "filer_name": filer_name,
                "period": period,
                "filing_date": filing_date,
                "form": form,
                "accession_number": accession,
                "rows": rows[start:start + CHUNK_ROWS],
                "source": "sec_13f",
            })
        log.info("Published %d holdings for %s %s", len(rows), cik, period)

    def _fetch(self):
        ok = True
        for cik in CIKS:
            try:
                recent = self.client.json(SUBMISSIONS.format(cik=cik))["filings"]["recent"]
                filings = [
                    (form, date, acc)
                    for form, date, acc in zip(
                        recent["form"], recent["filingDate"], recent["accessionNumber"])
                    if form.startswith("13F-HR")
                ][:MAX_FILINGS]
                # Oldest first so an amendment lands after the original it replaces.
                for form, filing_date, accession in reversed(filings):
                    if accession in self.seen:
                        continue
                    try:
                        self._publish(cik, form, filing_date, accession)
                        self.seen.add(accession)
                    except Exception as e:
                        ok = False
                        log.error("13F parse failed for %s %s: %s", cik, accession, e)
            except Exception as e:
                ok = False
                log.error("13F fetch failed for CIK %s: %s", cik, e)
        return ok

    def run(self):
        self.poll_forever(self._fetch, INTERVAL_SECONDS, RETRY_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    InstitutionalHoldingsProducer().run()
