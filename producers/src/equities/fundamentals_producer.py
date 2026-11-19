"""Publish income, balance sheet, cash flow and earnings rows."""

import logging
import time

import yfinance as yf

from .. import config
from ..base_producer import BaseProducer
from ..numeric import number

log = logging.getLogger(__name__)

SYMBOLS = config.SYMBOLS
INTERVAL_SECONDS = 86400  # daily

STATEMENTS = {
    "income": ("income_stmt", "quarterly_income_stmt"),
    "balance": ("balance_sheet", "quarterly_balance_sheet"),
    "cashflow": ("cashflow", "quarterly_cashflow"),
}
EARNINGS = ("epsActual", "epsEstimate", "epsDifference", "surprisePercent")


class FundamentalsProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_STATEMENTS)

    def _publish_frame(self, symbol, statement, period, frame):
        for column in frame.columns:
            rows = []
            for line_item, value in frame[column].items():
                value = number(value)
                if value is not None:
                    rows.append({"line_item": str(line_item), "value": value})
            if not rows:
                continue
            end_date = column.date().isoformat()
            self.publish(f"{symbol}:{statement}:{end_date}", {
                "symbol": symbol,
                "statement": statement,
                "period": period,
                "end_date": end_date,
                "rows": rows,
                "source": "yahoo",
            })

    def run(self):
        while True:
            for symbol in SYMBOLS:
                try:
                    ticker = yf.Ticker(symbol)
                    for statement, attrs in STATEMENTS.items():
                        for period, attr in zip(("annual", "quarterly"), attrs):
                            frame = getattr(ticker, attr)
                            if frame is not None and not frame.empty:
                                self._publish_frame(symbol, statement, period, frame)

                    earnings = ticker.earnings_history
                    if earnings is None or earnings.empty:
                        continue
                    for period_end, row in earnings.iterrows():
                        rows = []
                        for line_item in EARNINGS:
                            value = number(row.get(line_item))
                            if value is not None:
                                rows.append({"line_item": line_item, "value": value})
                        if not rows:
                            continue
                        end_date = period_end.date().isoformat()
                        self.publish(f"{symbol}:earnings:{end_date}", {
                            "symbol": symbol,
                            "statement": "earnings",
                            "period": "quarterly",
                            "end_date": end_date,
                            "rows": rows,
                            "source": "yahoo",
                        })
                except Exception as e:
                    log.error("Fundamentals fetch failed for %s: %s", symbol, e)
            self.flush()
            time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    FundamentalsProducer().run()
