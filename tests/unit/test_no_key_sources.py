import json
from datetime import date

from equities.nasdaq_symbols import parse_symbol_directory
from equities.sec_13f import parse_cover, parse_information_table
from transforms import parse_kafka_message


def one(topic, payload):
    return list(parse_kafka_message(topic, json.dumps(payload)))


def test_13f_cover_period_and_manager():
    xml = """<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">
      <headerData><filerInfo><periodOfReport>09-30-2026</periodOfReport></filerInfo></headerData>
      <formData><coverPage><filingManager><name>Example Capital</name></filingManager></coverPage></formData>
    </edgarSubmission>"""
    assert parse_cover(xml) == ("2026-09-30", "Example Capital")


def test_13f_information_table_aggregates_duplicate_lines():
    xml = """<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
      <infoTable><nameOfIssuer>ALLY</nameOfIssuer><titleOfClass>COM</titleOfClass>
        <cusip>02005N100</cusip><value>100</value>
        <shrsOrPrnAmt><sshPrnamt>10</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
      </infoTable>
      <infoTable><nameOfIssuer>ALLY</nameOfIssuer><titleOfClass>COM</titleOfClass>
        <cusip>02005N100</cusip><value>50</value>
        <shrsOrPrnAmt><sshPrnamt>5</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
      </infoTable>
    </informationTable>"""
    rows = parse_information_table(xml)
    assert rows == [{
        "cusip": "02005N100", "issuer": "ALLY", "title": "COM",
        "share_type": "SH", "shares": 15.0, "value": 150.0,
    }]


def test_13f_parser_rejects_other_xml():
    assert parse_information_table("<primaryDocument/>") is None


def test_symbol_directory_skips_test_issues():
    text = """Symbol|Security Name|Test Issue|ETF|Round Lot Size
AAPL|Apple Inc.|N|N|100
TESTX|Test Issue|Y|N|100
SPY|SPDR S&P 500|N|Y|100
File Creation Time: 1122202600:00
"""
    rows = list(parse_symbol_directory(text, "nasdaq"))
    assert rows == [
        {"symbol": "AAPL", "name": "Apple Inc.", "exchange": "NASDAQ", "etf": 0},
        {"symbol": "SPY", "name": "SPDR S&P 500", "exchange": "NASDAQ", "etf": 1},
    ]


def test_options_transform_keeps_missing_ratios_null():
    rows = one("options.metrics", {
        "symbol": "AAPL", "expiry": "2026-12-18", "timestamp": 1_700_000_000_000,
        "underlying_price": 200.0, "call_volume": 0, "put_volume": 0,
        "call_oi": 0, "put_oi": 0,
        "put_call_volume_ratio": None, "put_call_oi_ratio": None,
        "volume_oi_ratio": None, "source": "yahoo",
    })
    assert rows[0][0] == "options_metrics"
    assert rows[0][1][8:11] == (None, None, None)


def test_holdings_transform_expands_rows():
    rows = one("institutional.holdings", {
        "filer_cik": "0001067983", "filer_name": "Berkshire", "period": "2026-09-30",
        "filing_date": "2026-11-16", "form": "13F-HR", "accession_number": "a1",
        "source": "sec_13f",
        "rows": [{"cusip": "02005N100", "issuer": "ALLY", "title": "COM",
                  "shares": "15", "share_type": "SH", "value": "150"}],
    })
    assert rows == [("institutional_holdings", [(
        "0001067983", "Berkshire", date(2026, 9, 30),
        date(2026, 11, 16), "13F-HR", "a1",
        "02005N100", "ALLY", "COM", 15.0, "SH", 150.0, "sec_13f",
    )])]


def test_symbol_transform_keeps_missing_cik_null():
    rows = one("reference.symbols", {
        "symbol": "AAPL", "name": "Apple Inc.", "exchange": "NASDAQ",
        "etf": 0, "cik": None, "source": "nasdaq_trader",
    })
    assert rows == [("symbol_catalog", (
        "AAPL", "Apple Inc.", "NASDAQ", 0, None, "nasdaq_trader",
    ))]
