from equities.nasdaq_symbols import parse_symbol_directory
from equities.sec_13f import parse_cover, parse_information_table


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
