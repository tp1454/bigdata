"""Parse the Nasdaq Trader symbol directory files."""

EXCHANGES = {
    "A": "NYSE American",
    "N": "NYSE",
    "P": "NYSE Arca",
    "Z": "Cboe BZX",
    "V": "IEX",
}


def parse_symbol_directory(text, source):
    """Yield one dict per active, non-test symbol in a pipe-delimited file."""
    lines = text.splitlines()
    if not lines or "|" not in lines[0]:
        return
    header = lines[0].split("|")
    for line in lines[1:]:
        if "|" not in line or line.startswith("File Creation Time"):
            continue
        parts = line.split("|")
        if len(parts) != len(header):
            continue
        row = dict(zip(header, parts))
        if row.get("Test Issue") != "N":
            continue
        symbol = (row.get("Symbol") or row.get("ACT Symbol") or "").strip()
        if not symbol:
            continue
        if source == "nasdaq":
            exchange = "NASDAQ"
        else:
            code = (row.get("Exchange") or "").strip()
            exchange = EXCHANGES.get(code, code)
        yield {
            "symbol": symbol,
            "name": (row.get("Security Name") or "").strip(),
            "exchange": exchange,
            "etf": int((row.get("ETF") or "N") == "Y"),
        }
