"""Parse SEC 13F cover pages and information tables."""

import xml.etree.ElementTree as ET
from datetime import datetime


def _local(tag):
    return tag.rsplit("}", 1)[-1]


def _find(node, name):
    for child in node.iter():
        if _local(child.tag) == name:
            return child
    return None


def _text(node, name, default=""):
    found = _find(node, name)
    if found is None or found.text is None:
        return default
    return found.text.strip()


def _number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_cover(xml_text):
    """Return (period ISO date, filer name) from a 13F primary document."""
    root = ET.fromstring(xml_text)
    raw_period = _text(root, "periodOfReport") or _text(root, "reportCalendarOrQuarter")
    period = None
    for fmt in ("%m-%d-%Y", "%Y-%m-%d"):
        try:
            period = datetime.strptime(raw_period[:10], fmt).date().isoformat()
            break
        except ValueError:
            continue
    manager = _find(root, "filingManager")
    name = _text(manager, "name") if manager is not None else ""
    return period, name


def parse_information_table(xml_text):
    """Return aggregated holdings, or None when the XML is not a 13F table."""
    root = ET.fromstring(xml_text)
    if _local(root.tag) != "informationTable":
        return None

    holdings = {}
    for entry in root:
        if _local(entry.tag) != "infoTable":
            continue
        shares_node = _find(entry, "shrsOrPrnAmt")
        shares = _number(_text(shares_node, "sshPrnamt")) if shares_node is not None else 0.0
        share_type = _text(shares_node, "sshPrnamtType") if shares_node is not None else ""
        key = (
            _text(entry, "cusip"),
            _text(entry, "nameOfIssuer"),
            _text(entry, "titleOfClass"),
            share_type,
        )
        row = holdings.setdefault(
            key, {"cusip": key[0], "issuer": key[1], "title": key[2],
                  "share_type": share_type, "shares": 0.0, "value": 0.0})
        row["shares"] += shares
        row["value"] += _number(_text(entry, "value"))
    return sorted(holdings.values(), key=lambda row: (row["cusip"], row["share_type"]))
