"""Destination table schemas and ClickHouse JDBC sink DDLs."""

# Row shapes are the contract with transforms.TOPICS. ingested_at and
# pipeline_errors.ts keep their ClickHouse defaults because they are not in
# the DDL.
TABLES = {
    "equities_prices": {
        "columns": ["symbol", "ts", "open", "high", "low", "close", "volume"],
        "types": [
            "STRING", "TIMESTAMP(3)", "DOUBLE", "DOUBLE", "DOUBLE",
            "DOUBLE", "BIGINT",
        ],
    },
    "crypto_trades": {
        "columns": [
            "symbol", "timestamp", "price", "quantity", "trade_id", "source",
        ],
        "types": [
            "STRING", "TIMESTAMP(3)", "DOUBLE", "DOUBLE", "BIGINT", "STRING",
        ],
    },
    "crypto_market_data": {
        "columns": [
            "symbol", "timestamp", "price", "market_cap", "volume_24h",
            "source",
        ],
        "types": [
            "STRING", "TIMESTAMP(3)", "DOUBLE", "DOUBLE", "DOUBLE", "STRING",
        ],
    },
    "crypto_ohlcv": {
        "columns": [
            "symbol", "interval", "timestamp", "open", "high", "low", "close",
            "volume", "source",
        ],
        "types": [
            "STRING", "STRING", "TIMESTAMP(3)", "DOUBLE", "DOUBLE", "DOUBLE",
            "DOUBLE", "DOUBLE", "STRING",
        ],
    },
    "forex_rates": {
        "columns": ["pair", "timestamp", "rate", "source"],
        "types": ["STRING", "TIMESTAMP(3)", "DOUBLE", "STRING"],
    },
    "macro_indicators": {
        "columns": ["indicator", "timestamp", "value", "source"],
        "types": ["STRING", "DATE", "DOUBLE", "STRING"],
    },
    "company_filings": {
        "columns": [
            "cik", "entity_name", "sic", "accession_number", "filing_date",
            "form", "primary_document", "source",
        ],
        "types": [
            "STRING", "STRING", "STRING", "STRING", "DATE", "STRING",
            "STRING", "STRING",
        ],
    },
    "corporate_actions": {
        "columns": ["symbol", "action", "action_date", "value", "source"],
        "types": ["STRING", "STRING", "DATE", "DOUBLE", "STRING"],
    },
    "fundamentals": {
        "columns": [
            "symbol", "statement", "period", "end_date", "line_item", "value",
            "source",
        ],
        "types": [
            "STRING", "STRING", "STRING", "DATE", "STRING", "DOUBLE",
            "STRING",
        ],
    },
    "insider_transactions": {
        "columns": [
            "symbol", "insider", "position", "transaction", "transaction_date",
            "shares", "value", "ownership", "source",
        ],
        "types": [
            "STRING", "STRING", "STRING", "STRING", "DATE", "DOUBLE",
            "DOUBLE", "STRING", "STRING",
        ],
    },
    "pipeline_errors": {
        "columns": ["topic", "error", "raw"],
        "types": ["STRING", "STRING", "STRING"],
    },
}


def sink_ddl(table_name, host, port, database, username, password):
    spec = TABLES[table_name]
    columns = ", ".join(
        f"`{column}` {sql_type}"
        for column, sql_type in zip(spec["columns"], spec["types"])
    )
    url = f"jdbc:clickhouse://{host}:{port}/{database}"
    return f"""
CREATE TABLE sink_{table_name} (
    {columns}
) WITH (
    'connector' = 'jdbc',
    'url' = '{url}',
    'table-name' = '{table_name}',
    'driver' = 'com.clickhouse.jdbc.ClickHouseDriver',
    'username' = '{username}',
    'password' = '{password}',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
)
"""
