package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

public class MessageParser extends ProcessFunction<String, ClickHouseRow> {
    private static final long serialVersionUID = 1L;

    private final String topic;
    private transient ObjectMapper mapper;

    public MessageParser(String topic) {
        this.topic = topic;
    }

    private ObjectMapper mapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

    @Override
    public void processElement(String value, Context ctx, Collector<ClickHouseRow> out) {
        try {
            JsonNode root = mapper().readTree(value);
            switch (topic) {
                case "equities.prices":
                    out.collect(equity(root));
                    break;
                case "crypto.trades":
                    String source = text(root, "source", "binance");
                    JsonNode trades = root.has("trades") ? root.get("trades") : null;
                    if (trades != null && trades.isArray()) {
                        for (JsonNode trade : trades) {
                            try {
                                out.collect(tradeRow(trade, source));
                            } catch (RuntimeException e) {
                                out.collect(errorRow("bad trade: " + e.getMessage(), value));
                            }
                        }
                    } else {
                        out.collect(tradeRow(root, source));
                    }
                    break;
                case "crypto.ohlcv":
                    out.collect(ohlcv(root));
                    break;
                case "crypto.market":
                    out.collect(market(root));
                    break;
                case "forex.rates":
                    out.collect(forex(root));
                    break;
                case "macro.indicators":
                    out.collect(macro(root));
                    break;
                case "fundamentals.filings":
                    filings(root, out);
                    break;
                case "fundamentals.actions":
                    out.collect(action(root));
                    break;
                case "fundamentals.statements":
                    statements(root, out);
                    break;
                case "fundamentals.insider":
                    out.collect(insider(root));
                    break;
                case "options.metrics":
                    out.collect(options(root));
                    break;
                case "institutional.holdings":
                    holdings(root, out);
                    break;
                case "reference.symbols":
                    out.collect(symbol(root));
                    break;
                default:
                    out.collect(errorRow("unknown topic", value));
            }
        } catch (Exception e) {
            out.collect(errorRow(e.getClass().getSimpleName() + ": " + e.getMessage(), value));
        }
    }

    private ClickHouseRow equity(JsonNode row) {
        return new ClickHouseRow(
                "equities_prices",
                requiredText(row, "symbol"),
                isoTimestamp(requiredText(row, "ts")),
                requiredNumber(row, "open"),
                requiredNumber(row, "high"),
                requiredNumber(row, "low"),
                requiredNumber(row, "close"),
                requiredLong(row, "volume"));
    }

    private ClickHouseRow tradeRow(JsonNode trade, String fallbackSource) {
        String symbol = requiredText(trade, "symbol");
        long id = requiredLong(trade, "trade_id");
        return new ClickHouseRow(
                "crypto_trades",
                symbol,
                new Timestamp(requiredLong(trade, "timestamp")),
                requiredNumber(trade, "price"),
                requiredNumber(trade, "quantity"),
                id,
                text(trade, "source", fallbackSource));
    }

    private ClickHouseRow ohlcv(JsonNode row) {
        return new ClickHouseRow(
                "crypto_ohlcv",
                requiredText(row, "symbol"),
                requiredText(row, "interval"),
                new Timestamp(requiredLong(row, "timestamp")),
                requiredNumber(row, "open"),
                requiredNumber(row, "high"),
                requiredNumber(row, "low"),
                requiredNumber(row, "close"),
                requiredNumber(row, "volume"),
                requiredText(row, "source"));
    }

    private ClickHouseRow market(JsonNode row) {
        return new ClickHouseRow(
                "crypto_market_data",
                requiredText(row, "symbol"),
                new Timestamp(requiredLong(row, "timestamp")),
                requiredNumber(row, "price"),
                nullableNumber(row, "market_cap"),
                nullableNumber(row, "volume_24h"),
                requiredText(row, "source"));
    }

    private ClickHouseRow forex(JsonNode row) {
        return new ClickHouseRow(
                "forex_rates",
                requiredText(row, "pair"),
                new Timestamp(requiredLong(row, "timestamp")),
                requiredNumber(row, "rate"),
                requiredText(row, "source"));
    }

    private ClickHouseRow macro(JsonNode row) {
        return new ClickHouseRow(
                "macro_indicators",
                requiredText(row, "indicator"),
                date(requiredText(row, "timestamp")),
                requiredNumber(row, "value"),
                requiredText(row, "source"));
    }

    private void filings(JsonNode root, Collector<ClickHouseRow> out) {
        JsonNode recent = root.has("filings") ? root.get("filings") : null;
        if (recent == null) {
            return;
        }
        JsonNode accs = recent.path("accessionNumber");
        JsonNode dates = recent.path("filingDate");
        JsonNode forms = recent.path("form");
        JsonNode docs = recent.path("primaryDocument");
        String cik = text(root, "cik", "");
        String entity = text(root, "entity_name", "");
        String sic = text(root, "sic", "");
        String source = text(root, "source", "");
        for (int i = 0; i < accs.size(); i++) {
            String filed = dates.path(i).asText("");
            String form = forms.path(i).asText("");
            if (filed.isBlank() || form.isBlank()) {
                continue;
            }
            out.collect(new ClickHouseRow(
                    "company_filings",
                    cik,
                    entity,
                    sic,
                    accs.get(i).asText(),
                    date(filed),
                    form,
                    docs.path(i).asText(""),
                    source));
        }
    }

    private ClickHouseRow action(JsonNode row) {
        return new ClickHouseRow(
                "corporate_actions",
                requiredText(row, "symbol"),
                requiredText(row, "action"),
                date(requiredText(row, "action_date")),
                requiredNumber(row, "value"),
                requiredText(row, "source"));
    }

    private void statements(JsonNode root, Collector<ClickHouseRow> out) {
        JsonNode rows = root.get("rows");
        if (rows == null || !rows.isArray() || rows.isEmpty()) {
            throw new IllegalArgumentException("empty fundamentals batch");
        }
        String symbol = requiredText(root, "symbol");
        String statement = requiredText(root, "statement");
        String period = requiredText(root, "period");
        Date endDate = date(requiredText(root, "end_date"));
        String source = requiredText(root, "source");
        for (JsonNode row : rows) {
            out.collect(new ClickHouseRow(
                    "fundamentals",
                    symbol,
                    statement,
                    period,
                    endDate,
                    requiredText(row, "line_item"),
                    requiredNumber(row, "value"),
                    source));
        }
    }

    private ClickHouseRow insider(JsonNode row) {
        return new ClickHouseRow(
                "insider_transactions",
                requiredText(row, "symbol"),
                requiredText(row, "insider"),
                text(row, "position", ""),
                text(row, "transaction", ""),
                date(requiredText(row, "transaction_date")),
                requiredNumber(row, "shares"),
                nullableNumber(row, "value"),
                text(row, "ownership", ""),
                requiredText(row, "source"));
    }

    private ClickHouseRow options(JsonNode row) {
        return new ClickHouseRow(
                "options_metrics",
                requiredText(row, "symbol"),
                date(requiredText(row, "expiry")),
                new Timestamp(requiredLong(row, "timestamp")),
                requiredNumber(row, "underlying_price"),
                requiredNumber(row, "call_volume"),
                requiredNumber(row, "put_volume"),
                requiredNumber(row, "call_oi"),
                requiredNumber(row, "put_oi"),
                nullableNumber(row, "put_call_volume_ratio"),
                nullableNumber(row, "put_call_oi_ratio"),
                nullableNumber(row, "volume_oi_ratio"),
                requiredText(row, "source"));
    }

    private void holdings(JsonNode root, Collector<ClickHouseRow> out) {
        JsonNode rows = root.get("rows");
        if (rows == null || !rows.isArray() || rows.isEmpty()) {
            throw new IllegalArgumentException("empty holdings batch");
        }
        String filerCik = requiredText(root, "filer_cik");
        String filerName = text(root, "filer_name", "");
        Date period = date(requiredText(root, "period"));
        Date filingDate = date(requiredText(root, "filing_date"));
        String form = requiredText(root, "form");
        String accession = requiredText(root, "accession_number");
        String source = requiredText(root, "source");
        for (JsonNode row : rows) {
            out.collect(new ClickHouseRow(
                    "institutional_holdings",
                    filerCik,
                    filerName,
                    period,
                    filingDate,
                    form,
                    accession,
                    requiredText(row, "cusip"),
                    requiredText(row, "issuer"),
                    text(row, "title", ""),
                    requiredNumber(row, "shares"),
                    text(row, "share_type", ""),
                    requiredNumber(row, "value"),
                    source));
        }
    }

    private ClickHouseRow symbol(JsonNode row) {
        return new ClickHouseRow(
                "symbol_catalog",
                requiredText(row, "symbol"),
                text(row, "name", ""),
                requiredText(row, "exchange"),
                requiredLong(row, "etf"),
                nullableLong(row, "cik"),
                requiredText(row, "source"));
    }

    private ClickHouseRow errorRow(String error, String raw) {
        String truncated = raw.length() > 500 ? raw.substring(0, 500) : raw;
        return new ClickHouseRow("pipeline_errors", topic, error, truncated);
    }

    private static Timestamp isoTimestamp(String value) {
        try {
            return Timestamp.from(OffsetDateTime.parse(value).toInstant());
        } catch (DateTimeParseException e) {
            return Timestamp.valueOf(LocalDateTime.parse(value));
        }
    }

    private static Date date(String value) {
        String day = value.length() > 10 ? value.substring(0, 10) : value;
        return Date.valueOf(LocalDate.parse(day));
    }

    private static String requiredText(JsonNode node, String field) {
        String value = text(node, field, "");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("missing " + field);
        }
        return value;
    }

    private static String text(JsonNode node, String field, String fallback) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return fallback;
        }
        String text = value.asText();
        return text.isEmpty() ? fallback : text;
    }

    private static double requiredNumber(JsonNode node, String field) {
        Double value = nullableNumber(node, field);
        if (value == null) {
            throw new IllegalArgumentException("missing " + field);
        }
        return value;
    }

    private static long requiredLong(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new IllegalArgumentException("missing " + field);
        }
        if (value.isNumber()) {
            return value.asLong();
        }
        try {
            return Long.parseLong(value.asText());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad " + field, e);
        }
    }

    private static Double nullableNumber(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isNumber()) {
            return value.asDouble();
        }
        try {
            return Double.parseDouble(value.asText());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long nullableLong(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isNumber()) {
            return value.asLong();
        }
        try {
            return Long.parseLong(value.asText());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
