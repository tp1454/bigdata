package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class CryptoParser extends ProcessFunction<String, ClickHouseRow> {
    private static final long serialVersionUID = 1L;

    private final String topic;
    private transient ObjectMapper mapper;

    public CryptoParser(String topic) {
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
            if (root.has("trades")) {
                String source = text(root, "source", "binance");
                for (JsonNode trade : root.get("trades")) {
                    try {
                        out.collect(tradeRow(trade, source));
                    } catch (RuntimeException e) {
                        out.collect(errorRow("bad trade: " + e.getMessage(), value));
                    }
                }
            } else if (root.has("interval")) {
                out.collect(ohlcvRow(root));
            } else if (root.has("trade_id")) {
                out.collect(tradeRow(root, text(root, "source", "binance")));
            } else if (root.has("price")) {
                out.collect(marketRow(root));
            } else {
                out.collect(errorRow("unrecognized crypto payload", value));
            }
        } catch (Exception e) {
            out.collect(errorRow(e.getClass().getSimpleName() + ": " + e.getMessage(), value));
        }
    }

    private ClickHouseRow tradeRow(JsonNode trade, String fallbackSource) {
        String symbol = text(trade, "symbol", "");
        long ts = trade.path("timestamp").asLong(0);
        long id = trade.path("trade_id").asLong(0);
        if (symbol.isEmpty() || ts <= 0 || id <= 0) {
            throw new IllegalArgumentException("incomplete trade");
        }
        return new ClickHouseRow(
                "crypto_trades",
                symbol,
                new Timestamp(ts),
                number(trade, "price", 0.0),
                number(trade, "quantity", 0.0),
                id,
                text(trade, "source", fallbackSource));
    }

    private ClickHouseRow ohlcvRow(JsonNode row) {
        String symbol = text(row, "symbol", "");
        long ts = row.path("timestamp").asLong(0);
        if (symbol.isEmpty() || ts <= 0) {
            throw new IllegalArgumentException("incomplete ohlcv");
        }
        return new ClickHouseRow(
                "crypto_ohlcv",
                symbol,
                text(row, "interval", ""),
                new Timestamp(ts),
                number(row, "open", 0.0),
                number(row, "high", 0.0),
                number(row, "low", 0.0),
                number(row, "close", 0.0),
                number(row, "volume", 0.0),
                text(row, "source", "binance"));
    }

    private ClickHouseRow marketRow(JsonNode row) {
        String symbol = text(row, "symbol", "");
        long ts = row.path("timestamp").asLong(0);
        if (symbol.isEmpty() || ts <= 0) {
            throw new IllegalArgumentException("incomplete market row");
        }
        return new ClickHouseRow(
                "crypto_market_data",
                symbol,
                new Timestamp(ts),
                number(row, "price", 0.0),
                nullableNumber(row, "market_cap"),
                nullableNumber(row, "volume_24h"),
                text(row, "source", "coingecko"));
    }

    private ClickHouseRow errorRow(String error, String raw) {
        String truncated = raw.length() > 500 ? raw.substring(0, 500) : raw;
        return new ClickHouseRow("pipeline_errors", topic, error, truncated);
    }

    private static String text(JsonNode node, String field, String fallback) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return fallback;
        }
        String text = value.asText();
        return text.isEmpty() ? fallback : text;
    }

    private static double number(JsonNode node, String field, double fallback) {
        Double value = nullableNumber(node, field);
        return value == null ? fallback : value;
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
}
