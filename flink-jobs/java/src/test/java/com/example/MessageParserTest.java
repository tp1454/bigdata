package com.example;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageParserTest {
    private static List<ClickHouseRow> parse(String topic, String payload) {
        List<ClickHouseRow> rows = new ArrayList<>();
        Collector<ClickHouseRow> out = new Collector<ClickHouseRow>() {
            @Override
            public void collect(ClickHouseRow record) {
                rows.add(record);
            }

            @Override
            public void close() {
            }
        };
        new MessageParser(topic).processElement(payload, null, out);
        return rows;
    }

    @Test
    void equitiesRow() {
        List<ClickHouseRow> rows = parse("equities.prices",
                "{\"symbol\":\"AAPL\",\"ts\":\"2026-11-21T15:59:00-05:00\","
                        + "\"open\":1,\"high\":2,\"low\":0.5,\"close\":1.5,\"volume\":100}");
        assertEquals(1, rows.size());
        ClickHouseRow row = rows.get(0);
        assertEquals("equities_prices", row.table);
        assertEquals("AAPL", row.values[0]);
        assertInstanceOf(Timestamp.class, row.values[1]);
        assertEquals(100L, row.values[6]);
    }

    @Test
    void forexRow() {
        ClickHouseRow row = parse("forex.rates",
                "{\"pair\":\"USDJPY\",\"timestamp\":1700000000000,\"rate\":150.5,"
                        + "\"source\":\"frankfurter\"}").get(0);
        assertEquals("forex_rates", row.table);
        assertEquals("USDJPY", row.values[0]);
        assertEquals(150.5, (Double) row.values[2], 0.0001);
    }

    @Test
    void macroRow() {
        ClickHouseRow row = parse("macro.indicators",
                "{\"indicator\":\"CPI\",\"timestamp\":\"2025-01-01\","
                        + "\"value\":3.2,\"source\":\"bls\"}").get(0);
        assertEquals("macro_indicators", row.table);
        assertEquals(Date.valueOf("2025-01-01"), row.values[1]);
    }

    @Test
    void filingsSkipIncompleteRows() {
        List<ClickHouseRow> rows = parse("fundamentals.filings",
                "{\"cik\":\"0000320193\",\"entity_name\":\"Apple\",\"sic\":\"3571\","
                        + "\"source\":\"sec_edgar\",\"filings\":{"
                        + "\"accessionNumber\":[\"a1\",\"a2\"],"
                        + "\"filingDate\":[\"2025-01-01\",\"\"],"
                        + "\"form\":[\"10-K\",\"8-K\"],"
                        + "\"primaryDocument\":[\"d1\",\"d2\"]}}");
        assertEquals(1, rows.size());
        ClickHouseRow row = rows.get(0);
        assertEquals("company_filings", row.table);
        assertEquals("a1", row.values[3]);
        assertEquals(Date.valueOf("2025-01-01"), row.values[4]);
        assertEquals("10-K", row.values[5]);
    }

    @Test
    void statementsExpandLineItems() {
        List<ClickHouseRow> rows = parse("fundamentals.statements",
                "{\"symbol\":\"AAPL\",\"statement\":\"income\",\"period\":\"annual\","
                        + "\"end_date\":\"2025-09-30\",\"source\":\"yahoo\",\"rows\":["
                        + "{\"line_item\":\"Revenue\",\"value\":100},"
                        + "{\"line_item\":\"NetIncome\",\"value\":20}]}");
        assertEquals(2, rows.size());
        assertEquals("fundamentals", rows.get(0).table);
        assertEquals("Revenue", rows.get(0).values[4]);
        assertEquals("NetIncome", rows.get(1).values[4]);
    }

    @Test
    void insiderKeepsNullValue() {
        ClickHouseRow row = parse("fundamentals.insider",
                "{\"symbol\":\"AAPL\",\"insider\":\"X\",\"position\":\"CEO\","
                        + "\"transaction\":\"buy\",\"transaction_date\":\"2025-01-01\","
                        + "\"shares\":10.0,\"value\":null,\"ownership\":\"direct\","
                        + "\"source\":\"yahoo\"}").get(0);
        assertEquals("insider_transactions", row.table);
        assertNull(row.values[6]);
    }

    @Test
    void cryptoBatchExpandsTrades() {
        List<ClickHouseRow> rows = parse("crypto.trades",
                "{\"source\":\"binance\",\"trades\":[{\"symbol\":\"BTCUSDT\","
                        + "\"timestamp\":1700000000000,\"price\":\"100.5\","
                        + "\"quantity\":\"1.5\",\"trade_id\":7}]}");
        assertEquals(1, rows.size());
        assertEquals("crypto_trades", rows.get(0).table);
        assertEquals(7L, rows.get(0).values[4]);
    }

    @Test
    void badPayloadGoesToErrors() {
        List<ClickHouseRow> rows = parse("equities.prices", "not-json");
        assertEquals(1, rows.size());
        assertEquals("pipeline_errors", rows.get(0).table);
        assertTrue(((String) rows.get(0).values[1]).contains("Json"));
    }
}
