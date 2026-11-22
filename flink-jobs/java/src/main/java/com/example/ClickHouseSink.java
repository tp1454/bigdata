package com.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

public class ClickHouseSink extends RichSinkFunction<ClickHouseRow>
        implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;

    private static final Map<String, String> INSERT_SQL = Map.ofEntries(
            Map.entry("equities_prices",
                    "INSERT INTO equities_prices"
                            + " (symbol,ts,open,high,low,close,volume)"
                            + " VALUES (?,?,?,?,?,?,?)"),
            Map.entry("crypto_trades",
                    "INSERT INTO crypto_trades (symbol,timestamp,price,quantity,trade_id,source)"
                            + " VALUES (?,?,?,?,?,?)"),
            Map.entry("crypto_ohlcv",
                    "INSERT INTO crypto_ohlcv"
                            + " (symbol,interval,timestamp,open,high,low,close,volume,source)"
                            + " VALUES (?,?,?,?,?,?,?,?,?)"),
            Map.entry("crypto_market_data",
                    "INSERT INTO crypto_market_data"
                            + " (symbol,timestamp,price,market_cap,volume_24h,source)"
                            + " VALUES (?,?,?,?,?,?)"),
            Map.entry("forex_rates",
                    "INSERT INTO forex_rates (pair,timestamp,rate,source) VALUES (?,?,?,?)"),
            Map.entry("macro_indicators",
                    "INSERT INTO macro_indicators (indicator,timestamp,value,source)"
                            + " VALUES (?,?,?,?)"),
            Map.entry("company_filings",
                    "INSERT INTO company_filings"
                            + " (cik,entity_name,sic,accession_number,filing_date,form,"
                            + "primary_document,source)"
                            + " VALUES (?,?,?,?,?,?,?,?)"),
            Map.entry("corporate_actions",
                    "INSERT INTO corporate_actions (symbol,action,action_date,value,source)"
                            + " VALUES (?,?,?,?,?)"),
            Map.entry("fundamentals",
                    "INSERT INTO fundamentals"
                            + " (symbol,statement,period,end_date,line_item,value,source)"
                            + " VALUES (?,?,?,?,?,?,?)"),
            Map.entry("insider_transactions",
                    "INSERT INTO insider_transactions"
                            + " (symbol,insider,position,`transaction`,transaction_date,shares,"
                            + "value,ownership,source)"
                            + " VALUES (?,?,?,?,?,?,?,?,?)"),
            Map.entry("options_metrics",
                    "INSERT INTO options_metrics"
                            + " (symbol,expiry,timestamp,underlying_price,call_volume,put_volume,"
                            + "call_oi,put_oi,put_call_volume_ratio,put_call_oi_ratio,"
                            + "volume_oi_ratio,source)"
                            + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"),
            Map.entry("institutional_holdings",
                    "INSERT INTO institutional_holdings"
                            + " (filer_cik,filer_name,period,filing_date,form,accession_number,"
                            + "cusip,issuer,title,shares,share_type,value,source)"
                            + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"),
            Map.entry("symbol_catalog",
                    "INSERT INTO symbol_catalog (symbol,name,exchange,etf,cik,source)"
                            + " VALUES (?,?,?,?,?,?)"),
            Map.entry("pipeline_errors",
                    "INSERT INTO pipeline_errors (topic,error,raw) VALUES (?,?,?)"));

    private final String url;
    private final String user;
    private final String password;
    private final int batchSize;
    private final long flushIntervalMs;

    private transient Connection connection;
    private transient Map<String, PreparedStatement> statements;
    private transient List<ClickHouseRow> buffer;
    private transient ProcessingTimeService timerService;
    private transient ScheduledFuture<?> flushTimer;

    public ClickHouseSink(
            String url, String user, String password, int batchSize, long flushIntervalMs) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        connection = DriverManager.getConnection(url, user, password);
        statements = new HashMap<>();
        for (Map.Entry<String, String> entry : INSERT_SQL.entrySet()) {
            statements.put(entry.getKey(), connection.prepareStatement(entry.getValue()));
        }
        buffer = new ArrayList<>();
        if (getRuntimeContext() instanceof StreamingRuntimeContext) {
            timerService = ((StreamingRuntimeContext) getRuntimeContext())
                    .getProcessingTimeService();
            scheduleFlush();
        }
    }

    // invoke() only runs when a record arrives, so a timer bounds the wait for
    // low-rate topics instead of leaving rows until the next checkpoint.
    private void scheduleFlush() {
        if (timerService == null) {
            return;
        }
        flushTimer = timerService.registerTimer(
                timerService.getCurrentProcessingTime() + flushIntervalMs,
                this::flushOnTimer);
    }

    private void flushOnTimer(long timestamp) throws Exception {
        try {
            flush();
        } finally {
            scheduleFlush();
        }
    }

    @Override
    public void invoke(ClickHouseRow row, Context context) throws Exception {
        buffer.add(row);
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    private void flush() throws SQLException {
        if (buffer.isEmpty()) {
            return;
        }
        Map<String, List<ClickHouseRow>> byTable = new LinkedHashMap<>();
        for (ClickHouseRow row : buffer) {
            byTable.computeIfAbsent(row.table, key -> new ArrayList<>()).add(row);
        }
        for (Map.Entry<String, List<ClickHouseRow>> entry : byTable.entrySet()) {
            PreparedStatement statement = statements.get(entry.getKey());
            if (statement == null) {
                throw new SQLException("no insert statement for table " + entry.getKey());
            }
            for (ClickHouseRow row : entry.getValue()) {
                bind(statement, row.values);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        buffer.clear();
    }

    private static void bind(PreparedStatement statement, Object[] values) throws SQLException {
        int index = 1;
        for (Object value : values) {
            if (value == null) {
                statement.setNull(index++, Types.OTHER);
            } else if (value instanceof Timestamp) {
                statement.setTimestamp(index++, (Timestamp) value);
            } else if (value instanceof java.sql.Date) {
                statement.setDate(index++, (java.sql.Date) value);
            } else if (value instanceof Double) {
                statement.setDouble(index++, (Double) value);
            } else if (value instanceof Long) {
                statement.setLong(index++, (Long) value);
            } else {
                statement.setObject(index++, value);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
    }

    @Override
    public void close() throws Exception {
        if (flushTimer != null) {
            flushTimer.cancel(false);
            flushTimer = null;
        }
        timerService = null;
        if (connection == null) {
            return;
        }
        flush();
        for (PreparedStatement statement : statements.values()) {
            statement.close();
        }
        connection.close();
    }
}
