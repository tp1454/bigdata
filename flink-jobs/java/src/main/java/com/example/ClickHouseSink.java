package com.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

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

public class ClickHouseSink extends RichSinkFunction<ClickHouseRow>
        implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;

    private static final Map<String, String> INSERT_SQL = Map.of(
            "crypto_trades",
            "INSERT INTO crypto_trades (symbol,timestamp,price,quantity,trade_id,source)"
                    + " VALUES (?,?,?,?,?,?)",
            "crypto_ohlcv",
            "INSERT INTO crypto_ohlcv"
                    + " (symbol,interval,timestamp,open,high,low,close,volume,source)"
                    + " VALUES (?,?,?,?,?,?,?,?,?)",
            "crypto_market_data",
            "INSERT INTO crypto_market_data"
                    + " (symbol,timestamp,price,market_cap,volume_24h,source)"
                    + " VALUES (?,?,?,?,?,?)",
            "pipeline_errors",
            "INSERT INTO pipeline_errors (topic,error,raw) VALUES (?,?,?)");

    private final String url;
    private final String user;
    private final String password;
    private final int batchSize;
    private final long flushIntervalMs;

    private transient Connection connection;
    private transient Map<String, PreparedStatement> statements;
    private transient List<ClickHouseRow> buffer;
    private transient long lastFlush;

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
        lastFlush = System.currentTimeMillis();
    }

    @Override
    public void invoke(ClickHouseRow row, Context context) throws Exception {
        buffer.add(row);
        if (buffer.size() >= batchSize
                || System.currentTimeMillis() - lastFlush >= flushIntervalMs) {
            flush();
        }
    }

    private void flush() throws SQLException {
        if (buffer.isEmpty()) {
            lastFlush = System.currentTimeMillis();
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
        lastFlush = System.currentTimeMillis();
    }

    private static void bind(PreparedStatement statement, Object[] values) throws SQLException {
        int index = 1;
        for (Object value : values) {
            if (value == null) {
                statement.setNull(index++, Types.OTHER);
            } else if (value instanceof Timestamp) {
                statement.setTimestamp(index++, (Timestamp) value);
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
