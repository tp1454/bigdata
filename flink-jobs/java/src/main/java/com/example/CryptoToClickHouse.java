package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class CryptoToClickHouse {
    private static final String[] TOPICS = {
        "crypto.trades", "crypto.ohlcv", "crypto.market"
    };

    public static void main(String[] args) throws Exception {
        String brokers = getenv("KAFKA_BROKER", "kafka-1:29092,kafka-2:29092,kafka-3:29092");
        String groupId = getenv("KAFKA_GROUP_ID", "flink_consumer_group") + "_crypto";

        String jdbcUrl = "jdbc:clickhouse://" + getenv("CLICKHOUSE_HOST", "clickhouse")
                + ":8123/" + getenv("CLICKHOUSE_DB", "analytics");
        String clickhouseUser = getenv("CLICKHOUSE_USER", "default");
        String clickhousePassword = getenv("CLICKHOUSE_PASSWORD", "");

        int parallelism = Integer.parseInt(getenv("FLINK_PARALLELISM", "1"));
        long checkpointInterval = Long.parseLong(getenv("CHECKPOINT_INTERVAL_MS", "60000"));
        long checkpointTimeout = Long.parseLong(getenv("CHECKPOINT_TIMEOUT_MS", "600000"));
        int batchSize = Integer.parseInt(getenv("CLICKHOUSE_BATCH_SIZE", "10000"));
        long flushInterval = Long.parseLong(getenv("CLICKHOUSE_FLUSH_MS", "1000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);

        List<DataStream<ClickHouseRow>> streams = new ArrayList<>();
        for (String topic : TOPICS) {
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topic)
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.committedOffsets(
                            OffsetResetStrategy.EARLIEST))
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            DataStream<String> raw = env.fromSource(
                    source, WatermarkStrategy.noWatermarks(), "Kafka_" + topic);
            streams.add(raw.process(new CryptoParser(topic)));
        }

        DataStream<ClickHouseRow> rows = streams.get(0);
        for (int i = 1; i < streams.size(); i++) {
            rows = rows.union(streams.get(i));
        }

        rows.addSink(new ClickHouseSink(
                        jdbcUrl, clickhouseUser, clickhousePassword, batchSize, flushInterval))
                .name("ClickHouse")
                .setParallelism(parallelism);

        env.execute("Kafka_to_ClickHouse_crypto");
    }

    private static String getenv(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
