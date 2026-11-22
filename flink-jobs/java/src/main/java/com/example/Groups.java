package com.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class Groups {
    private static final Map<String, List<String>> TOPIC_GROUPS = Map.of(
            "equities", List.of("equities.prices", "options.metrics"),
            "crypto", List.of("crypto.trades", "crypto.ohlcv", "crypto.market"),
            "fundamentals", List.of(
                    "fundamentals.filings", "fundamentals.actions",
                    "fundamentals.statements", "fundamentals.insider",
                    "institutional.holdings", "reference.symbols"),
            "macro", List.of("forex.rates", "macro.indicators"));

    private Groups() {
    }

    static List<String> topics(String group) {
        if (group == null || group.isBlank()) {
            List<String> all = new ArrayList<>();
            for (List<String> topics : TOPIC_GROUPS.values()) {
                all.addAll(topics);
            }
            return all;
        }
        List<String> topics = TOPIC_GROUPS.get(group);
        if (topics == null) {
            throw new IllegalArgumentException("unknown FLINK_GROUP " + group);
        }
        return topics;
    }

    static String jobName(String group) {
        return group == null || group.isBlank()
                ? "Kafka_to_ClickHouse"
                : "Kafka_to_ClickHouse_" + group;
    }

    static String groupId(String group) {
        String base = System.getenv().getOrDefault("KAFKA_GROUP_ID", "flink_consumer_group");
        return group == null || group.isBlank() ? base : base + "_" + group;
    }
}
