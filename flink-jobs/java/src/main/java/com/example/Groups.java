package com.example;

import java.util.ArrayList;
import java.util.List;

/*
 * The submit script owns the group -> topics mapping (flink-jobs/src/config.py)
 * and passes this group's topics in FLINK_TOPICS, so the job never keeps a
 * second copy that can drift.
 */
final class Groups {
    private Groups() {
    }

    static List<String> topics() {
        String raw = System.getenv("FLINK_TOPICS");
        List<String> topics = new ArrayList<>();
        if (raw != null) {
            for (String topic : raw.split(",")) {
                if (!topic.isBlank()) {
                    topics.add(topic.trim());
                }
            }
        }
        if (topics.isEmpty()) {
            throw new IllegalStateException(
                    "FLINK_TOPICS is required; submit through the compose service");
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
