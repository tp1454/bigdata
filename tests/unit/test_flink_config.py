import importlib.util
import os
from pathlib import Path

import pytest

os.environ.setdefault("CLICKHOUSE_PASSWORD", "test")

_CONFIG = Path(__file__).resolve().parents[2] / "flink-jobs" / "src" / "config.py"
_spec = importlib.util.spec_from_file_location("flink_config", _CONFIG)
flink_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(flink_config)


def test_parallelism_reads_the_env(monkeypatch):
    monkeypatch.setenv("FLINK_PARALLELISM", "4")
    assert flink_config.parallelism() == 4


def test_parallelism_defaults_and_rejects_zero(monkeypatch):
    monkeypatch.delenv("FLINK_PARALLELISM", raising=False)
    assert flink_config.parallelism() == 1
    monkeypatch.setenv("FLINK_PARALLELISM", "0")
    with pytest.raises(RuntimeError):
        flink_config.parallelism()


def test_topic_groups_cover_every_topic():
    grouped = [
        topic
        for topics in flink_config.TOPIC_GROUPS.values()
        for topic in topics
    ]
    assert sorted(grouped) == sorted(flink_config.KAFKA_TOPICS)


def test_job_identity_for_a_group(monkeypatch):
    monkeypatch.setenv("FLINK_GROUP", "crypto")
    assert flink_config.topics_for_group() == flink_config.TOPIC_GROUPS["crypto"]
    assert flink_config.job_name() == "Kafka_to_ClickHouse_crypto"
    assert flink_config.job_group_id() == (
        f"{flink_config.KAFKA_GROUP_ID}_crypto"
    )


def test_job_identity_defaults_to_one_all_topics_job(monkeypatch):
    monkeypatch.delenv("FLINK_GROUP", raising=False)
    assert flink_config.topics_for_group() == flink_config.KAFKA_TOPICS
    assert flink_config.job_name() == "Kafka_to_ClickHouse_Pipeline"
    assert flink_config.job_group_id() == flink_config.KAFKA_GROUP_ID


def test_unknown_group_is_rejected(monkeypatch):
    monkeypatch.setenv("FLINK_GROUP", "nope")
    with pytest.raises(RuntimeError):
        flink_config.job_name()
