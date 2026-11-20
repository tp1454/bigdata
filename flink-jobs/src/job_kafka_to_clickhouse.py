import sys

# Make the distributed pyFiles importable on the TaskManager.
sys.path.append('/opt/flink/usrlib/src')

import logging
from functools import partial

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import DataTypes
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.table import StreamTableEnvironment

import config
import tables
import transforms

log = logging.getLogger(__name__)


def _flink_type(sql_type):
    types = {
        "STRING": DataTypes.STRING(),
        "TIMESTAMP(3)": DataTypes.TIMESTAMP(3),
        "DOUBLE": DataTypes.DOUBLE(),
        "BIGINT": DataTypes.BIGINT(),
        "DATE": DataTypes.DATE(),
    }
    if sql_type not in types:
        raise ValueError(f"Unsupported sink type {sql_type}")
    return types[sql_type]


def _expand(item):
    table_name, row = item
    if isinstance(row, list):
        for value in row:
            yield (table_name, value)
    else:
        yield (table_name, row)


def _row(item):
    return item[1]


def _sink_ddl(table_name):
    return tables.sink_ddl(
        table_name,
        config.CLICKHOUSE_HOST,
        config.CLICKHOUSE_PORT,
        config.CLICKHOUSE_DB,
        config.CLICKHOUSE_USER,
        config.CLICKHOUSE_PASSWORD,
    )


def _add_sink(t_env, statement_set, table_name, rows):
    spec = tables.TABLES[table_name]
    source = t_env.from_data_stream(
        rows,
        Types.ROW_NAMED(
            spec["columns"],
            [_flink_type(sql_type) for sql_type in spec["types"]],
        ),
    )
    t_env.execute_sql(_sink_ddl(table_name))
    statement_set.add_insert(f"sink_{table_name}", source)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.parallelism())
    env.enable_checkpointing(config.CHECKPOINT_INTERVAL_MS)
    t_env = StreamTableEnvironment.create(env)
    statement_set = t_env.create_statement_set()

    topics = config.topics_for_group()
    error_streams = []

    for topic in topics:
        log.info("Configuring Kafka source for topic: %s", topic)

        source = KafkaSource.builder() \
            .set_bootstrap_servers(config.KAFKA_BROKER) \
            .set_topics(topic) \
            .set_group_id(config.job_group_id()) \
            .set_starting_offsets(
                KafkaOffsetsInitializer.committed_offsets(
                    KafkaOffsetResetStrategy.EARLIEST
                )
            ) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        ds = env.from_source(
            source,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name=f"Kafka_{topic}",
        )

        parsed = ds.flat_map(
            partial(transforms.parse_kafka_message, topic)
        ).flat_map(_expand)

        table_name, _, _ = transforms.TOPICS[topic]
        target = parsed.filter(lambda x: x[0] == table_name).map(_row)
        _add_sink(t_env, statement_set, table_name, target)

        errors = parsed.filter(
            lambda x: x[0] == transforms.ERROR_TABLE
        ).map(_row)
        error_streams.append(errors)

    if error_streams:
        errors = error_streams[0]
        for stream in error_streams[1:]:
            errors = errors.union(stream)
        _add_sink(t_env, statement_set, transforms.ERROR_TABLE, errors)

    log.info("Starting %s", config.job_name())
    statement_set.execute(config.job_name())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()
