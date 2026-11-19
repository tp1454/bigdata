import sys

# Make the distributed pyFiles importable on the TaskManager.
sys.path.append('/opt/flink/usrlib/src')

import logging
from functools import partial

import clickhouse_connect
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction

import config
import transforms

log = logging.getLogger(__name__)


# ==============================================================================
# CLICKHOUSE SINK
# ==============================================================================
class ClickHouseSink(MapFunction):
    """Insert each row synchronously.

    PyFlink 1.18 exposes neither CheckpointedFunction nor a Python SinkWriter,
    so there is no hook that runs when a checkpoint is taken. A buffer would
    therefore still hold rows when Kafka offsets commit, and a later failure
    would lose them. Inserting per record keeps the write inside the record's
    processing: if the insert fails the task fails and the checkpoint never
    commits, so Kafka replays the batch (at-least-once).

    Server-side async_insert batches the small HTTP inserts into proper parts.
    """

    def __init__(self):
        self.client = None
        self.columns_map = {table: cols for table, cols, _ in transforms.TOPICS.values()}
        self.columns_map[transforms.ERROR_TABLE] = transforms.ERROR_COLUMNS

    def open(self, runtime_context):
        self.client = clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DB,
        )
        log.info("Connected to ClickHouse at %s:%s", config.CLICKHOUSE_HOST, config.CLICKHOUSE_PORT)

    def map(self, value):
        if value is None:
            return True

        table_name, row = value
        column_names = self.columns_map.get(table_name)
        if column_names is None:
            log.warning("Dropping row for unknown table %s", table_name)
            return True

        try:
            self.client.insert(
                table=table_name,
                data=row if isinstance(row, list) else [row],
                column_names=column_names,
                settings={"async_insert": 1, "wait_for_async_insert": 1},
            )
        except Exception:
            # Fail the task instead of dropping the row: without a completed
            # checkpoint Kafka replays it on restart.
            log.exception("Insert into %s failed; failing task to replay", table_name)
            raise
        return True

    def close(self):
        if self.client:
            self.client.close()


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # shortcut: checkpoint state lives in JobManager memory; add a durable
    # state backend when the job manager itself must survive restarts.
    env.enable_checkpointing(config.CHECKPOINT_INTERVAL_MS)

    all_streams = []

    for topic in config.KAFKA_TOPICS:
        log.info("Configuring Kafka source for topic: %s", topic)

        source = KafkaSource.builder() \
            .set_bootstrap_servers(config.KAFKA_BROKER) \
            .set_topics(topic) \
            .set_group_id(config.KAFKA_GROUP_ID) \
            .set_starting_offsets(
                KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)
            ) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        ds = env.from_source(
            source,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name=f"Kafka_{topic}",
        )

        parsed_ds = ds.flat_map(partial(transforms.parse_kafka_message, topic))
        all_streams.append(parsed_ds)

    if not all_streams:
        log.error("No topics configured. Aborting job.")
        return

    merged_stream = all_streams[0]
    for stream in all_streams[1:]:
        merged_stream = merged_stream.union(stream)

    merged_stream.map(ClickHouseSink(), output_type=Types.BOOLEAN())

    log.info("Starting Kafka to ClickHouse job")
    env.execute("Kafka_to_ClickHouse_Pipeline")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()
