import sys

# Make the distributed pyFiles importable on the TaskManager.
sys.path.append('/opt/flink/usrlib/src')

import logging
from functools import partial

import clickhouse_connect
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetResetType,
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import CheckpointedFunction, MapFunction

import config
import transforms

log = logging.getLogger(__name__)


# ==============================================================================
# CLICKHOUSE SINK
# ==============================================================================
class ClickHouseSink(MapFunction, CheckpointedFunction):
    """Buffer rows per table and insert them in batches.

    Buffers are deliberately not part of Flink state: Kafka offsets are only
    committed when a checkpoint completes, so rows buffered after the last
    checkpoint are replayed by the source if the task fails. Flushing in
    snapshot_state() keeps the window between insert and offset commit aligned
    with the checkpoint, which is at-least-once delivery.
    """

    def __init__(self, batch_size=1000):
        self.client = None
        self.batch_size = batch_size
        self.columns_map = {table: cols for table, cols, _ in transforms.TOPICS.values()}
        self.buffers = {table: [] for table in self.columns_map}

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

        table_name, row_data = value
        if table_name not in self.buffers:
            log.warning("Dropping row for unknown table %s", table_name)
            return True

        self.buffers[table_name].append(row_data)
        if len(self.buffers[table_name]) >= self.batch_size:
            self.flush_table(table_name)
        return True

    def flush_table(self, table_name):
        rows = self.buffers[table_name]
        if not rows:
            return
        try:
            self.client.insert(
                table=table_name,
                data=rows,
                column_names=self.columns_map[table_name],
            )
        except Exception:
            # Fail the task instead of dropping rows: the checkpoint never
            # completes, so Kafka replays the batch on restart.
            log.exception("Insert into %s failed (%d rows); failing task to replay", table_name, len(rows))
            raise
        log.info("Inserted %d rows into %s", len(rows), table_name)
        self.buffers[table_name] = []

    def flush_all(self):
        for table_name in self.buffers:
            self.flush_table(table_name)

    def snapshot_state(self, context):
        self.flush_all()

    def initialize_state(self, context):
        pass

    def close(self):
        try:
            self.flush_all()
        except Exception:
            log.exception("Final flush failed; buffered rows will be replayed on restart")
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
                KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetType.EARLIEST)
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

    merged_stream.map(
        ClickHouseSink(batch_size=config.SINK_BATCH_SIZE),
        output_type=Types.BOOLEAN(),
    )

    log.info("Starting Kafka to ClickHouse job")
    env.execute("Kafka_to_ClickHouse_Pipeline")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()
