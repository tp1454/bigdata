"""Abstract producer: shared Kafka publish/delivery-report logic."""

import json
import logging
from abc import ABC, abstractmethod

from confluent_kafka import Producer

from . import config

log = logging.getLogger(__name__)


class BaseProducer(ABC):
    def __init__(self, topic):
        self.topic = topic
        self._dropped = 0
        self.producer = Producer({
            "bootstrap.servers": config.KAFKA_BROKER,
            "queue.buffering.max.messages": 10000,
            "queue.buffering.max.kbytes": 65536,
        })

    def publish(self, key, value):
        record = dict(key=str(key), value=json.dumps(value),
                      callback=self._delivery_report)
        # One retry after polling; never block the websocket thread.
        for attempt in range(2):
            try:
                self.producer.produce(self.topic, **record)
                break
            except BufferError:
                if attempt == 0:
                    self.producer.poll(0.1)
                    continue
                self._dropped += 1
                if self._dropped == 1 or self._dropped % 1000 == 0:
                    log.warning("producer queue full; dropped %d records",
                                self._dropped)
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()

    @staticmethod
    def _delivery_report(err, msg):
        if err is not None:
            log.error("Delivery failed for %s: %s", msg.key(), err)
        else:
            log.debug("Delivered to %s[%d]", msg.topic(), msg.partition())

    @abstractmethod
    def run(self):
        """Produce records until interrupted."""
