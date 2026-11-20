"""Abstract producer: shared Kafka publish/delivery-report logic."""

import json
import logging
import os
import time
from abc import ABC, abstractmethod

from confluent_kafka import Producer

from . import config

log = logging.getLogger(__name__)


class BaseProducer(ABC):
    def __init__(self, topic):
        self.topic = topic
        self._dropped = 0
        self.publish_timeout = float(os.getenv("PUBLISH_TIMEOUT", "5"))
        self.producer = Producer({
            "bootstrap.servers": config.KAFKA_BROKER,
            "acks": "all",
            "enable.idempotence": True,
            "queue.buffering.max.messages": 10000,
            "queue.buffering.max.kbytes": 65536,
        })

    def publish(self, key, value, weight=1):
        record = dict(key=str(key), value=json.dumps(value),
                      callback=self._delivery_report)
        # Wait for queue space; drop only after PUBLISH_TIMEOUT under
        # prolonged backpressure.
        deadline = time.monotonic() + self.publish_timeout
        while True:
            try:
                self.producer.produce(self.topic, **record)
                break
            except BufferError:
                if time.monotonic() >= deadline:
                    self._dropped += weight
                    if self._dropped == 1 or self._dropped % 1000 == 0:
                        log.warning("producer queue full; dropped %d records",
                                    self._dropped)
                    break
                self.producer.poll(0.1)
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()

    @staticmethod
    def _delivery_report(err, msg):
        if err is not None:
            log.error("Delivery failed for %s: %s", msg.key(), err)
        else:
            log.debug("Delivered to %s[%d]", msg.topic(), msg.partition())

    def poll_forever(self, fetch, interval, retry=None):
        """Run fetch() forever; retry sooner if it fails or returns False."""
        while True:
            try:
                ok = fetch()
            except Exception as e:
                log.error("poll failed: %s", e)
                ok = False
            self.flush()
            time.sleep(interval if ok else (retry or interval))

    @abstractmethod
    def run(self):
        """Produce records until interrupted."""
