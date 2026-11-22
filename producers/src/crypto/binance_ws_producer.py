"""Stream Binance trade events into the crypto topic."""

import logging
import os
import queue
import signal
import threading
import time

from binance import ThreadedWebsocketManager

from .. import config
from ..base_producer import BaseProducer
from .trade_batch import flush_due, grouped_trades

log = logging.getLogger(__name__)

SYMBOLS = config.crypto_symbols()
QUEUE_MAX = 10000
# python-binance's default (100) overflows on raw trade streams.
SOCKET_QUEUE_MAX = 10000


class BinanceProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_CRYPTO)
        self._q = queue.Queue(maxsize=QUEUE_MAX)
        self._worker = threading.Thread(target=self._drain, daemon=True)
        self._worker.start()
        self.bm = ThreadedWebsocketManager(max_queue_size=SOCKET_QUEUE_MAX)
        self.bm.start()

    def _handle_trade(self, msg):
        """Callback fired on every trade event from Binance."""
        if msg.get("e") == "error":
            log.error("binance socket error: %s", msg)
            # python-binance 1.0.37 never reconnects once its read loop closes
            # (queue overflow); exit so Docker restarts instead of spinning.
            if msg.get("type") == "ReadLoopClosed":
                os._exit(1)
            return
        if msg.get("e") != "aggTrade":
            return
        try:
            self._q.put_nowait(msg)
        except queue.Full:
            log.warning("dropping trade; queue full")

    def _publish_batch(self, batch):
        if not batch:
            return
        for symbol, trades in grouped_trades(batch).items():
            try:
                self.publish(symbol, {
                    "source": "binance",
                    "trades": trades,
                }, weight=len(trades))
            except Exception:
                log.exception("failed to publish trade batch for %s", symbol)

    def _drain(self):
        batch = []
        window_start = None
        while True:
            try:
                msg = self._q.get(timeout=0.2)
            except queue.Empty:
                msg = False
            if msg is None:
                self._publish_batch(batch)
                break
            if msg:
                if not batch:
                    window_start = time.time()
                batch.append(msg)
            now = time.time()
            if batch and flush_due(len(batch), now - window_start):
                self._publish_batch(batch)
                batch = []
                window_start = None
        self.flush()

    def _shutdown(self, signum, frame):
        self.bm.stop()

    def run(self):
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)
        for symbol in SYMBOLS:
            self.bm.start_aggtrade_socket(symbol=symbol, callback=self._handle_trade)
        self.bm.join()  # returns after _shutdown() stops the sockets
        self._q.put(None)
        self._worker.join(timeout=10)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
    BinanceProducer().run()
