"""Rate-controlled synthetic Kafka load for scaling tests.

Run through Compose:

    make load RATE=1000 DURATION=60 TOPIC=crypto.trades
"""

import argparse
import json
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from . import config
from .loadgen_data import equity_row, event_time, event_time_ms, trade_batch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--topic",
        default=config.TOPIC_CRYPTO,
        choices=[config.TOPIC_CRYPTO, config.TOPIC_EQUITIES],
    )
    parser.add_argument("--rate", type=float, default=500.0,
                        help="records per second (crypto: trades)")
    parser.add_argument("--duration", type=float, default=60.0,
                        help="seconds; 0 runs until interrupted")
    parser.add_argument("--batch-size", type=int, default=200,
                        help="trades per crypto message")
    parser.add_argument("--symbol", default=None)
    args = parser.parse_args()
    if args.rate <= 0 or args.batch_size <= 0:
        raise SystemExit("--rate and --batch-size must be greater than zero")

    if args.symbol:
        symbol = args.symbol
    elif args.topic == config.TOPIC_CRYPTO:
        symbol = config.crypto_symbols()[0]
    else:
        symbol = config.equity_symbols()[0]

    producer = Producer({
        "bootstrap.servers": config.KAFKA_BROKER,
        "linger.ms": 5,
        "compression.type": "lz4",
    })
    if args.topic == config.TOPIC_CRYPTO:
        start_ms = int(time.time() * 1000)
        next_id = 1
    else:
        base_ts = datetime.now(timezone.utc)
    price = 100.0
    sent = 0
    start = time.monotonic()

    try:
        while args.duration == 0 or time.monotonic() - start < args.duration:
            if args.topic == config.TOPIC_CRYPTO:
                payload = trade_batch(
                    symbol, next_id, args.batch_size,
                    event_time_ms(start_ms, sent, args.rate), price,
                )
                next_id += args.batch_size
                count = args.batch_size
            else:
                payload = equity_row(
                    symbol, event_time(base_ts, sent, args.rate), price
                )
                count = 1
            while True:
                try:
                    producer.produce(args.topic, key=symbol,
                                     value=json.dumps(payload))
                    break
                except BufferError:
                    producer.poll(0.1)
            sent += count
            producer.poll(0)
            target = sent / args.rate
            elapsed = time.monotonic() - start
            if elapsed < target:
                time.sleep(target - elapsed)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(30)

    elapsed = max(time.monotonic() - start, 1e-9)
    print(f"sent {sent} records in {elapsed:.1f}s "
          f"({sent / elapsed:.1f} records/s)")


if __name__ == "__main__":
    main()
