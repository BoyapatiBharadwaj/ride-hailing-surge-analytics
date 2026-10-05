"""Shared Kafka producer plumbing (confluent-kafka)."""
import json
import logging
import signal
import threading
import time
from typing import Callable, Optional

from confluent_kafka import Producer

from common.config import KAFKA_BOOTSTRAP

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")


def make_producer(client_id: str, **extra) -> Producer:
    """Reliability settings from PDF 9.1: acks=all + idempotence (topics have min.insync.replicas=2)."""
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        "compression.type": "lz4",
        "delivery.timeout.ms": 300000,   # ride out a broker restart (broker-kill drill)
        "retry.backoff.ms": 200,
        "queue.buffering.max.messages": 500000,
    }
    conf.update(extra)
    return Producer(conf)


class Stats:
    """Thread-safe delivery counters (delivered == acked by all in-sync replicas)."""

    def __init__(self):
        self.delivered = 0
        self.failed = 0
        self._lock = threading.Lock()

    def callback(self, err, msg):
        with self._lock:
            if err is None:
                self.delivered += 1
            else:
                self.failed += 1
                logging.getLogger("producer").warning("delivery failed: %s", err)


def send(producer: Producer, topic: str, key: str, event: dict, stats: Optional[Stats] = None) -> None:
    cb = stats.callback if stats else None
    while True:
        try:
            producer.produce(topic, key=key, value=json.dumps(event, separators=(",", ":")), on_delivery=cb)
            break
        except BufferError:        # local queue full -> wait for deliveries
            producer.poll(0.5)
    producer.poll(0)


class Stopper:
    """SIGTERM/SIGINT -> graceful flush."""

    def __init__(self):
        self.stop = False
        signal.signal(signal.SIGTERM, self._h)
        signal.signal(signal.SIGINT, self._h)

    def _h(self, *_):
        self.stop = True

    def sleep(self, seconds: float):
        end = time.monotonic() + seconds
        while not self.stop and time.monotonic() < end:
            time.sleep(min(0.5, max(0.0, end - time.monotonic())))


def retry(fn: Callable, tries: int = 5, base: float = 2.0, logger=None):
    """Exponential backoff for flaky external APIs (PDF risk table: retry with backoff)."""
    for i in range(tries):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            if logger:
                logger.warning("attempt %d/%d failed: %s", i + 1, tries, exc)
            time.sleep(base ** i)
    raise RuntimeError("all retries failed")
