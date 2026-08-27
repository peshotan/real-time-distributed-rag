from __future__ import annotations

import argparse
import json
import logging
import os
import time
from collections.abc import Iterator
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from common.config import settings
from common.health import ProcessHealth, start_health_server
from common.models import RawEvent
from ingestion.live_sources import hn_stream, wikimedia_sse
from ingestion.synthetic_source import events as synthetic_events

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("ingestion")


def event_stream(source: str, rate: float) -> Iterator[dict[str, Any]]:
    if source == "synthetic":
        return synthetic_events(rate)
    if source == "wikimedia":
        return wikimedia_sse.events(float(os.getenv("LIVE_RECONNECT_SECONDS", "5")))
    if source == "hackernews":
        return hn_stream.events(float(os.getenv("HN_POLL_SECONDS", "5")))
    raise ValueError(f"Unsupported source: {source}")


def build_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
                acks="all",
                retries=8,
                retry_backoff_ms=500,
                request_timeout_ms=15_000,
                linger_ms=10,
                batch_size=32_768,
                key_serializer=lambda value: value.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode(
                    "utf-8"
                ),
            )
            producer.bootstrap_connected()
            return producer
        except (KafkaError, OSError) as exc:
            LOGGER.warning("Kafka not ready: %s; retrying", exc)
            time.sleep(3)


def run(source: str, rate: float, health: ProcessHealth) -> None:
    producer = build_producer()
    try:
        for raw in event_stream(source, rate):
            event = RawEvent.model_validate(raw)
            future = producer.send(
                settings.kafka_topic,
                key=event.event_id,
                value=event.model_dump(mode="json"),
            )
            future.get(timeout=30)
            producer.flush()
            health.record_success()
            LOGGER.info("published event_id=%s source=%s", event.event_id, event.source)
    finally:
        producer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish normalized events to Redpanda.")
    parser.add_argument(
        "--source",
        choices=("synthetic", "wikimedia", "hackernews"),
        default=os.getenv("SOURCE", "synthetic"),
    )
    parser.add_argument("--rate", type=float, default=float(os.getenv("EVENT_RATE", "5")))
    parser.add_argument("--health-port", type=int, default=int(os.getenv("INGESTION_HEALTH_PORT", "8001")))
    args = parser.parse_args()
    health = ProcessHealth("ingestion")
    start_health_server(health, args.health_port)
    while True:
        try:
            run(args.source, args.rate, health)
        except (KafkaError, OSError, ValueError) as exc:
            health.record_error(str(exc))
            LOGGER.exception("Producer loop failed; restarting")
            time.sleep(3)


if __name__ == "__main__":
    main()