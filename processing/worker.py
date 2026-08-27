from __future__ import annotations

import logging
import os
import time
import uuid

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from qdrant_client import QdrantClient
from qdrant_client.http import models

from common.config import settings
from common.embedding import EmbeddingModel
from common.health import ProcessHealth, start_health_server
from common.models import RawEvent
from processing.chunker import TextChunk, chunk_event

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("worker")


def ensure_collection(client: QdrantClient) -> None:
    last_error: Exception | None = None
    for attempt in range(1, 11):
        try:
            client.get_collection(settings.qdrant_collection)
            return
        except Exception as exc:
            last_error = exc
            try:
                LOGGER.info("Creating Qdrant collection=%s", settings.qdrant_collection)
                client.create_collection(
                    collection_name=settings.qdrant_collection,
                    vectors_config=models.VectorParams(
                        size=settings.qdrant_vector_size,
                        distance=models.Distance.COSINE,
                    ),
                )
                return
            except Exception as create_error:
                last_error = create_error
                LOGGER.warning("Qdrant is not ready (attempt %s/10): %s", attempt, create_error)
                time.sleep(3)
    raise RuntimeError(f"Qdrant collection initialization failed: {last_error}")


def point_for_chunk(chunk: TextChunk, vector: list[float]) -> models.PointStruct:
    point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, chunk.chunk_id))
    return models.PointStruct(
        id=point_id,
        vector=vector,
        payload={
            "event_id": chunk.event_id,
            "chunk_id": chunk.chunk_id,
            "source": chunk.source,
            "title": chunk.title,
            "text": chunk.text,
            "timestamp": chunk.timestamp,
            "url": chunk.url,
            "metadata": chunk.metadata,
        },
    )


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        group_id=settings.kafka_group_id,
        enable_auto_commit=False,
        auto_offset_reset=settings.kafka_auto_offset_reset,
        consumer_timeout_ms=-1,
        max_poll_records=settings.kafka_batch_size,
        value_deserializer=lambda value: value.decode("utf-8"),
        key_deserializer=lambda value: value.decode("utf-8") if value else None,
    )


def run(health: ProcessHealth) -> None:
    client = QdrantClient(url=settings.qdrant_url, timeout=10)
    ensure_collection(client)
    embedder = EmbeddingModel()
    consumer = create_consumer()
    LOGGER.info(
        "worker started topic=%s group=%s embedding_dimension=%s",
        settings.kafka_topic,
        settings.kafka_group_id,
        embedder.dimension,
    )
    try:
        while True:
            records = consumer.poll(
                timeout_ms=settings.kafka_poll_timeout_ms,
                max_records=settings.kafka_batch_size,
            )
            messages = [message for partition in records.values() for message in partition]
            if not messages:
                continue
            chunks: list[TextChunk] = []
            for message in messages:
                event = RawEvent.model_validate_json(message.value)
                chunks.extend(chunk_event(event))
            if not chunks:
                consumer.commit()
                health.record_success(len(messages))
                continue
            vectors = embedder.encode([chunk.text for chunk in chunks])
            points = [
                point_for_chunk(chunk, vector) for chunk, vector in zip(chunks, vectors, strict=True)
            ]
            for offset in range(0, len(points), settings.qdrant_upsert_batch_size):
                client.upsert(
                    collection_name=settings.qdrant_collection,
                    points=points[offset : offset + settings.qdrant_upsert_batch_size],
                    wait=True,
                )
            # Acknowledge only after every point in the polled batch is durable.
            consumer.commit()
            health.record_success(len(messages))
            LOGGER.info("indexed events=%s chunks=%s", len(messages), len(points))
    finally:
        consumer.close()
        client.close()


def main() -> None:
    health = ProcessHealth("worker")
    start_health_server(health, int(os.getenv("WORKER_HEALTH_PORT", "8002")))
    while True:
        try:
            run(health)
        except (KafkaError, OSError, ValueError) as exc:
            health.record_error(str(exc))
            LOGGER.exception("Worker loop failed; retrying")
            time.sleep(3)


if __name__ == "__main__":
    main()