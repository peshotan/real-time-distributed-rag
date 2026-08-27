from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "raw-events")
    kafka_group_id: str = os.getenv("KAFKA_GROUP_ID", "rag-workers")
    kafka_auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    kafka_poll_timeout_ms: int = _env_int("KAFKA_POLL_TIMEOUT_MS", 1000)
    kafka_batch_size: int = _env_int("KAFKA_BATCH_SIZE", 32)

    qdrant_url: str = os.getenv("QDRANT_URL", "http://qdrant:6333")
    qdrant_collection: str = os.getenv("QDRANT_COLLECTION", "rag_chunks")
    qdrant_vector_size: int = _env_int("QDRANT_VECTOR_SIZE", 384)
    qdrant_upsert_batch_size: int = _env_int("QDRANT_UPSERT_BATCH_SIZE", 64)

    redis_url: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    cache_ttl_seconds: int = _env_int("CACHE_TTL_SECONDS", 900)
    cache_similarity_threshold: float = _env_float("CACHE_SIMILARITY_THRESHOLD", 0.92)
    cache_max_entries_scanned: int = _env_int("CACHE_MAX_ENTRIES_SCANNED", 500)

    embedding_model: str = os.getenv(
        "EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"
    )
    embedding_device: str = os.getenv("EMBEDDING_DEVICE", "cpu")
    embedding_batch_size: int = _env_int("EMBEDDING_BATCH_SIZE", 32)
    embedding_allow_hashing_fallback: bool = (
        os.getenv("EMBEDDING_ALLOW_HASHING_FALLBACK", "true").lower() == "true"
    )
    chunk_size_words: int = _env_int("CHUNK_SIZE_WORDS", 180)
    chunk_overlap_words: int = _env_int("CHUNK_OVERLAP_WORDS", 35)

    llm_provider: str = os.getenv("LLM_PROVIDER", "mock").lower()
    llm_base_url: str = os.getenv("LLM_BASE_URL", "http://host.docker.internal:11434/v1")
    ollama_base_url: str = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
    llm_model: str = os.getenv("LLM_MODEL", "llama3.2")
    llm_api_key: str = os.getenv("LLM_API_KEY", "")
    llm_timeout_seconds: float = _env_float("LLM_TIMEOUT_SECONDS", 2.5)
    circuit_failure_threshold: int = _env_int("CIRCUIT_FAILURE_THRESHOLD", 3)
    circuit_reset_seconds: float = _env_float("CIRCUIT_RESET_SECONDS", 15)

    gateway_host: str = os.getenv("GATEWAY_HOST", "0.0.0.0")
    gateway_port: int = _env_int("GATEWAY_PORT", 8000)


settings = Settings()