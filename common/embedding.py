from __future__ import annotations

import hashlib
import logging
import re
import threading
from typing import Sequence

import numpy as np
from sentence_transformers import SentenceTransformer

from common.config import settings

LOGGER = logging.getLogger(__name__)
_TOKEN_RE = re.compile(r"[a-z0-9]+")


class EmbeddingModel:
    """Process-local singleton around the CPU embedding model."""

    _instance: "EmbeddingModel | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "EmbeddingModel":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    try:
                        instance._model: SentenceTransformer | None = SentenceTransformer(
                            settings.embedding_model, device=settings.embedding_device
                        )
                        instance._fallback = False
                    except Exception as exc:
                        if not settings.embedding_allow_hashing_fallback:
                            raise
                        instance._model = None
                        instance._fallback = True
                        LOGGER.warning(
                            "Embedding model unavailable (%s); using explicit offline "
                            "hashing fallback with dimension=%s",
                            exc.__class__.__name__,
                            settings.qdrant_vector_size,
                        )
                    cls._instance = instance
        return cls._instance

    def encode(self, texts: str | Sequence[str]) -> list[list[float]]:
        values = [texts] if isinstance(texts, str) else list(texts)
        if not values:
            return []
        if self._fallback:
            return [self._hash_encode(value) for value in values]
        assert self._model is not None
        vectors = self._model.encode(
            values,
            batch_size=settings.embedding_batch_size,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        array = np.atleast_2d(vectors).astype(np.float32)
        return array.tolist()

    @property
    def dimension(self) -> int:
        if self._fallback:
            return settings.qdrant_vector_size
        assert self._model is not None
        return int(self._model.get_sentence_embedding_dimension())

    @staticmethod
    def _hash_encode(text: str) -> list[float]:
        """Offline deterministic embedding for air-gapped smoke tests."""
        dimension = settings.qdrant_vector_size
        vector = np.zeros(dimension, dtype=np.float32)
        tokens = _TOKEN_RE.findall(text.lower())
        features = tokens + [f"{left}:{right}" for left, right in zip(tokens, tokens[1:])]
        for feature in features:
            digest = hashlib.blake2b(feature.encode(), digest_size=8).digest()
            index = int.from_bytes(digest[:4], "little") % dimension
            vector[index] += 1.0 if digest[4] & 1 else -1.0
        norm = np.linalg.norm(vector)
        if norm:
            vector /= norm
        return vector.tolist()


def cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    left_array = np.asarray(left, dtype=np.float32)
    right_array = np.asarray(right, dtype=np.float32)
    left_norm = np.linalg.norm(left_array)
    right_norm = np.linalg.norm(right_array)
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return float(np.dot(left_array, right_array) / (left_norm * right_norm))