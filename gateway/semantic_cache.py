from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis

from common.config import settings
from common.embedding import cosine_similarity


@dataclass(frozen=True)
class CacheHit:
    response: dict[str, Any]
    similarity: float


def normalize_query(query: str) -> str:
    return " ".join(query.lower().split())


class SemanticCache:
    def __init__(self, client: Redis) -> None:
        self.client = client

    async def lookup(self, vector: list[float]) -> CacheHit | None:
        scanned = 0
        async for key in self.client.scan_iter(match="rag:semantic:*", count=100):
            if scanned >= settings.cache_max_entries_scanned:
                break
            scanned += 1
            raw = await self.client.get(key)
            if not raw:
                continue
            try:
                entry = json.loads(raw)
                similarity = cosine_similarity(vector, entry["vector"])
                if similarity > settings.cache_similarity_threshold:
                    return CacheHit(entry["response"], similarity)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                # A malformed/expired cache entry is not allowed to break queries.
                continue
        return None

    async def put(self, query: str, vector: list[float], response: dict[str, Any]) -> None:
        key = f"rag:semantic:{uuid.uuid4()}"
        entry = {
            "query": normalize_query(query),
            "vector": vector,
            "response": response,
            "created_at": time.time(),
        }
        await self.client.set(
            key,
            json.dumps(entry, separators=(",", ":")),
            ex=settings.cache_ttl_seconds,
        )