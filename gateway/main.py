from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Header, HTTPException, Response
from qdrant_client import QdrantClient
from redis.asyncio import Redis

from common.config import settings
from common.embedding import EmbeddingModel
from common.models import (
    HealthResponse,
    MetricsResponse,
    QueryRequest,
    QueryResponse,
    SourceResult,
)
from gateway.circuit_breaker import BreakerState, CircuitBreaker
from gateway.semantic_cache import SemanticCache

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("gateway")

redis_client: Redis | None = None
qdrant_client: QdrantClient | None = None
embedder: EmbeddingModel | None = None
breaker = CircuitBreaker(settings.circuit_failure_threshold, settings.circuit_reset_seconds)
metrics = {
    "queries_total": 0,
    "cache_hits_total": 0,
    "fallback_total": 0,
    "llm_failures_total": 0,
    "indexed_chunks_total": 0,
}


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global redis_client, qdrant_client, embedder
    redis_client = Redis.from_url(settings.redis_url, decode_responses=False)
    qdrant_client = QdrantClient(url=settings.qdrant_url, timeout=10)
    # Load the model before reporting the gateway as ready, avoiding first-query latency.
    embedder = await asyncio.to_thread(EmbeddingModel)
    yield
    await redis_client.aclose()
    qdrant_client.close()


app = FastAPI(
    title="Real-Time Distributed RAG Gateway",
    version="1.0.0",
    description="Low-latency semantic retrieval over a streaming event index.",
    lifespan=lifespan,
)


async def llm_answer(prompt: str, simulate_failure: bool = False) -> str:
    if simulate_failure:
        raise TimeoutError("simulated LLM failure")
    if settings.llm_provider == "mock":
        return (
            "Mock synthesis grounded in the retrieved context: "
            + prompt.split("Context:", 1)[-1].strip()[:700]
        )
    timeout = httpx.Timeout(settings.llm_timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout) as client:
        if settings.llm_provider == "ollama":
            response = await client.post(
                f"{settings.ollama_base_url.rstrip('/')}/api/generate",
                json={"model": settings.llm_model, "prompt": prompt, "stream": False},
            )
            response.raise_for_status()
            body = response.json()
            return str(body["response"])
        if settings.llm_provider in {"openai", "openai_compatible"}:
            headers = {"authorization": f"Bearer {settings.llm_api_key}"} if settings.llm_api_key else {}
            response = await client.post(
                f"{settings.llm_base_url.rstrip('/')}/chat/completions",
                headers=headers,
                json={
                    "model": settings.llm_model,
                    "messages": [
                        {
                            "role": "system",
                            "content": "Answer only from the provided retrieved context. Be concise.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.1,
                },
            )
            response.raise_for_status()
            body = response.json()
            return str(body["choices"][0]["message"]["content"])
    raise ValueError(f"Unsupported LLM_PROVIDER={settings.llm_provider}")


def extractive_answer(sources: list[SourceResult]) -> str:
    if not sources:
        return "No matching context was found in the indexed event stream."
    excerpts = [f"{source.title}: {source.text}" for source in sources]
    return "LLM unavailable. Relevant indexed excerpts: " + " | ".join(excerpts)[:1800]


async def retrieve(vector: list[float], top_k: int) -> list[SourceResult]:
    if qdrant_client is None:
        raise RuntimeError("Qdrant client is not initialized")

    def search() -> list[Any]:
        return qdrant_client.search(
            collection_name=settings.qdrant_collection,
            query_vector=vector,
            limit=top_k,
            with_payload=True,
        )

    points = await asyncio.to_thread(search)
    sources: list[SourceResult] = []
    for point in points:
        payload = point.payload or {}
        try:
            sources.append(
                SourceResult(
                    event_id=str(payload["event_id"]),
                    chunk_id=str(payload["chunk_id"]),
                    source=str(payload["source"]),
                    title=str(payload.get("title", "")),
                    text=str(payload["text"]),
                    score=float(point.score),
                    timestamp=payload["timestamp"],
                    url=payload.get("url"),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            LOGGER.warning("Skipping malformed Qdrant payload: %s", exc)
    return sources


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    dependencies: dict[str, str] = {}
    if redis_client is None or qdrant_client is None or embedder is None:
        dependencies["gateway"] = "starting"
        return Response(
            content=HealthResponse(status="starting", dependencies=dependencies).model_dump_json(),
            status_code=503,
            media_type="application/json",
        )
    try:
        await redis_client.ping()
        dependencies["redis"] = "ok"
    except Exception as exc:
        dependencies["redis"] = f"error: {exc.__class__.__name__}"
    try:
        await asyncio.to_thread(qdrant_client.get_collections)
        dependencies["qdrant"] = "ok"
    except Exception as exc:
        dependencies["qdrant"] = f"error: {exc.__class__.__name__}"
    dependencies["embedding_model"] = "ok"
    ready = all(value == "ok" for value in dependencies.values())
    body = HealthResponse(status="ok" if ready else "degraded", dependencies=dependencies)
    if not ready:
        return Response(content=body.model_dump_json(), status_code=503, media_type="application/json")
    return body


@app.get("/metrics", response_model=MetricsResponse)
async def get_metrics() -> MetricsResponse:
    if qdrant_client is not None:
        try:
            indexed = await asyncio.to_thread(
                lambda: qdrant_client.count(settings.qdrant_collection, exact=True).count
            )
            metrics["indexed_chunks_total"] = int(indexed)
        except Exception:
            LOGGER.warning("Could not refresh indexed chunk count", exc_info=True)
    return MetricsResponse(**metrics)


@app.post("/query", response_model=QueryResponse)
async def query(
    request: QueryRequest,
    x_simulate_llm_failure: str | None = Header(default=None),
) -> QueryResponse:
    if embedder is None or redis_client is None:
        raise HTTPException(status_code=503, detail="Gateway is still starting")
    started = time.perf_counter()
    metrics["queries_total"] += 1
    vector = await asyncio.to_thread(embedder.encode, request.query)
    query_vector = vector[0]
    cache = SemanticCache(redis_client)
    simulate_llm_failure = (x_simulate_llm_failure or "").lower() == "true"
    # Explicit failure injection is a diagnostic path and must exercise the
    # breaker even when this query was previously cached.
    hit = None if simulate_llm_failure else await cache.lookup(query_vector)
    if hit:
        metrics["cache_hits_total"] += 1
        cached_response = QueryResponse.model_validate(hit.response)
        return cached_response.model_copy(
            update={
                "cached": True,
                "cache_similarity": hit.similarity,
                "latency_ms": round((time.perf_counter() - started) * 1000, 2),
            }
        )

    sources = await retrieve(query_vector, request.top_k)
    context = "\n".join(
        f"[{index + 1}] {source.title} ({source.source}): {source.text}"
        for index, source in enumerate(sources)
    )
    prompt = f"Question: {request.query}\nContext:\n{context or '[no retrieved context]'}"
    fallback = False
    state = await breaker.current_state()
    try:
        if not await breaker.allow():
            raise RuntimeError("LLM circuit breaker is open")
        answer = await llm_answer(
            prompt,
            simulate_failure=simulate_llm_failure,
        )
        await breaker.record_success()
    except Exception as exc:
        metrics["llm_failures_total"] += 1
        metrics["fallback_total"] += 1
        fallback = True
        await breaker.record_failure()
        LOGGER.warning("LLM synthesis failed state=%s error=%s", state, exc)
        answer = extractive_answer(sources)

    response = QueryResponse(
        answer=answer,
        query=request.query,
        sources=sources,
        cached=False,
        fallback_mode=fallback,
        latency_ms=round((time.perf_counter() - started) * 1000, 2),
        breaker_state=str(await breaker.current_state()),
    )
    if not fallback:
        await cache.put(request.query, query_vector, response.model_dump(mode="json"))
    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("gateway.main:app", host=settings.gateway_host, port=settings.gateway_port)