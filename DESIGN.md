# Real-Time Distributed RAG Pipeline Design

## 1. Architecture and data flow

The pipeline is intentionally local-first: embeddings run on CPU, all stateful
services run in Docker Compose, and the gateway can synthesize with Ollama,
another OpenAI-compatible endpoint, or the built-in deterministic mock.

```text
 ┌────────────────────┐       ┌──────────────────┐
 │ Synthetic source   │       │ Wikimedia SSE    │
 │ Hacker News stream │       │ / HN updates     │
 └─────────┬──────────┘       └────────┬─────────┘
           │                            │
           └──────────────┬─────────────┘
                          │ normalized Event JSON
                          v
                 ┌────────────────────┐
                 │ Redpanda           │
                 │ topic: raw-events  │
                 │ consumer offsets   │
                 └─────────┬──────────┘
                           │ consumer group: rag-workers
                           v
                 ┌────────────────────┐
                 │ Worker nodes       │
                 │ chunk + embed CPU  │
                 │ manual offset ack  │
                 └─────────┬──────────┘
                           │ batched vectors + payloads
                           v
                 ┌────────────────────┐
                 │ Qdrant             │
                 │ cosine collection  │
                 └─────────┬──────────┘
                           │ top-k chunks
                           v
 ┌──────────────┐   query  │  context
 │ Client       ├──────────┴───────────────┐
 └──────────────┘                          v
                                  ┌──────────────────┐
                                  │ FastAPI Gateway  │
                                  │ route + timeout  │
                                  └───────┬──────────┘
                                          │
                         ┌────────────────┴─────────────────┐
                         v                                  v
                 ┌──────────────────┐             ┌──────────────────┐
                 │ Redis semantic   │ cache hit   │ LLM provider    │
                 │ cosine lookup    ├────────────>│ Ollama / OpenAI  │
                 └──────────────────┘             │ / mock           │
                                                  └────────┬─────────┘
                                                           │
                                                           v
                                               answer + sources + metadata
```

### Request path

1. The gateway validates a request and embeds the query with the same
   `all-MiniLM-L6-v2` model used by workers.
2. Redis scans the bounded semantic cache and computes cosine similarity in
   application code. A score strictly greater than `CACHE_SIMILARITY_THRESHOLD`
   (default `0.92`) returns the previous response without touching Qdrant or
   the LLM.
3. A cache miss searches Qdrant using cosine distance and includes the top
   chunks in the synthesis prompt.
4. The LLM call is protected by a timeout and circuit breaker. A failure,
   timeout, or open breaker returns an extractive answer assembled from the
   retrieved chunks and marks `fallback_mode: true`.
5. Successful answers are cached with their query vector and a short TTL.

## 2. Components and responsibilities

### Redpanda

- Kafka-compatible broker with no ZooKeeper dependency.
- Owns the durable `raw-events` log.
- Partitions are selected by `event_id` so retries stay deterministic.
- Consumer offsets are stored in the broker under the `rag-workers` group.

### Worker nodes

- `processing/worker.py` consumes `raw-events`.
- `processing/chunker.py` splits long text into overlapping word windows and
  embeds chunks locally with Sentence Transformers.
- Workers initialize the Qdrant collection if needed and upsert vectors in
  configurable batches.
- Offsets are committed only after a batch is successfully indexed. Multiple
  worker replicas can therefore share the `rag-workers` group safely.

### Qdrant

- Stores the vector, chunk text, and event metadata.
- Uses a 384-dimensional cosine collection for `all-MiniLM-L6-v2`.
- Point IDs are deterministic UUID5 values derived from event and chunk IDs,
  making worker retries idempotent.

### Redis

- Stores complete successful gateway responses alongside their normalized query
  vectors.
- Cache entries have a TTL and a configurable maximum scan count.
- The gateway computes normalized dot products (cosine similarity) and never
  treats a text-only exact match as semantic equivalence.

### FastAPI Gateway

- Exposes `/query`, `/health`, and `/metrics`.
- Routes query embedding, semantic-cache lookup, Qdrant retrieval, and LLM
  synthesis.
- Enforces request size and top-k limits.
- Provides a circuit breaker around the LLM so retrieval remains useful while a
  provider is degraded.

## 3. Data schemas

### Kafka event (`raw-events` value)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "RawEvent",
  "type": "object",
  "required": ["event_id", "timestamp", "source", "title", "content"],
  "properties": {
    "event_id": {"type": "string", "minLength": 1},
    "timestamp": {"type": "string", "format": "date-time"},
    "source": {"type": "string", "minLength": 1},
    "title": {"type": "string"},
    "content": {"type": "string"},
    "url": {"type": ["string", "null"], "format": "uri"},
    "metadata": {"type": "object", "additionalProperties": true}
  },
  "additionalProperties": false
}
```

### Qdrant payload

```json
{
  "event_id": "event-123",
  "chunk_id": "event-123:0",
  "source": "synthetic",
  "title": "Example title",
  "text": "The chunk text used for retrieval.",
  "timestamp": "2026-08-27T20:00:00Z",
  "url": null,
  "metadata": {}
}
```

Vector shape: an array of 384 IEEE-754 floats, normalized by the embedding
model. The collection distance is `Cosine`.

### `POST /query` request

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "QueryRequest",
  "type": "object",
  "required": ["query"],
  "properties": {
    "query": {"type": "string", "minLength": 2, "maxLength": 2000},
    "top_k": {"type": "integer", "minimum": 1, "maximum": 20, "default": 3}
  },
  "additionalProperties": false
}
```

### `POST /query` response

```json
{
  "answer": "A grounded answer based on the retrieved chunks.",
  "query": "What happened?",
  "sources": [
    {
      "event_id": "event-123",
      "chunk_id": "event-123:0",
      "source": "synthetic",
      "title": "Example title",
      "text": "The chunk text used for retrieval.",
      "score": 0.9412,
      "timestamp": "2026-08-27T20:00:00Z",
      "url": null
    }
  ],
  "cached": false,
  "fallback_mode": false,
  "latency_ms": 48.2,
  "cache_similarity": null,
  "breaker_state": "closed"
}
```

## 4. Latency and fault tolerance

### Latency budget

- Query embedding: local CPU model, reused as a process singleton.
- Redis cache: one bounded `SCAN` pass, configurable by
  `CACHE_MAX_ENTRIES_SCANNED`; vectors are normalized before storage.
- Qdrant: one nearest-neighbor search with `top_k` bounded at 20.
- LLM: hard timeout at `LLM_TIMEOUT_SECONDS` (default 2.5 seconds).
- The response includes `latency_ms` so the test script and operators can
  measure the end-to-end path.

### Circuit breaker

The breaker starts `closed`. Consecutive LLM failures increment the failure
counter. After `CIRCUIT_FAILURE_THRESHOLD` failures it becomes `open` and
short-circuits new LLM calls for `CIRCUIT_RESET_SECONDS`. The first request
after the reset window becomes `half_open`; one success closes the breaker and
one failure reopens it. Every path returns an extractive response when LLM
generation is unavailable.

### Redis cosine cache

Each cache key contains:

```json
{
  "query": "normalized query text",
  "vector": [0.01, -0.02],
  "response": {"answer": "...", "sources": []},
  "created_at": "2026-08-27T20:00:00Z"
}
```

The gateway normalizes whitespace and lowercases for the stored query label,
but compares the embedding vectors, not the strings. It computes
`dot(normalized_query_vector, normalized_cached_vector)` and accepts only
scores strictly above `0.92`. Cache misses never write partial or fallback
responses, which prevents an upstream outage from poisoning the cache.

### Kafka offset management

Workers use `enable_auto_commit=false`. A batch is acknowledged as follows:

1. Poll records into memory.
2. Chunk and embed the complete batch.
3. Upsert all points into Qdrant.
4. Commit the highest processed offset per partition.

If the worker exits between steps 3 and 4, records are replayed and overwrite
the same deterministic point IDs. If Qdrant is unavailable, no offsets are
committed, preserving at-least-once delivery. `KAFKA_AUTO_OFFSET_RESET`
defaults to `earliest` for a new environment and can be changed to `latest`.

### Service health

Docker health checks gate startup on Redpanda, Qdrant, and Redis. Gateway
health is dependency-aware and reports `503` when required dependencies are
unavailable. Ingestion and worker processes expose a small health endpoint with
their latest activity so Compose can restart a wedged process.