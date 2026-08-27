# Real-Time Distributed RAG Pipeline

An end-to-end, local-first streaming RAG system. Events arrive through
Redpanda, worker nodes chunk and embed them with
`sentence-transformers/all-MiniLM-L6-v2`, Qdrant indexes the vectors, and a
FastAPI gateway retrieves context and synthesizes an answer. Redis provides a
cosine-similarity semantic cache, while a circuit breaker keeps the API useful
when the LLM is unavailable.

## Architecture highlights

- Kafka-compatible Redpanda broker with a durable `raw-events` topic.
- At-least-once worker processing with manual consumer-group offset commits.
- CPU-only local embeddings; no GPU, hosted vector database, or required API
  key.
- Qdrant cosine search with deterministic point IDs for idempotent retries.
- Redis semantic cache with a strict `> 0.92` cosine hit threshold.
- LLM adapters for `mock`, local Ollama, and OpenAI-compatible APIs.
- Extractive fallback responses and a half-open circuit breaker after LLM
  failures.
- Synthetic, Wikimedia Recent Changes SSE, and Hacker News streaming sources.

## Prerequisites

- Docker Engine 24+ with Docker Compose v2
- Python 3.10+
- 4 GB RAM available to Docker (the embedding model is loaded by gateway and
  worker containers)

## Quickstart

From the repository root:

```bash
cp .env.example .env
docker compose up --build -d
python scripts/test_pipeline.py
```

The first start downloads `all-MiniLM-L6-v2` inside the gateway and worker
containers when the runtime has access to Hugging Face. If the machine is
offline, the default `EMBEDDING_ALLOW_HASHING_FALLBACK=true` keeps the stack
operational with a clearly logged deterministic local hashing embedder; set it
to `false` in production when model availability must be enforced. The
integration test publishes a unique event, waits until it is searchable,
verifies retrieval, verifies a semantic-cache hit, and exercises the
extractive fallback.

Useful endpoints:

| Service | URL |
| --- | --- |
| Gateway Swagger UI | http://localhost:8000/docs |
| Gateway health | http://localhost:8000/health |
| Gateway metrics | http://localhost:8000/metrics |
| Qdrant dashboard/API | http://localhost:6333/dashboard |
| Qdrant health | http://localhost:6333/healthz |
| Redpanda Admin API | http://localhost:9644/v1/status/ready |
| Redis | localhost:6379 |

## Running the pipeline

### Synthetic mode

Synthetic mode is the default and is safe for demos and integration tests. It
generates realistic financial-news, technology, and operations events.

```bash
docker compose up -d ingestion
# Change rate without rebuilding:
EVENT_RATE=5 docker compose up -d --force-recreate ingestion
```

The producer accepts `SOURCE=synthetic` and `EVENT_RATE` from `.env`.

### Wikimedia live stream

Wikimedia's public Recent Changes SSE firehose needs no credentials:

```bash
SOURCE=wikimedia docker compose up -d --force-recreate ingestion
docker compose logs -f ingestion
```

The adapter normalizes edit metadata and revision comments into the common
event schema. Set `LIVE_RECONNECT_SECONDS` to control reconnect backoff.

### Hacker News live updates

Hacker News exposes an official Firebase updates stream. The adapter polls the
updates list, fetches each new story/comment, and emits normalized events:

```bash
SOURCE=hackernews docker compose up -d --force-recreate ingestion
docker compose logs -f ingestion
```

Set `HN_POLL_SECONDS` to control polling frequency. Public services can rate
limit; the adapter retries with exponential backoff and continues from the
latest observed IDs.

### Switching back to synthetic mode

```bash
SOURCE=synthetic docker compose up -d --force-recreate ingestion
```

## Verification

Health and metrics:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/metrics
curl http://localhost:6333/collections
docker compose ps
docker compose logs --tail=100 gateway worker ingestion
```

Publish a smoke-test event:

```bash
python scripts/seed_data.py
```

Query the gateway:

```bash
curl -s http://localhost:8000/query \
  -H 'content-type: application/json' \
  -d '{"query":"What is the latest synthetic event?","top_k":3}'
```

Redis cache behavior is visible in the response fields `cached` and
`cache_similarity`. Run the same request twice and expect the second response
to have `"cached": true` when the cache entry has not expired:

```bash
curl -s http://localhost:8000/query \
  -H 'content-type: application/json' \
  -d '{"query":"Which synthetic event was just published?","top_k":3}'
curl -s http://localhost:8000/query \
  -H 'content-type: application/json' \
  -d '{"query":"Which synthetic event was just published?","top_k":3}'
docker compose exec redis redis-cli --scan --pattern 'rag:semantic:*'
```

Exercise the fallback explicitly without changing infrastructure:

```bash
curl -s http://localhost:8000/query \
  -H 'content-type: application/json' \
  -H 'X-Simulate-LLM-Failure: true' \
  -d '{"query":"Summarize the latest event","top_k":3}'
```

The response contains `"fallback_mode": true`; fallback responses are not
cached.

## Configuration

All configuration is documented in `.env.example`. Copy it before first start.
The default `LLM_PROVIDER=mock` makes the stack runnable with zero credentials.
For Ollama, set `LLM_PROVIDER=ollama` and `OLLAMA_BASE_URL`, then make the model
available to Ollama. For an OpenAI-compatible service, set
`LLM_PROVIDER=openai_compatible`, `LLM_BASE_URL`, `LLM_MODEL`, and the secret in
`LLM_API_KEY` through your local environment or secret manager.

## Operations

```bash
docker compose ps
docker compose logs -f
docker compose restart gateway worker
docker compose down
docker compose down -v  # removes Redpanda, Qdrant, and Redis data
```

The worker group is named by `KAFKA_GROUP_ID`. To intentionally replay all
events in a fresh environment, use a new group ID or remove the Redpanda volume.

## Repository map

```text
gateway/                 FastAPI query API, cache, breaker, LLM clients
ingestion/               Source adapters and resilient Kafka producer
processing/              Chunking, embedding, Qdrant index worker
scripts/                 Seed and end-to-end verification scripts
docker-compose.yml       Local distributed infrastructure
DESIGN.md                Architecture, schemas, and reliability design
```

## License

MIT