# Real-Time Distributed RAG Pipeline

An end-to-end, local-first streaming RAG system. Events arrive through
Redpanda, worker nodes chunk and embed them with
`sentence-transformers/all-MiniLM-L6-v2`, Qdrant indexes the vectors, and a
FastAPI gateway retrieves context and synthesizes an answer. Redis provides a
cosine-similarity semantic cache, while a circuit breaker keeps the API useful
when the LLM is unavailable.

## Project purpose

This project demonstrates a production-oriented, locally runnable real-time
Retrieval-Augmented Generation (RAG) platform. It turns continuously arriving
events into searchable knowledge, then answers questions against the most
recent indexed content without requiring a hosted vector database, a GPU, or a
mandatory paid API.

The design prioritizes operational correctness as much as answer quality:

- Streaming ingestion is durable and Kafka-compatible.
- Indexing is at-least-once and safe to retry because offsets are committed
  only after successful Qdrant writes.
- Embeddings run locally on CPU using
  `sentence-transformers/all-MiniLM-L6-v2`.
- Repeated or semantically similar questions can bypass retrieval and LLM
  generation through Redis.
- LLM outages, slow responses, and open circuit-breakers still produce a
  useful extractive response from retrieved source text.

  ## Detailed architecture

  ```mermaid
  flowchart TB
      subgraph INGESTION["1. Streaming ingestion"]
          SYN["Synthetic event generator<br/>financial, technology, operations"]
          WIKI["Wikimedia Recent Changes<br/>SSE adapter"]
          HN["Hacker News updates<br/>Firebase polling adapter"]
          PRODUCER["Resilient ingestion producer<br/>normalize to RawEvent JSON<br/>key messages by event_id"]

          SYN --> PRODUCER
          WIKI --> PRODUCER
          HN --> PRODUCER
      end

      BROKER["Redpanda broker<br/>Kafka-compatible<br/>topic: raw-events<br/>durable log + consumer offsets"]
      PRODUCER -->|"publish normalized events"| BROKER

      subgraph INDEXING["2. Asynchronous indexing"]
          WORKER["Qdrant indexing worker<br/>consumer group: rag-workers<br/>auto-commit disabled"]
          CHUNK["Overlapping chunker<br/>word-window segmentation"]
          DOC_EMBED["CPU document embedder<br/>all-MiniLM-L6-v2<br/>384-dimensional vectors"]
          UPSERT["Batch Qdrant upsert<br/>deterministic UUID5 point IDs"]

          WORKER --> CHUNK --> DOC_EMBED --> UPSERT
      end

      BROKER -->|"poll events"| WORKER
      UPSERT -->|"write vectors and payloads"| QDRANT["Qdrant vector database<br/>Cosine collection<br/>text + metadata payloads"]
      UPSERT -.->|"commit offsets only after Qdrant succeeds"| BROKER

      CLIENT["API client"] --> GATEWAY

      subgraph QUERY["3. Synchronous query path"]
          GATEWAY["FastAPI gateway<br/>validate request + orchestrate"]
          QUERY_EMBED["CPU query embedder<br/>same model as indexing"]
          CACHE["Redis semantic cache<br/>bounded vector scan<br/>strict similarity > 0.92"]
          SEARCH["Qdrant top-k search"]
          CONTEXT["Context assembly<br/>retrieved chunks + source metadata"]
          BREAKER["LLM circuit breaker<br/>closed → open → half-open"]
          LLM["LLM adapter<br/>Ollama / OpenAI-compatible / mock<br/>timeout: 2.5 seconds"]
          FALLBACK["Extractive fallback<br/>answer from retrieved chunks"]
          RESPONSE["Response<br/>answer + sources + latency<br/>cache and fallback metadata"]
          CACHE_WRITE["Cache successful response<br/>TTL + query vector"]

          GATEWAY --> QUERY_EMBED --> CACHE
          CACHE -->|"hit: return cached response"| RESPONSE
          CACHE -->|"miss"| SEARCH
          SEARCH -->|"nearest-neighbor query"| QDRANT
          QDRANT -->|"top-k chunks"| CONTEXT
          CONTEXT --> BREAKER
          BREAKER -->|"healthy"| LLM
          LLM -->|"successful synthesis"| RESPONSE
          BREAKER -->|"open, timeout, or failure"| FALLBACK
          FALLBACK -->|"fallback_mode = true<br/>not cached"| RESPONSE
          RESPONSE -->|"successful LLM response only"| CACHE_WRITE
          CACHE_WRITE --> CACHE
      end

      subgraph PROVIDERS["LLM provider options"]
          OLLAMA["Local Ollama"]
          OPENAI["OpenAI-compatible API"]
          MOCK["Deterministic mock"]
      end

      LLM --- OLLAMA
      LLM --- OPENAI
      LLM --- MOCK

      HEALTH["Operational visibility<br/>Docker health checks<br/>/health · /metrics"]
      HEALTH -.-> BROKER
      HEALTH -.-> QDRANT
      HEALTH -.-> CACHE
      HEALTH -.-> GATEWAY
      HEALTH -.-> WORKER
      HEALTH -.-> PRODUCER

      classDef source fill:#e8f3ff,stroke:#2878c8,color:#102a43
      classDef durable fill:#fff1d6,stroke:#c77d00,color:#4a2c00
      classDef compute fill:#e9f8ef,stroke:#2f855a,color:#173b29
      classDef query fill:#f3eaff,stroke:#7b4ab5,color:#2d1747
      classDef resilience fill:#ffe9e9,stroke:#c53030,color:#4a1515

      class SYN,WIKI,HN,PRODUCER source
      class BROKER,QDRANT,CACHE durable
      class WORKER,CHUNK,DOC_EMBED,UPSERT,QUERY_EMBED,SEARCH,CONTEXT compute
      class GATEWAY,RESPONSE,LLM,OLLAMA,OPENAI,MOCK query
      class BREAKER,FALLBACK resilience
  ```

  The system has two complementary flows. The ingestion/indexing flow is
  asynchronous and optimized for durable throughput. The query flow is
  synchronous and optimized for low latency: it checks the semantic cache first,
  retrieves from Qdrant on a miss, and then calls the configured LLM only after
  relevant context has been assembled.

  ### SEQUENCE DIAGRAM OF DATA INGESTION

  ```mermaid
  sequenceDiagram
    autonumber

    participant SRC as Event Source
    participant ING as Ingestion
    participant RP as Redpanda
    participant W as Worker
    participant EMB as Embedding Model
    participant Q as Qdrant

    SRC->>ING: Raw event
    ING->>ING: Normalize event
    ING->>RP: Publish RawEvent
    RP-->>W: Consume batch

    W->>W: Chunk content
    W->>EMB: Generate embeddings
    EMB-->>W: 384-dim vectors

    W->>Q: Upsert vectors + payloads
    Q-->>W: Success

    W->>RP: Commit offsets

    Note over RP,W: At-least-once processing
    Note over W,Q: Deterministic UUID5 IDs<br/>make retries idempotent
  ```

  ### QUERY PATH

  ```mermaid
  flowchart TD

    A["User Query"] --> B["FastAPI Gateway"]

    B --> C["Validate Request"]
    C --> D["Embed Query<br/>all-MiniLM-L6-v2"]

    D --> E{"Redis Semantic<br/>Cache Hit?"}

    E -- "YES<br/>similarity > 0.92" --> F["Return Cached Response"]

    E -- "NO" --> G["Qdrant Vector Search"]

    G --> H["Top-K Relevant Chunks"]

    H --> I["Build Grounded Prompt"]

    I --> J["LLM Circuit Breaker"]

    J --> K{"LLM Available?"}

    K -- "YES" --> L["LLM Provider"]

    K -- "NO" --> M["Extractive Fallback"]

    L --> N["Generated Answer"]
    M --> O["Fallback Answer"]

    N --> P["Attach Sources + Metadata"]
    O --> P

    P --> Q["Cache Successful Response"]
    Q --> R["Return Response"]

    F --> R
  ```

  ## Components and responsibilities

  ### Data sources and ingestion

  - **Synthetic source** generates deterministic, realistic events for local
    demos, smoke tests, and repeatable end-to-end verification.
  - **Wikimedia SSE adapter** consumes the public Recent Changes stream and
    converts edit metadata and revision comments into the common event schema.
  - **Hacker News adapter** follows the public updates feed, fetches new
    stories/comments, and emits normalized events with reconnect and backoff
    behavior.
  - **Ingestion producer** routes the selected source, validates and normalizes
    events, publishes them to Kafka-compatible Redpanda, and retries transient
    broker failures.

  ### Streaming and indexing

  - **Redpanda** provides the durable `raw-events` Kafka topic, partitions events
    by `event_id`, and stores consumer-group offsets for the indexing workers.
  - **Indexing worker** consumes events with auto-commit disabled, processes them
    in batches, and commits offsets only after every corresponding Qdrant write
    succeeds. A crash before the commit replays the event safely.
  - **Chunker** splits long event content into overlapping word windows so
    retrieval can return focused passages while preserving neighboring context.
  - **Sentence Transformer embedder** produces 384-dimensional CPU embeddings
    using `all-MiniLM-L6-v2`. A deterministic hashing fallback can keep offline
    development operational when explicitly enabled.
  - **Qdrant** stores vectors together with chunk text and event metadata. Stable
    UUID5 point IDs make replayed or retried writes idempotent.

  ### Query and answer generation

  - **FastAPI gateway** validates requests, embeds queries, coordinates cache and
    vector retrieval, invokes the LLM adapter, and exposes `/query`, `/health`,
    and `/metrics`.
  - **Redis semantic cache** stores successful responses with their normalized
    query vectors and a TTL. The gateway computes cosine similarity and accepts a
    cache hit only when the score is strictly greater than `0.92`.
  - **Qdrant retrieval** performs bounded top-k cosine search and supplies the
    highest-scoring chunks as grounded context for synthesis.
  - **LLM adapters** provide a common interface for local Ollama, an
    OpenAI-compatible endpoint, or the built-in mock provider. The default mock
    makes the stack runnable without credentials.
  - **Circuit breaker** protects the gateway from repeated LLM failures. It
    transitions between closed, open, and half-open states and prevents a
    degraded provider from consuming every request.
  - **Extractive fallback** builds an answer directly from retrieved chunks when
    the LLM times out, fails, or is blocked by the circuit breaker. Fallback
    responses are deliberately not written to Redis.

### Operations and verification

- **Docker Compose** runs Redpanda, Qdrant, Redis, the gateway, the worker, and
  the ingestion process as a local distributed stack with dependency health
  checks.
- **Health and metrics endpoints** expose dependency status, indexed counts,
  cache activity, breaker state, and request latency for operators and tests.
- **Seed and end-to-end scripts** publish known events and verify infrastructure
  health, Kafka ingestion, Qdrant search, semantic-cache hits, and fallback
  behavior.

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
