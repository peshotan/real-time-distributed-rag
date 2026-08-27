from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RawEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1, max_length=256)
    timestamp: datetime
    source: str = Field(min_length=1, max_length=128)
    title: str = Field(default="", max_length=1000)
    content: str = Field(min_length=1, max_length=100_000)
    url: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("content")
    @classmethod
    def content_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("content must not be blank")
        return value


class QueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(min_length=2, max_length=2000)
    top_k: int = Field(default=3, ge=1, le=20)

    @field_validator("query")
    @classmethod
    def query_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("query must not be blank")
        return value.strip()


class SourceResult(BaseModel):
    event_id: str
    chunk_id: str
    source: str
    title: str
    text: str
    score: float
    timestamp: datetime
    url: str | None = None


class QueryResponse(BaseModel):
    answer: str
    query: str
    sources: list[SourceResult]
    cached: bool
    fallback_mode: bool
    latency_ms: float
    cache_similarity: float | None = None
    breaker_state: str


class HealthResponse(BaseModel):
    status: str
    dependencies: dict[str, str]


class MetricsResponse(BaseModel):
    queries_total: int
    cache_hits_total: int
    fallback_total: int
    llm_failures_total: int
    indexed_chunks_total: int