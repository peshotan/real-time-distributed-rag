from __future__ import annotations

from dataclasses import dataclass

from common.config import settings
from common.models import RawEvent


@dataclass(frozen=True)
class TextChunk:
    event_id: str
    chunk_id: str
    source: str
    title: str
    text: str
    timestamp: str
    url: str | None
    metadata: dict[str, object]


def chunk_event(event: RawEvent) -> list[TextChunk]:
    """Create overlapping word windows while retaining event provenance."""
    title = event.title.strip()
    body = " ".join(event.content.split())
    combined = f"{title}\n{body}".strip() if title else body
    words = combined.split()
    if not words:
        return []

    size = max(1, settings.chunk_size_words)
    overlap = min(max(0, settings.chunk_overlap_words), size - 1)
    stride = size - overlap
    chunks: list[TextChunk] = []
    for index, start in enumerate(range(0, len(words), stride)):
        text = " ".join(words[start : start + size]).strip()
        if not text:
            continue
        chunks.append(
            TextChunk(
                event_id=event.event_id,
                chunk_id=f"{event.event_id}:{index}",
                source=event.source,
                title=event.title,
                text=text,
                timestamp=event.timestamp.isoformat(),
                url=event.url,
                metadata=event.metadata,
            )
        )
        if start + size >= len(words):
            break
    return chunks