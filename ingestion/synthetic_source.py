from __future__ import annotations

import random
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime


_EVENTS = (
    (
        "Markets steady after infrastructure investment update",
        "Technology and financial markets are evaluating a new infrastructure investment "
        "announcement. Analysts are watching supply chains, cloud capacity, and long-term "
        "capital expenditure plans.",
        "financial-news",
    ),
    (
        "Distributed systems team publishes streaming reliability report",
        "The engineering team published a report covering consumer lag, retry budgets, "
        "idempotent writes, and graceful degradation for real-time data pipelines.",
        "technology",
    ),
    (
        "Operations alert: elevated request latency observed",
        "The platform observed elevated request latency in one region. On-call engineers "
        "are comparing cache hit rates, queue depth, and vector database response time.",
        "system-log",
    ),
)


def events(rate: float = 5.0) -> Iterator[dict[str, object]]:
    """Yield realistic, deterministic-shape events at the requested rate."""
    delay = 1.0 / max(rate, 0.01)
    while True:
        title, content, category = random.choice(_EVENTS)
        now = datetime.now(UTC)
        yield {
            "event_id": f"synthetic:{uuid.uuid4()}",
            "timestamp": now.isoformat(),
            "source": "synthetic",
            "title": title,
            "content": content,
            "metadata": {"category": category, "generated": True},
        }
        time.sleep(delay)