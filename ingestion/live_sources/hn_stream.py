from __future__ import annotations

import html
import logging
import re
import time
from collections.abc import Iterator
from datetime import UTC, datetime

import httpx

LOGGER = logging.getLogger(__name__)
UPDATES_URL = "https://hacker-news.firebaseio.com/v0/updates.json"
ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
_TAG_RE = re.compile(r"<[^>]+>")


def events(poll_seconds: float = 5.0) -> Iterator[dict[str, object]]:
    """Poll the official HN Firebase updates feed and emit new items."""
    seen: set[int] = set()
    timeout = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=10.0)
    with httpx.Client(timeout=timeout) as client:
        while True:
            try:
                response = client.get(UPDATES_URL)
                response.raise_for_status()
                updates = response.json()
                ids = updates.get("items", []) if isinstance(updates, dict) else []
                for item_id in ids:
                    if not isinstance(item_id, int) or item_id in seen:
                        continue
                    item_response = client.get(ITEM_URL.format(item_id=item_id))
                    item_response.raise_for_status()
                    item = item_response.json()
                    if isinstance(item, dict) and item.get("type") in {"story", "comment"}:
                        seen.add(item_id)
                        yield normalize(item)
                # Avoid unbounded memory while preserving the recent working set.
                if len(seen) > 10_000:
                    seen = set(list(seen)[-5_000:])
            except (httpx.HTTPError, ValueError, OSError) as exc:
                LOGGER.warning("Hacker News polling error: %s", exc)
                time.sleep(max(1.0, poll_seconds))
            else:
                time.sleep(max(0.5, poll_seconds))


def normalize(item: dict[str, object]) -> dict[str, object]:
    item_id = int(item["id"])
    title = str(item.get("title") or item.get("by") or "Hacker News update")
    raw_text = str(item.get("text") or item.get("title") or "")
    content = html.unescape(_TAG_RE.sub(" ", raw_text))
    timestamp = datetime.fromtimestamp(
        float(item.get("time") or datetime.now(UTC).timestamp()), tz=UTC
    ).isoformat()
    return {
        "event_id": f"hackernews:{item_id}",
        "timestamp": timestamp,
        "source": "hackernews",
        "title": title,
        "content": content.strip() or title,
        "url": str(item.get("url") or f"https://news.ycombinator.com/item?id={item_id}"),
        "metadata": {
            "item_type": item.get("type"),
            "author": item.get("by"),
            "score": item.get("score"),
            "parent": item.get("parent"),
        },
    }