from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime

import httpx

LOGGER = logging.getLogger(__name__)
URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def events(reconnect_seconds: float = 5.0) -> Iterator[dict[str, object]]:
    """Yield normalized Wikimedia Recent Changes events from the public SSE feed."""
    while True:
        try:
            timeout = httpx.Timeout(connect=15.0, read=None, write=15.0, pool=15.0)
            with httpx.Client(timeout=timeout, headers={"accept": "text/event-stream"}) as client:
                with client.stream("GET", URL) as response:
                    response.raise_for_status()
                    data_lines: list[str] = []
                    for line in response.iter_lines():
                        if line.startswith("data:"):
                            data_lines.append(line[5:].strip())
                        elif not line and data_lines:
                            payload = json.loads("".join(data_lines))
                            data_lines = []
                            yield normalize(payload)
        except (httpx.HTTPError, json.JSONDecodeError, OSError) as exc:
            LOGGER.warning("Wikimedia stream disconnected: %s", exc)
            time.sleep(max(1.0, reconnect_seconds))


def normalize(payload: dict[str, object]) -> dict[str, object]:
    server_name = str(payload.get("server_name") or "wikimedia.org")
    page_title = str(payload.get("title") or "Wikimedia recent change")
    comment = str(payload.get("comment") or "")
    user = str(payload.get("user") or "unknown user")
    wiki = str(payload.get("wiki") or "unknown wiki")
    revision = payload.get("revision") if isinstance(payload.get("revision"), dict) else {}
    revision_id = revision.get("new") if isinstance(revision, dict) else None
    event_id = str(payload.get("id") or revision_id or uuid.uuid4())
    numeric_time = payload.get("timestamp")
    timestamp = (
        datetime.fromtimestamp(float(numeric_time), tz=UTC).isoformat()
        if isinstance(numeric_time, (int, float))
        else datetime.now(UTC).isoformat()
    )
    title = f"{page_title} ({wiki})"
    content = f"{user} changed {page_title} on {wiki}. {comment}".strip()
    return {
        "event_id": f"wikimedia:{event_id}",
        "timestamp": timestamp,
        "source": "wikimedia",
        "title": title,
        "content": content,
        "url": f"https://{server_name}/wiki/{page_title.replace(' ', '_')}",
        "metadata": {
            "type": payload.get("type"),
            "namespace": payload.get("namespace"),
            "bot": payload.get("bot", False),
        },
    }