from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime


GATEWAY = "http://localhost:8000"


def get_json(url: str) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def post_json(url: str, body: dict, headers: dict[str, str] | None = None) -> tuple[int, dict]:
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json", **(headers or {})},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def redis_ping() -> tuple[bool, str]:
    try:
        with socket.create_connection(("127.0.0.1", 6379), timeout=3) as connection:
            connection.sendall(b"*1\r\n$4\r\nPING\r\n")
            response = connection.recv(32)
        return response.startswith(b"+PONG"), response.decode(errors="replace")
    except OSError as exc:
        return False, str(exc)


def wait_for(name: str, check, timeout: float = 90.0) -> None:
    deadline = time.monotonic() + timeout
    last = "not checked"
    while time.monotonic() < deadline:
        try:
            ok, last = check()
            if ok:
                return
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            last = str(exc)
        time.sleep(2)
    raise AssertionError(f"{name} did not become ready: {last}")


def publish_event(event: dict) -> None:
    # Reuse the already-built worker image so the host only needs Python stdlib
    # and Docker; no host-side kafka package or virtualenv is required.
    code = (
        "import json; from kafka import KafkaProducer; "
        "p=KafkaProducer(bootstrap_servers='redpanda:9092', "
        "value_serializer=lambda v: json.dumps(v).encode()); "
        f"p.send('raw-events', key={event['event_id']!r}.encode(), value={event!r}); "
        "p.flush(); p.close()"
    )
    result = subprocess.run(
        ["docker", "compose", "run", "--rm", "--no-deps", "worker", "python", "-c", code],
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode:
        raise AssertionError(result.stderr.strip() or "Kafka publish failed")


def main() -> int:
    print("Checking Redpanda, Qdrant, Redis, and gateway health...")
    wait_for(
        "Redpanda",
        lambda: (
            get_json("http://localhost:9644/v1/status/ready")[0] == 200,
            get_json("http://localhost:9644/v1/status/ready")[1],
        ),
    )
    wait_for(
        "Qdrant",
        lambda: (
            get_json("http://localhost:6333/collections")[0] == 200,
            get_json("http://localhost:6333/collections")[1],
        ),
    )
    wait_for(
        "Redis",
        redis_ping,
    )
    wait_for(
        "Gateway",
        lambda: (
            get_json(f"{GATEWAY}/health")[0] == 200,
            get_json(f"{GATEWAY}/health")[1],
        ),
        timeout=180,
    )
    print("All services are healthy.")

    token = f"e2e-{uuid.uuid4().hex[:12]}"
    event = {
        "event_id": f"test:{token}",
        "timestamp": datetime.now(UTC).isoformat(),
        "source": "integration-test",
        "title": f"Unique distributed RAG event {token}",
        "content": (
            f"The real-time pipeline test event contains the exact unique phrase {token}. "
            "Its presence proves Kafka ingestion, CPU embedding, and Qdrant indexing work."
        ),
        "metadata": {"test_token": token},
    }
    print(f"Publishing unique event {token}...")
    publish_event(event)

    query_body = {"query": f"What does the event {token} say?", "top_k": 3}
    started = time.perf_counter()
    found: dict | None = None
    while time.monotonic() - started < 90:
        status, payload = post_json(f"{GATEWAY}/query", query_body)
        if status == 200 and any(source.get("event_id") == event["event_id"] for source in payload["sources"]):
            found = payload
            break
        time.sleep(2)
    if found is None:
        raise AssertionError("Published event was not searchable within 90 seconds")
    ingestion_latency = (time.perf_counter() - started) * 1000
    print(f"Event searchable after {ingestion_latency:.0f} ms.")
    assert any(source["event_id"] == event["event_id"] for source in found["sources"])

    status, cached = post_json(f"{GATEWAY}/query", query_body)
    assert status == 200 and cached["cached"] is True, cached
    assert cached["cache_similarity"] > 0.92, cached
    print(f"Redis semantic cache hit verified (similarity={cached['cache_similarity']:.4f}).")

    status, fallback = post_json(
        f"{GATEWAY}/query",
        {"query": f"Give a failure-path summary for {token} now", "top_k": 3},
        headers={"X-Simulate-LLM-Failure": "true"},
    )
    assert status == 200 and fallback["fallback_mode"] is True, fallback
    assert fallback["cached"] is False, fallback
    print("Circuit-breaker extractive fallback verified.")
    print("PASS: end-to-end distributed RAG pipeline is healthy.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AssertionError, OSError, subprocess.SubprocessError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)