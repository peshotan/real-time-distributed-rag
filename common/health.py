from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class ProcessHealth:
    def __init__(self, service: str) -> None:
        self.service = service
        self._lock = threading.Lock()
        self.processed = 0
        self.last_error: str | None = None

    def record_success(self, count: int = 1) -> None:
        with self._lock:
            self.processed += count
            self.last_error = None

    def record_error(self, message: str) -> None:
        with self._lock:
            self.last_error = message[:500]

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "status": "degraded" if self.last_error else "ok",
                "service": self.service,
                "processed": self.processed,
                "last_error": self.last_error,
            }


def start_health_server(health: ProcessHealth, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path != "/health":
                self.send_response(404)
                self.end_headers()
                return
            payload = json.dumps(health.snapshot()).encode("utf-8")
            self.send_response(200 if health.snapshot()["status"] == "ok" else 503)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server