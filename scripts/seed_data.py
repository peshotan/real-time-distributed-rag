from __future__ import annotations

import json
import subprocess
import sys
import uuid
from datetime import UTC, datetime


def main() -> int:
    token = f"seed-{uuid.uuid4().hex[:12]}"
    event = {
        "event_id": f"manual:{token}",
        "timestamp": datetime.now(UTC).isoformat(),
        "source": "manual-seed",
        "title": f"Pipeline smoke event {token}",
        "content": (
            f"This manually seeded event contains the unique verification phrase {token}. "
            "It should become searchable after the processing worker indexes it."
        ),
        "metadata": {"purpose": "smoke-test"},
    }
    code = (
        "import json; from kafka import KafkaProducer; "
        "p=KafkaProducer(bootstrap_servers='redpanda:9092', "
        "value_serializer=lambda v: json.dumps(v).encode()); "
        f"p.send('raw-events', key={event['event_id']!r}.encode(), value={event!r}); "
        "p.flush(); p.close()"
    )
    completed = subprocess.run(
        ["docker", "compose", "run", "--rm", "--no-deps", "worker", "python", "-c", code],
        check=False,
        text=True,
        capture_output=True,
    )
    if completed.returncode:
        print(completed.stderr.strip() or "Could not publish event.", file=sys.stderr)
        return completed.returncode
    print(json.dumps({"event_id": event["event_id"], "search_phrase": token}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())