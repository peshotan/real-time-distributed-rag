from __future__ import annotations

import asyncio
import time
from enum import StrEnum


class BreakerState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, failure_threshold: int, reset_seconds: float) -> None:
        self.failure_threshold = max(1, failure_threshold)
        self.reset_seconds = max(0.1, reset_seconds)
        self._state = BreakerState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> BreakerState:
        if self._state == BreakerState.OPEN and time.monotonic() - self._opened_at >= self.reset_seconds:
            return BreakerState.HALF_OPEN
        return self._state

    async def allow(self) -> bool:
        async with self._lock:
            if self._state == BreakerState.OPEN:
                if time.monotonic() - self._opened_at < self.reset_seconds:
                    return False
                self._state = BreakerState.HALF_OPEN
            return True

    async def record_success(self) -> None:
        async with self._lock:
            self._state = BreakerState.CLOSED
            self._failures = 0

    async def record_failure(self) -> None:
        async with self._lock:
            self._failures += 1
            if self._failures >= self.failure_threshold or self._state == BreakerState.HALF_OPEN:
                self._state = BreakerState.OPEN
                self._opened_at = time.monotonic()

    async def current_state(self) -> BreakerState:
        async with self._lock:
            if self._state == BreakerState.OPEN and time.monotonic() - self._opened_at >= self.reset_seconds:
                self._state = BreakerState.HALF_OPEN
            return self._state