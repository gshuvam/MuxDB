from __future__ import annotations

import time
from enum import Enum
from typing import Any, Callable, TypeVar

from muxdb.errors import CircuitOpenError

class State(Enum):
    CLOSED = 1
    OPEN = 2
    HALF_OPEN = 3

T = TypeVar("T")

class CircuitBreaker:
    """A Circuit Breaker (CLOSED -> OPEN -> HALF_OPEN) state machine to isolate failures.

    Protects resources (like databases or shards) from cascading failures.
    """

    def __init__(
        self,
        shard_id: str,
        failure_threshold: int = 5,
        recovery_timeout_s: float = 10.0,
    ) -> None:
        self.shard_id = shard_id
        self.failure_threshold = failure_threshold
        self.recovery_timeout_s = recovery_timeout_s
        self.state = State.CLOSED
        self.failure_count = 0
        self.last_state_change = time.time()

    def _on_success(self) -> None:
        if self.state == State.HALF_OPEN:
            self.state = State.CLOSED
            self.failure_count = 0
            self.last_state_change = time.time()
        elif self.state == State.CLOSED:
            self.failure_count = 0

    def _on_failure(self) -> None:
        self.failure_count += 1
        if self.state == State.CLOSED and self.failure_count >= self.failure_threshold:
            self.state = State.OPEN
            self.last_state_change = time.time()
        elif self.state == State.HALF_OPEN:
            self.state = State.OPEN
            self.last_state_change = time.time()

    def check_state(self) -> None:
        """Verify if the circuit is healthy. Raises CircuitOpenError if OPEN."""
        if self.state == State.OPEN:
            elapsed = time.time() - self.last_state_change
            if elapsed >= self.recovery_timeout_s:
                self.state = State.HALF_OPEN
                self.last_state_change = time.time()
            else:
                raise CircuitOpenError(
                    self.shard_id,
                    recovery_after_s=self.recovery_timeout_s - elapsed,
                )

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Call a function wrapped inside the circuit breaker."""
        self.check_state()
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception:
            self._on_failure()
            raise

    async def call_async(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Call an async function wrapped inside the circuit breaker."""
        self.check_state()
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception:
            self._on_failure()
            raise
