from __future__ import annotations

import time
from threading import Lock

class TokenBucketRateLimiter:
    """Thread-safe Token Bucket Rate Limiter for query limiting."""

    def __init__(self, rate: float, capacity: float) -> None:
        self.rate = rate          # Tokens replenished per second
        self.capacity = capacity  # Maximum tokens in the bucket
        self.tokens = capacity
        self.last_refill = time.time()
        self.lock = Lock()

    def _refill(self) -> None:
        now = time.time()
        elapsed = now - self.last_refill
        if elapsed > 0:
            refilled = elapsed * self.rate
            self.tokens = min(self.capacity, self.tokens + refilled)
            self.last_refill = now

    def acquire(self, tokens: float = 1.0) -> bool:
        """Attempt to acquire a set number of tokens. Returns True if successful."""
        with self.lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False
