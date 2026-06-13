from __future__ import annotations

from threading import Semaphore
from typing import Any, Callable, TypeVar

from muxdb.errors import BulkheadLimitExceeded

T = TypeVar("T")

class Bulkhead:
    """A Bulkhead concurrency controller to prevent single-resource starvation."""

    def __init__(self, max_concurrency: int) -> None:
        self.semaphore = Semaphore(max_concurrency)
        self.max_concurrency = max_concurrency

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute a function inside the bulkhead. Raises BulkheadLimitExceeded if full."""
        acquired = self.semaphore.acquire(blocking=False)
        if not acquired:
            raise BulkheadLimitExceeded(
                f"Bulkhead concurrency limit of {self.max_concurrency} exceeded."
            )
        try:
            return func(*args, **kwargs)
        finally:
            self.semaphore.release()

    async def call_async(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute an async function inside the bulkhead. Raises BulkheadLimitExceeded if full."""
        acquired = self.semaphore.acquire(blocking=False)
        if not acquired:
            raise BulkheadLimitExceeded(
                f"Bulkhead concurrency limit of {self.max_concurrency} exceeded."
            )
        try:
            return await func(*args, **kwargs)
        finally:
            self.semaphore.release()
