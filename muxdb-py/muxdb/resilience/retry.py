from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Callable, TypeVar

T = TypeVar("T")

def retry_with_backoff(
    func: Callable[..., T],
    max_retries: int = 3,
    base_delay: float = 0.1,
    max_delay: float = 2.0,
    exponential_base: float = 2.0,
    *args: Any,
    **kwargs: Any,
) -> T:
    """Retry a synchronous function with exponential backoff and jitter."""
    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            if attempt == max_retries:
                break
            
            delay = min(max_delay, base_delay * (exponential_base ** attempt))
            jitter = random.uniform(0, 0.1 * delay)
            time.sleep(delay + jitter)
            
    raise last_exception or RuntimeError("Retry failed")

async def retry_with_backoff_async(
    func: Callable[..., Any],
    max_retries: int = 3,
    base_delay: float = 0.1,
    max_delay: float = 2.0,
    exponential_base: float = 2.0,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Retry an asynchronous function with exponential backoff and jitter."""
    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            if attempt == max_retries:
                break
            
            delay = min(max_delay, base_delay * (exponential_base ** attempt))
            jitter = random.uniform(0, 0.1 * delay)
            await asyncio.sleep(delay + jitter)
            
    raise last_exception or RuntimeError("Retry failed")
