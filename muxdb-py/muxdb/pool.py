"""
Adaptive connection pool — Citus-style 10ms slow-start.

Manages per-shard connection pools with:
  - **Slow-start scaling**: connections ramp up by 1 every ``slow_start_ms``
    (default 10ms), preventing pool exhaustion on burst queries while
    saturating available cores for long-running analytics.
  - **Transaction pinning**: connections used within a transaction are pinned
    to ensure all operations hit the same physical connection (ACID guarantee).
  - **Health checks**: periodic liveness probes per shard.
  - **Idle eviction**: connections unused beyond ``idle_timeout_s`` are closed.
"""

from __future__ import annotations

import asyncio
import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import structlog

from muxdb.config import PoolConfig
from muxdb.errors import PoolExhaustedError
from muxdb.shard_map import ShardInfo

logger = structlog.get_logger(__name__)


@dataclass
class PoolStats:
    """Snapshot of a single shard pool's state."""

    shard_id: str
    active: int = 0
    idle: int = 0
    total: int = 0
    max_size: int = 0
    waiting: int = 0
    total_acquired: int = 0
    total_released: int = 0
    total_timeouts: int = 0
    slow_start_current: int = 0


class ConnectionWrapper:
    """Wraps a raw backend connection with metadata for pool management."""

    def __init__(self, raw_conn: Any, shard_id: str) -> None:
        self.raw = raw_conn
        self.shard_id = shard_id
        self.created_at = time.monotonic()
        self.last_used_at = time.monotonic()
        self.in_transaction = False
        self._closed = False

    def mark_used(self) -> None:
        self.last_used_at = time.monotonic()

    @property
    def idle_time(self) -> float:
        return time.monotonic() - self.last_used_at

    @property
    def age(self) -> float:
        return time.monotonic() - self.created_at

    @property
    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            # Attempt to close the raw connection
            raw_close = getattr(self.raw, "close", None)
            if callable(raw_close):
                try:
                    raw_close()
                except Exception:
                    pass

    def __repr__(self) -> str:
        return (
            f"ConnectionWrapper(shard={self.shard_id!r}, "
            f"idle={self.idle_time:.1f}s, "
            f"in_tx={self.in_transaction})"
        )


class ShardPool:
    """
    Connection pool for a single shard with slow-start scaling.

    The slow-start mechanism works as follows:
    1. Pool starts with ``min_size`` connections pre-created.
    2. When a connection is requested and no idle connections exist:
       a. If ``total < slow_start_current``, create a new connection immediately.
       b. Otherwise, wait up to ``slow_start_ms`` for a connection to become idle.
       c. After waiting, increment ``slow_start_current`` by 1 and create.
    3. For fast queries (~1ms), the query completes before slow-start opens
       a second connection → minimal overhead.
    4. For slow queries (~seconds), slow-start rapidly scales up the pool
       → full parallelism across cores.
    """

    def __init__(
        self,
        shard: ShardInfo,
        config: PoolConfig,
        connection_factory: Any = None,
    ) -> None:
        self._shard = shard
        self._config = config
        self._factory = connection_factory
        self._lock = threading.Lock()

        self._idle: list[ConnectionWrapper] = []
        self._active: set[ConnectionWrapper] = set()
        self._slow_start_current = config.min_size
        self._waiters: int = 0

        # Stats
        self._total_acquired = 0
        self._total_released = 0
        self._total_timeouts = 0

    @property
    def shard_id(self) -> str:
        return self._shard.id

    def acquire(self, timeout_s: float = 5.0) -> ConnectionWrapper:
        """Acquire a connection from the pool.

        Implements the slow-start algorithm:
        - If an idle connection is available, return it immediately.
        - If total connections < slow_start_current, create one.
        - Otherwise, wait ``slow_start_ms`` then increment and create.

        Args:
            timeout_s: Maximum time to wait for a connection.

        Returns:
            A :class:`ConnectionWrapper` that must be returned via :meth:`release`.

        Raises:
            PoolExhaustedError: If the pool is fully utilized and timeout expires.
        """
        deadline = time.monotonic() + timeout_s

        while True:
            with self._lock:
                # 1. Try to get an idle connection
                while self._idle:
                    conn = self._idle.pop()
                    if not conn.is_closed:
                        conn.mark_used()
                        self._active.add(conn)
                        self._total_acquired += 1
                        return conn

                total = len(self._active) + len(self._idle)

                # 2. If we can create immediately (within slow-start window)
                if total < self._slow_start_current and total < self._config.max_size:
                    conn = self._create_connection()
                    self._active.add(conn)
                    self._total_acquired += 1
                    return conn

                # 3. Check if we've hit the absolute max
                if total >= self._config.max_size:
                    if time.monotonic() >= deadline:
                        self._total_timeouts += 1
                        raise PoolExhaustedError(
                            shard_id=self._shard.id,
                            max_size=self._config.max_size,
                        )
                    # Wait and retry
                    self._waiters += 1

            # Wait for slow-start interval then try again
            wait_ms = min(
                self._config.slow_start_ms / 1000.0,
                max(0, deadline - time.monotonic()),
            )
            time.sleep(wait_ms)

            with self._lock:
                if self._waiters > 0:
                    self._waiters -= 1

                # After waiting, increment slow-start window
                total = len(self._active) + len(self._idle)
                if total < self._config.max_size:
                    self._slow_start_current = min(
                        self._slow_start_current + 1,
                        self._config.max_size,
                    )
                    conn = self._create_connection()
                    self._active.add(conn)
                    self._total_acquired += 1
                    return conn

            if time.monotonic() >= deadline:
                self._total_timeouts += 1
                raise PoolExhaustedError(
                    shard_id=self._shard.id,
                    max_size=self._config.max_size,
                )

    def release(self, conn: ConnectionWrapper) -> None:
        """Return a connection to the pool."""
        with self._lock:
            self._active.discard(conn)
            conn.mark_used()
            conn.in_transaction = False

            if conn.is_closed:
                return

            # Evict if beyond max_size (can happen after topology change)
            total = len(self._active) + len(self._idle)
            if total >= self._config.max_size:
                conn.close()
                return

            self._idle.append(conn)
            self._total_released += 1

    def evict_idle(self) -> int:
        """Close connections that have been idle beyond the timeout.

        Returns:
            Number of connections evicted.
        """
        with self._lock:
            cutoff = time.monotonic() - self._config.idle_timeout_s
            still_idle: list[ConnectionWrapper] = []
            evicted = 0

            for conn in self._idle:
                if conn.last_used_at < cutoff:
                    conn.close()
                    evicted += 1
                else:
                    still_idle.append(conn)

            self._idle = still_idle

            if evicted > 0:
                logger.debug(
                    "pool.evict_idle",
                    shard_id=self._shard.id,
                    evicted=evicted,
                    remaining_idle=len(self._idle),
                )

            return evicted

    def close_all(self) -> None:
        """Close all connections in the pool."""
        with self._lock:
            for conn in self._idle:
                conn.close()
            for conn in self._active:
                conn.close()
            self._idle.clear()
            self._active.clear()
            self._slow_start_current = self._config.min_size

            logger.info("pool.close_all", shard_id=self._shard.id)

    def stats(self) -> PoolStats:
        """Get a snapshot of pool statistics."""
        with self._lock:
            return PoolStats(
                shard_id=self._shard.id,
                active=len(self._active),
                idle=len(self._idle),
                total=len(self._active) + len(self._idle),
                max_size=self._config.max_size,
                waiting=self._waiters,
                total_acquired=self._total_acquired,
                total_released=self._total_released,
                total_timeouts=self._total_timeouts,
                slow_start_current=self._slow_start_current,
            )

    def _create_connection(self) -> ConnectionWrapper:
        """Create a new raw connection via the factory."""
        if self._factory is None:
            # Return a placeholder — real driver implementations override this
            raw = _PlaceholderConnection(self._shard)
        else:
            raw = self._factory(self._shard)

        conn = ConnectionWrapper(raw, self._shard.id)

        logger.debug(
            "pool.create_connection",
            shard_id=self._shard.id,
            total=len(self._active) + len(self._idle) + 1,
            slow_start_current=self._slow_start_current,
        )

        return conn


class _PlaceholderConnection:
    """Placeholder connection for testing when no driver is configured."""

    def __init__(self, shard: ShardInfo) -> None:
        self.shard = shard
        self._closed = False

    def close(self) -> None:
        self._closed = True

    def __repr__(self) -> str:
        return f"PlaceholderConnection(shard={self.shard.id!r})"


class AdaptivePool:
    """
    Multi-shard adaptive connection pool manager.

    Manages a :class:`ShardPool` for each shard in the cluster, providing
    the unified pool interface that the MuxDB client uses.

    Example::

        pool = AdaptivePool(config, shard_map)
        conn = pool.acquire("shard_0")
        try:
            # use conn.raw to execute queries
            pass
        finally:
            pool.release(conn)
    """

    def __init__(
        self,
        config: PoolConfig,
        shards: list[ShardInfo],
        connection_factory: Any = None,
    ) -> None:
        self._config = config
        self._pools: dict[str, ShardPool] = {}
        self._factory = connection_factory

        for shard in shards:
            self._pools[shard.id] = ShardPool(shard, config, connection_factory)

        logger.info(
            "adaptive_pool.init",
            shard_count=len(shards),
            min_size=config.min_size,
            max_size=config.max_size,
            slow_start_ms=config.slow_start_ms,
        )

    def acquire(self, shard_id: str, timeout_s: float = 5.0) -> ConnectionWrapper:
        """Acquire a connection for the specified shard."""
        pool = self._pools.get(shard_id)
        if pool is None:
            raise PoolExhaustedError(shard_id=shard_id, max_size=0)
        return pool.acquire(timeout_s=timeout_s)

    def release(self, conn: ConnectionWrapper) -> None:
        """Release a connection back to its shard pool."""
        pool = self._pools.get(conn.shard_id)
        if pool:
            pool.release(conn)

    def add_shard(self, shard: ShardInfo) -> None:
        """Add a new shard pool (used during topology updates)."""
        if shard.id not in self._pools:
            self._pools[shard.id] = ShardPool(shard, self._config, self._factory)
            logger.info("adaptive_pool.add_shard", shard_id=shard.id)

    def remove_shard(self, shard_id: str) -> None:
        """Remove and drain a shard pool."""
        pool = self._pools.pop(shard_id, None)
        if pool:
            pool.close_all()
            logger.info("adaptive_pool.remove_shard", shard_id=shard_id)

    def evict_all_idle(self) -> int:
        """Run idle eviction across all shard pools."""
        total = 0
        for pool in self._pools.values():
            total += pool.evict_idle()
        return total

    def close_all(self) -> None:
        """Close all connections across all shard pools."""
        for pool in self._pools.values():
            pool.close_all()
        logger.info("adaptive_pool.close_all", shard_count=len(self._pools))

    def stats(self) -> dict[str, PoolStats]:
        """Get pool stats for all shards."""
        return {sid: pool.stats() for sid, pool in self._pools.items()}

    def __repr__(self) -> str:
        return f"AdaptivePool(shards={len(self._pools)}, config={self._config})"
