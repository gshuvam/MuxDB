"""
MuxDB client — the main entry point.

Provides a single logical database interface that transparently routes
queries across shards, manages connection pools, and orchestrates
scatter-gather for cross-shard operations.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Generator, Sequence

import structlog

from muxdb.config import MuxConfig
from muxdb.drivers.base import BaseDriver, QueryResult
from muxdb.errors import (
    CrossShardTransactionError,
    MuxDBError,
    RoutingError,
)
from muxdb.pool import AdaptivePool, ConnectionWrapper
from muxdb.router import QueryContext, RouteDecision, RouteType, Router
from muxdb.shard_map import ShardInfo, ShardMap, create_shard_map

logger = structlog.get_logger(__name__)


class MuxTransaction:
    """
    A single-shard transaction context.

    All operations within a transaction are pinned to the same shard
    and the same physical connection, preserving ACID semantics.

    Usage::

        with db.transaction(shard_key=42) as tx:
            tx.execute("UPDATE accounts SET balance = balance - 100 WHERE id = %s", [1])
            tx.execute("UPDATE accounts SET balance = balance + 100 WHERE id = %s", [2])
        # auto-commits on exit; rolls back on exception
    """

    def __init__(
        self,
        shard: ShardInfo,
        conn: ConnectionWrapper,
        driver: BaseDriver,
        router: Router,
    ) -> None:
        self._shard = shard
        self._conn = conn
        self._driver = driver
        self._router = router
        self._committed = False
        self._rolled_back = False

    @property
    def shard_id(self) -> str:
        return self._shard.id

    def execute(
        self,
        query: str,
        params: Sequence[Any] = (),
    ) -> QueryResult:
        """Execute a query within this transaction.

        The query is validated to ensure it targets the same shard.
        Cross-shard operations within a transaction raise
        :class:`CrossShardTransactionError`.
        """
        ctx = QueryContext(
            query=query,
            params=tuple(params),
            transaction_shard=self._shard.id,
        )
        decision = self._router.route(ctx)

        # Verify single-shard constraint
        for target in decision.targets:
            if target.id != self._shard.id:
                raise CrossShardTransactionError(
                    shards=[self._shard.id, target.id],
                )

        return self._driver.execute(self._conn.raw, query, params)

    def commit(self) -> None:
        """Explicitly commit the transaction."""
        if not self._committed and not self._rolled_back:
            self._driver.commit(self._conn.raw)
            self._committed = True
            logger.debug("transaction.commit", shard_id=self._shard.id)

    def rollback(self) -> None:
        """Explicitly roll back the transaction."""
        if not self._committed and not self._rolled_back:
            self._driver.rollback(self._conn.raw)
            self._rolled_back = True
            logger.debug("transaction.rollback", shard_id=self._shard.id)


class MuxDB:
    """
    MuxDB — Autonomous Data Orchestration Client.

    The primary interface for interacting with a sharded database cluster.
    MuxDB transparently routes queries to the correct shard(s), manages
    adaptive connection pools, and orchestrates scatter-gather for cross-shard
    operations.

    Example::

        from muxdb import MuxDB, MuxConfig

        config = MuxConfig.from_file("muxdb.yaml")
        db = MuxDB(config)
        db.connect()

        result = db.execute("SELECT * FROM orders WHERE user_id = %s", [42])
        for row in result.rows:
            print(row)

        db.close()
    """

    def __init__(self, config: MuxConfig) -> None:
        self._config = config
        self._shard_map: ShardMap | None = None
        self._router: Router | None = None
        self._pool: AdaptivePool | None = None
        self._driver: BaseDriver | None = None
        self._connected = False

    @property
    def config(self) -> MuxConfig:
        return self._config

    @property
    def shard_map(self) -> ShardMap:
        if self._shard_map is None:
            raise MuxDBError("MuxDB is not connected. Call db.connect() first.")
        return self._shard_map

    @property
    def router(self) -> Router:
        if self._router is None:
            raise MuxDBError("MuxDB is not connected. Call db.connect() first.")
        return self._router

    @property
    def pool(self) -> AdaptivePool:
        if self._pool is None:
            raise MuxDBError("MuxDB is not connected. Call db.connect() first.")
        return self._pool

    @property
    def is_connected(self) -> bool:
        return self._connected

    def connect(self, driver: BaseDriver | None = None) -> MuxDB:
        """Initialize the shard map, router, and connection pools.

        Args:
            driver: An optional driver instance. If not provided, a default
                driver is selected based on the shard backend type.

        Returns:
            self (for method chaining).
        """
        if self._connected:
            logger.warning("muxdb.already_connected")
            return self

        self._shard_map = create_shard_map(self._config)
        self._router = Router(self._config, self._shard_map)

        # Resolve driver
        if driver:
            self._driver = driver
        else:
            self._driver = self._auto_detect_driver()

        # Build adaptive pool
        self._pool = AdaptivePool(
            config=self._config.pool,
            shards=self._shard_map.shards,
            connection_factory=self._make_connection_factory(),
        )

        self._connected = True

        logger.info(
            "muxdb.connected",
            cluster=self._config.cluster.name,
            strategy=self._config.cluster.strategy,
            shard_count=len(self._shard_map),
            shard_ids=self._shard_map.shard_ids,
        )

        return self

    def close(self) -> None:
        """Close all connections and shut down the client."""
        if self._pool:
            self._pool.close_all()
        self._connected = False
        logger.info("muxdb.closed", cluster=self._config.cluster.name)

    # --- Query execution ---

    def execute(
        self,
        query: str,
        params: Sequence[Any] = (),
        *,
        shard_key: Any = None,
    ) -> QueryResult:
        """Execute a query with automatic shard routing.

        Args:
            query: SQL query string.
            params: Query parameters.
            shard_key: Explicit shard key value (bypasses query parsing).

        Returns:
            A :class:`QueryResult` with the combined results.
        """
        self._ensure_connected()

        ctx = QueryContext(
            query=query,
            params=tuple(params),
            shard_key_value=shard_key,
        )
        decision = self._router.route(ctx)

        if decision.type == RouteType.SINGLE:
            return self._execute_single(decision, query, params)
        elif decision.type == RouteType.SCATTER:
            return self._execute_scatter(decision, query, params)
        elif decision.type == RouteType.BROADCAST:
            return self._execute_broadcast(decision, query, params)
        else:
            raise RoutingError(f"Unknown route type: {decision.type}")

    def execute_on_shard(
        self,
        shard_id: str,
        query: str,
        params: Sequence[Any] = (),
    ) -> QueryResult:
        """Execute a query on a specific shard (bypass routing).

        Useful for admin operations or when you know the exact target.
        """
        self._ensure_connected()
        shard = self._shard_map.get_shard(shard_id)
        return self._execute_on_target(shard, query, params)

    # --- Transaction ---

    @contextmanager
    def transaction(
        self,
        shard_key: Any = None,
        shard_id: str | None = None,
    ) -> Generator[MuxTransaction, None, None]:
        """Begin a single-shard transaction.

        Either ``shard_key`` or ``shard_id`` must be provided to determine
        which shard the transaction is pinned to.

        Usage::

            with db.transaction(shard_key=42) as tx:
                tx.execute("UPDATE ...", [...])
                tx.execute("UPDATE ...", [...])
            # auto-commits

        Args:
            shard_key: The shard key value to determine the target shard.
            shard_id: Explicit shard ID (overrides shard_key).

        Yields:
            A :class:`MuxTransaction` pinned to a single shard.
        """
        self._ensure_connected()

        # Determine target shard
        if shard_id:
            shard = self._shard_map.get_shard(shard_id)
        elif shard_key is not None:
            shard = self._shard_map.resolve(shard_key)
        else:
            raise MuxDBError(
                "transaction() requires either shard_key or shard_id. "
                "Transactions must be pinned to a single shard."
            )

        # Acquire and pin a connection
        conn = self._pool.acquire(shard.id)
        conn.in_transaction = True

        try:
            self._driver.begin(conn.raw)
            tx = MuxTransaction(shard, conn, self._driver, self._router)
            yield tx
            # Auto-commit if no explicit commit/rollback
            tx.commit()
        except Exception:
            tx.rollback()
            raise
        finally:
            conn.in_transaction = False
            self._pool.release(conn)

    # --- Internal execution ---

    def _execute_single(
        self, decision: RouteDecision, query: str, params: Sequence[Any]
    ) -> QueryResult:
        """Execute on a single targeted shard."""
        target = decision.targets[0]
        return self._execute_on_target(target, query, params)

    def _execute_scatter(
        self, decision: RouteDecision, query: str, params: Sequence[Any]
    ) -> QueryResult:
        """Scatter-gather: execute on all shards and merge results."""
        all_rows: list[dict[str, Any]] = []
        all_columns: list[str] = []
        total_count = 0
        total_elapsed = 0.0

        for target in decision.targets:
            result = self._execute_on_target(target, query, params)
            all_rows.extend(result.rows)
            total_count += result.row_count
            total_elapsed += result.elapsed_ms
            if not all_columns and result.columns:
                all_columns = result.columns

        return QueryResult(
            rows=all_rows,
            row_count=total_count,
            columns=all_columns,
            shard_id="scatter",
            elapsed_ms=total_elapsed,
        )

    def _execute_broadcast(
        self, decision: RouteDecision, query: str, params: Sequence[Any]
    ) -> QueryResult:
        """Broadcast: execute on all shards (DDL, no result merge)."""
        last_result = None
        for target in decision.targets:
            last_result = self._execute_on_target(target, query, params)

        return last_result or QueryResult(
            rows=[], row_count=0, columns=[], shard_id="broadcast"
        )

    def _execute_on_target(
        self, target: ShardInfo, query: str, params: Sequence[Any]
    ) -> QueryResult:
        """Execute a query on a specific shard with pool management."""
        conn = self._pool.acquire(target.id)
        try:
            result = self._driver.execute(conn.raw, query, params)
            result.shard_id = target.id
            return result
        finally:
            self._pool.release(conn)

    # --- Helpers ---

    def _ensure_connected(self) -> None:
        if not self._connected:
            raise MuxDBError(
                "MuxDB is not connected. Call db.connect() first."
            )

    def _auto_detect_driver(self) -> BaseDriver:
        """Auto-detect the driver based on the first shard's backend type."""
        if not self._config.shards:
            raise MuxDBError("No shards configured — cannot auto-detect driver")

        backend = self._config.shards[0].backend

        if backend == "postgresql":
            from muxdb.drivers.postgresql import PsycopgDriver
            return PsycopgDriver()
        else:
            # Return a no-op driver for backends that need explicit configuration
            raise MuxDBError(
                f"No auto-detected driver for backend '{backend}'. "
                f"Provide a driver explicitly via db.connect(driver=MyDriver())"
            )

    def _make_connection_factory(self):
        """Create a connection factory function for the pool."""
        driver = self._driver

        def factory(shard: ShardInfo):
            raw = driver.connect(shard)
            # Tag the raw connection with shard metadata
            try:
                raw._muxdb_shard_id = shard.id
            except (AttributeError, TypeError):
                pass
            return raw

        return factory

    # --- Status ---

    def status(self) -> dict[str, Any]:
        """Get a summary of the client's current state."""
        result: dict[str, Any] = {
            "connected": self._connected,
            "cluster": self._config.cluster.name,
            "strategy": self._config.cluster.strategy,
        }

        if self._shard_map:
            result["shard_count"] = len(self._shard_map)
            result["shard_map_version"] = self._shard_map.version
            result["shard_ids"] = self._shard_map.shard_ids

        if self._pool:
            result["pool_stats"] = {
                sid: {
                    "active": s.active,
                    "idle": s.idle,
                    "total": s.total,
                    "max": s.max_size,
                }
                for sid, s in self._pool.stats().items()
            }

        return result

    def __repr__(self) -> str:
        state = "connected" if self._connected else "disconnected"
        shards = len(self._shard_map) if self._shard_map else 0
        return (
            f"MuxDB(cluster={self._config.cluster.name!r}, "
            f"state={state}, shards={shards})"
        )

    def __enter__(self) -> MuxDB:
        self.connect()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
