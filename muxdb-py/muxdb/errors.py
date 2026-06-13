"""
MuxDB error hierarchy.

All MuxDB exceptions inherit from MuxDBError, allowing callers to catch
the base class for broad handling or specific subclasses for targeted recovery.

Hierarchy:
    MuxDBError
    ├── ConfigError
    ├── RoutingError
    │   ├── ShardNotFoundError
    │   └── CrossShardTransactionError
    ├── DriverError
    │   └── ConnectionError
    ├── PoolExhaustedError
    ├── CircuitOpenError
    └── MigrationError
"""

from __future__ import annotations


class MuxDBError(Exception):
    """Base exception for all MuxDB errors."""

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        self.details = details or {}
        super().__init__(message)


# --- Configuration ---


class ConfigError(MuxDBError):
    """Raised when MuxDB configuration is invalid or cannot be loaded."""


# --- Routing ---


class RoutingError(MuxDBError):
    """Raised when a query cannot be routed to a shard."""


class ShardNotFoundError(RoutingError):
    """Raised when no shard matches the routing key."""

    def __init__(self, key: str | int, *, details: dict | None = None) -> None:
        self.key = key
        super().__init__(
            f"No shard found for routing key: {key!r}",
            details=details,
        )


class CrossShardTransactionError(RoutingError):
    """Raised when a transaction attempts to span multiple shards.

    MuxDB enforces single-shard transactions for ACID guarantees.
    Cross-shard operations require explicit scatter-gather outside a transaction.
    """

    def __init__(
        self,
        shards: list[str],
        *,
        details: dict | None = None,
    ) -> None:
        self.shards = shards
        super().__init__(
            f"Transaction cannot span multiple shards: {shards}. "
            "Use db.execute() outside a transaction for cross-shard queries.",
            details=details,
        )


# --- Driver & Connection ---


class DriverError(MuxDBError):
    """Raised when a backend driver encounters an error."""

    def __init__(
        self,
        message: str,
        *,
        shard_id: str | None = None,
        backend: str | None = None,
        details: dict | None = None,
    ) -> None:
        self.shard_id = shard_id
        self.backend = backend
        extra = {}
        if shard_id:
            extra["shard_id"] = shard_id
        if backend:
            extra["backend"] = backend
        if details:
            extra.update(details)
        super().__init__(message, details=extra)


class ConnectionError(DriverError):  # noqa: A001 — intentional shadow of builtin
    """Raised when a connection to a backend node cannot be established."""


# --- Pool ---


class PoolExhaustedError(MuxDBError):
    """Raised when the connection pool for a shard is fully utilized."""

    def __init__(self, shard_id: str, max_size: int, *, details: dict | None = None) -> None:
        self.shard_id = shard_id
        self.max_size = max_size
        super().__init__(
            f"Connection pool exhausted for shard '{shard_id}' (max_size={max_size}). "
            "Consider increasing pool.max_size or reducing query concurrency.",
            details=details,
        )


# --- Resilience ---


class CircuitOpenError(MuxDBError):
    """Raised when a shard's circuit breaker is in the OPEN state.

    The shard has experienced too many consecutive failures and is
    temporarily unavailable. Queries will be retried after the recovery window.
    """

    def __init__(
        self,
        shard_id: str,
        *,
        recovery_after_s: float | None = None,
        details: dict | None = None,
    ) -> None:
        self.shard_id = shard_id
        self.recovery_after_s = recovery_after_s
        msg = f"Circuit breaker OPEN for shard '{shard_id}'."
        if recovery_after_s is not None:
            msg += f" Recovery attempt in {recovery_after_s:.1f}s."
        super().__init__(msg, details=details)


# --- Migration ---


class MigrationError(MuxDBError):
    """Raised when a live migration operation fails."""

    def __init__(
        self,
        message: str,
        *,
        source_shard: str | None = None,
        dest_shard: str | None = None,
        details: dict | None = None,
    ) -> None:
        self.source_shard = source_shard
        self.dest_shard = dest_shard
        extra = {}
        if source_shard:
            extra["source_shard"] = source_shard
        if dest_shard:
            extra["dest_shard"] = dest_shard
        if details:
            extra.update(details)
        super().__init__(message, details=extra)


# --- Security ---


class SecurityError(MuxDBError):
    """Raised when security validation or authentication fails."""


# --- Resilience ---


class BulkheadLimitExceeded(MuxDBError):
    """Raised when the concurrency limit (bulkhead) for a resource is exceeded."""


