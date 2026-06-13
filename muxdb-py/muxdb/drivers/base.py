"""
Base driver interface.

All MuxDB backend drivers implement this abstract interface, allowing the
core router and pool to work identically regardless of the underlying
database technology (PostgreSQL, MongoDB, Redis, Qdrant, etc.).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Sequence

from muxdb.shard_map import ShardInfo


@dataclass
class QueryResult:
    """Uniform result container across all drivers."""

    rows: list[dict[str, Any]]
    row_count: int
    columns: list[str]
    shard_id: str
    elapsed_ms: float = 0.0

    @property
    def scalar(self) -> Any:
        """Return the first column of the first row, or None."""
        if self.rows and self.columns:
            return self.rows[0].get(self.columns[0])
        return None

    @property
    def empty(self) -> bool:
        return self.row_count == 0


class BaseDriver(ABC):
    """Abstract base class for all MuxDB backend drivers.

    Subclasses must implement connection lifecycle and query execution
    for their specific database technology.
    """

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """The driver's backend identifier (e.g. 'postgresql', 'mongodb')."""
        ...

    @abstractmethod
    def connect(self, shard: ShardInfo) -> Any:
        """Create a raw connection to the given shard.

        Returns:
            A raw connection object (driver-specific type).
        """
        ...

    @abstractmethod
    def close(self, connection: Any) -> None:
        """Close a raw connection."""
        ...

    @abstractmethod
    def execute(
        self,
        connection: Any,
        query: str,
        params: Sequence[Any] = (),
    ) -> QueryResult:
        """Execute a query on the given connection.

        Args:
            connection: A raw connection from :meth:`connect`.
            query: The query string (SQL, or backend-specific format).
            params: Query parameters.

        Returns:
            A :class:`QueryResult` with the results.
        """
        ...

    @abstractmethod
    def begin(self, connection: Any) -> None:
        """Begin a transaction on the connection."""
        ...

    @abstractmethod
    def commit(self, connection: Any) -> None:
        """Commit the current transaction."""
        ...

    @abstractmethod
    def rollback(self, connection: Any) -> None:
        """Roll back the current transaction."""
        ...

    def ping(self, connection: Any) -> bool:
        """Health check — verify the connection is alive.

        Default implementation attempts a lightweight query. Drivers should
        override with backend-specific health checks.
        """
        try:
            self.execute(connection, "SELECT 1")
            return True
        except Exception:
            return False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(backend={self.backend_name!r})"


class AsyncBaseDriver(ABC):
    """Async variant of :class:`BaseDriver` for asyncio-based backends."""

    @property
    @abstractmethod
    def backend_name(self) -> str:
        ...

    @abstractmethod
    async def connect(self, shard: ShardInfo) -> Any:
        ...

    @abstractmethod
    async def close(self, connection: Any) -> None:
        ...

    @abstractmethod
    async def execute(
        self,
        connection: Any,
        query: str,
        params: Sequence[Any] = (),
    ) -> QueryResult:
        ...

    @abstractmethod
    async def begin(self, connection: Any) -> None:
        ...

    @abstractmethod
    async def commit(self, connection: Any) -> None:
        ...

    @abstractmethod
    async def rollback(self, connection: Any) -> None:
        ...

    async def ping(self, connection: Any) -> bool:
        try:
            await self.execute(connection, "SELECT 1")
            return True
        except Exception:
            return False
