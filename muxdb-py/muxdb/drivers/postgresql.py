"""
PostgreSQL driver using psycopg (v3).

Provides both sync and async adapters for PostgreSQL backends.
Requires the ``psycopg`` optional dependency: ``pip install muxdb[psycopg]``
"""

from __future__ import annotations

import time
from typing import Any, Sequence

import structlog

from muxdb.drivers.base import BaseDriver, AsyncBaseDriver, QueryResult
from muxdb.errors import DriverError, ConnectionError as MuxConnectionError
from muxdb.shard_map import ShardInfo

logger = structlog.get_logger(__name__)


class PsycopgDriver(BaseDriver):
    """Synchronous PostgreSQL driver using ``psycopg``."""

    @property
    def backend_name(self) -> str:
        return "postgresql"

    def connect(self, shard: ShardInfo) -> Any:
        try:
            import psycopg

            conn = psycopg.connect(
                host=shard.host,
                port=shard.port,
                dbname=shard.database,
                user=getattr(shard, "_username", None),
                password=getattr(shard, "_password", None),
                autocommit=True,
            )
            logger.debug("psycopg.connect", shard_id=shard.id, host=shard.host)
            return conn
        except ImportError as exc:
            raise DriverError(
                "psycopg is not installed. Run: pip install muxdb[psycopg]",
                backend="postgresql",
                shard_id=shard.id,
            ) from exc
        except Exception as exc:
            raise MuxConnectionError(
                f"Failed to connect to PostgreSQL shard '{shard.id}' at "
                f"{shard.host}:{shard.port}/{shard.database}: {exc}",
                shard_id=shard.id,
                backend="postgresql",
            ) from exc

    def close(self, connection: Any) -> None:
        try:
            connection.close()
        except Exception as exc:
            logger.warning("psycopg.close_error", error=str(exc))

    def execute(
        self,
        connection: Any,
        query: str,
        params: Sequence[Any] = (),
    ) -> QueryResult:
        t0 = time.monotonic()
        shard_id = getattr(connection, "_muxdb_shard_id", "unknown")

        try:
            with connection.cursor() as cur:
                cur.execute(query, params or None)

                columns: list[str] = []
                rows: list[dict[str, Any]] = []

                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    raw_rows = cur.fetchall()
                    rows = [dict(zip(columns, row)) for row in raw_rows]

                elapsed_ms = (time.monotonic() - t0) * 1000

                return QueryResult(
                    rows=rows,
                    row_count=cur.rowcount if cur.rowcount >= 0 else len(rows),
                    columns=columns,
                    shard_id=shard_id,
                    elapsed_ms=elapsed_ms,
                )

        except Exception as exc:
            raise DriverError(
                f"PostgreSQL query failed on shard '{shard_id}': {exc}",
                shard_id=shard_id,
                backend="postgresql",
            ) from exc

    def begin(self, connection: Any) -> None:
        connection.autocommit = False

    def commit(self, connection: Any) -> None:
        connection.commit()
        connection.autocommit = True

    def rollback(self, connection: Any) -> None:
        connection.rollback()
        connection.autocommit = True

    def ping(self, connection: Any) -> bool:
        try:
            with connection.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception:
            return False


class AsyncPsycopgDriver(AsyncBaseDriver):
    """Asynchronous PostgreSQL driver using ``psycopg`` async support."""

    @property
    def backend_name(self) -> str:
        return "postgresql"

    async def connect(self, shard: ShardInfo) -> Any:
        try:
            import psycopg

            conn = await psycopg.AsyncConnection.connect(
                host=shard.host,
                port=shard.port,
                dbname=shard.database,
                autocommit=True,
            )
            logger.debug("async_psycopg.connect", shard_id=shard.id)
            return conn
        except ImportError as exc:
            raise DriverError(
                "psycopg is not installed. Run: pip install muxdb[psycopg]",
                backend="postgresql",
                shard_id=shard.id,
            ) from exc
        except Exception as exc:
            raise MuxConnectionError(
                f"Failed to async-connect to PostgreSQL shard '{shard.id}': {exc}",
                shard_id=shard.id,
                backend="postgresql",
            ) from exc

    async def close(self, connection: Any) -> None:
        try:
            await connection.close()
        except Exception as exc:
            logger.warning("async_psycopg.close_error", error=str(exc))

    async def execute(
        self,
        connection: Any,
        query: str,
        params: Sequence[Any] = (),
    ) -> QueryResult:
        t0 = time.monotonic()
        shard_id = getattr(connection, "_muxdb_shard_id", "unknown")

        try:
            async with connection.cursor() as cur:
                await cur.execute(query, params or None)

                columns: list[str] = []
                rows: list[dict[str, Any]] = []

                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    raw_rows = await cur.fetchall()
                    rows = [dict(zip(columns, row)) for row in raw_rows]

                elapsed_ms = (time.monotonic() - t0) * 1000

                return QueryResult(
                    rows=rows,
                    row_count=cur.rowcount if cur.rowcount >= 0 else len(rows),
                    columns=columns,
                    shard_id=shard_id,
                    elapsed_ms=elapsed_ms,
                )

        except Exception as exc:
            raise DriverError(
                f"Async PostgreSQL query failed on shard '{shard_id}': {exc}",
                shard_id=shard_id,
                backend="postgresql",
            ) from exc

    async def begin(self, connection: Any) -> None:
        connection.autocommit = False

    async def commit(self, connection: Any) -> None:
        await connection.commit()
        connection.autocommit = True

    async def rollback(self, connection: Any) -> None:
        await connection.rollback()
        connection.autocommit = True
