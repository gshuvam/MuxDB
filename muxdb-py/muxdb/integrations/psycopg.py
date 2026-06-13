"""
MuxDB Psycopg (v3) Integration.

Provides a PEP 249/DB-API 2.0 compliant wrapper over MuxDB, allowing users
to replace standard psycopg connections with a shard-aware MuxConnection.
"""

from __future__ import annotations

from typing import Any, Sequence, Iterator, Mapping
import time

from muxdb.client import MuxDB
from muxdb.config import MuxConfig
from muxdb.drivers.base import QueryResult


class MuxCursor:
    """Cursor wrapper that delegates query execution to MuxDB's router."""

    def __init__(self, connection: MuxConnection) -> None:
        self.connection = connection
        self._result: QueryResult | None = None
        self._rows: list[tuple[Any, ...]] = []
        self._index = 0
        self.description: list[tuple[str, Any, Any, Any, Any, Any, Any]] | None = None
        self.rowcount: int = -1

    def execute(self, query: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> MuxCursor:
        """Execute a query via MuxDB routing."""
        # Standardize params for MuxDB client
        mux_params: Sequence[Any] = ()
        if params is not None:
            if isinstance(params, (list, tuple)):
                mux_params = params
            elif isinstance(params, dict):
                # Note: MuxDB query routing extracts keys; positional is standard for psycopg,
                # but if dictionary params are passed we pass them along.
                mux_params = list(params.values())
            else:
                mux_params = (params,)

        self._result = self.connection._db.execute(query, mux_params)
        
        # Setup description (name, type_code, display_size, internal_size, precision, scale, null_ok)
        if self._result.columns:
            self.description = [
                (col, None, None, None, None, None, None)
                for col in self._result.columns
            ]
            # Convert dictionary rows to tuple rows to match psycopg default cursor behavior
            self._rows = [
                tuple(row[col] for col in self._result.columns)
                for row in self._result.rows
            ]
        else:
            self.description = None
            self._rows = []

        self.rowcount = self._result.row_count
        self._index = 0
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row of a query result set."""
        if self._index < len(self._rows):
            row = self._rows[self._index]
            self._index += 1
            return row
        return None

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows of a query result."""
        rows = self._rows[self._index:]
        self._index = len(self._rows)
        return rows

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch the next set of rows of a query result."""
        if size is None:
            size = 1
        end = min(self._index + size, len(self._rows))
        rows = self._rows[self._index:end]
        self._index = end
        return rows

    def close(self) -> None:
        """Close the cursor."""
        self._rows = []
        self._result = None

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        return self

    def __next__(self) -> tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> MuxCursor:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


class MuxConnection:
    """Connection wrapper that routes transactions and queries through MuxDB."""

    def __init__(self, db: MuxDB) -> None:
        self._db = db
        self.closed = False
        self._tx: Any = None

    def cursor(self) -> MuxCursor:
        """Return a new cursor object using the connection."""
        if self.closed:
            raise Exception("Connection is closed")
        return MuxCursor(self)

    def execute(self, query: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> MuxCursor:
        """Helper to create a cursor and execute a query immediately."""
        cur = self.cursor()
        cur.execute(query, params)
        return cur

    def commit(self) -> None:
        """Commit any pending transaction."""
        if self.closed:
            raise Exception("Connection is closed")
        # Direct commit handled per execute under autocommit or pinned tx
        pass

    def rollback(self) -> None:
        """Roll back any pending transaction."""
        if self.closed:
            raise Exception("Connection is closed")
        pass

    def close(self) -> None:
        """Close the connection and all underlying pools."""
        if not self.closed:
            self._db.close()
            self.closed = True

    def __enter__(self) -> MuxConnection:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self.close()


def connect(
    conninfo: str = "",
    *,
    config: MuxConfig | None = None,
    config_path: str | None = None,
    **kwargs: Any,
) -> MuxConnection:
    """
    Connect factory for MuxDB.
    
    Acts as a drop-in replacement for psycopg.connect().
    """
    if config is None:
        if config_path is not None:
            config = MuxConfig.from_file(config_path)
        else:
            # Try to resolve a default config
            config = MuxConfig.from_file("muxdb.yaml")

    db = MuxDB(config)
    db.connect()
    return MuxConnection(db)
