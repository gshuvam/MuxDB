"""
MuxDB asyncpg Integration.

Provides drop-in asynchronous connection and pool wrappers for asyncpg,
routing queries transparently across shards.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Sequence, Generator, AsyncGenerator

from muxdb.client import MuxDB
from muxdb.config import MuxConfig
from muxdb.router import QueryContext, RouteType


class MuxAsyncConnection:
    """Connection wrapper for asyncpg that routes queries through MuxDB."""

    def __init__(self, db: MuxDB, conns: dict[str, Any]) -> None:
        self._db = db
        self._conns = conns  # shard_id -> asyncpg.Connection
        self._closed = False

    async def execute(self, query: str, *args: Any, shard_key: Any = None) -> str:
        """Execute a query and return a status string."""
        if self._closed:
            raise Exception("Connection is closed")

        ctx = QueryContext(query=query, params=args, shard_key_value=shard_key)
        decision = self._db.router.route(ctx)

        if decision.type == RouteType.SINGLE:
            conn = self._conns[decision.targets[0].id]
            return await conn.execute(query, *args)
        elif decision.type == RouteType.BROADCAST:
            tasks = [self._conns[t.id].execute(query, *args) for t in decision.targets]
            res = await asyncio.gather(*tasks)
            return res[0] if res else ""
        else:
            tasks = [self._conns[t.id].execute(query, *args) for t in decision.targets]
            res = await asyncio.gather(*tasks)
            return res[-1] if res else ""

    async def fetch(self, query: str, *args: Any, shard_key: Any = None) -> list[Any]:
        """Fetch rows from the database."""
        if self._closed:
            raise Exception("Connection is closed")

        ctx = QueryContext(query=query, params=args, shard_key_value=shard_key)
        decision = self._db.router.route(ctx)

        if decision.type == RouteType.SINGLE:
            conn = self._conns[decision.targets[0].id]
            return await conn.fetch(query, *args)
        else:
            tasks = [self._conns[t.id].fetch(query, *args) for t in decision.targets]
            results = await asyncio.gather(*tasks)
            merged = []
            for res in results:
                merged.extend(res)
            return merged

    async def fetchrow(self, query: str, *args: Any, shard_key: Any = None) -> Any | None:
        """Fetch a single row from the database."""
        rows = await self.fetch(query, *args, shard_key=shard_key)
        return rows[0] if rows else None

    async def fetchval(self, query: str, *args: Any, column: int = 0, shard_key: Any = None) -> Any | None:
        """Fetch a single scalar value."""
        row = await self.fetchrow(query, *args, shard_key=shard_key)
        if row:
            return row[column]
        return None

    async def close(self) -> None:
        """Close the connection to all shards."""
        if not self._closed:
            await asyncio.gather(*(conn.close() for conn in self._conns.values()))
            self._closed = True


class MuxAsyncPool:
    """Connection pool wrapper for asyncpg that routes queries through MuxDB."""

    def __init__(self, db: MuxDB, pools: dict[str, Any]) -> None:
        self._db = db
        self._pools = pools  # shard_id -> asyncpg.Pool
        self._closed = False

    async def execute(self, query: str, *args: Any, shard_key: Any = None) -> str:
        """Execute a query on the pool."""
        ctx = QueryContext(query=query, params=args, shard_key_value=shard_key)
        decision = self._db.router.route(ctx)

        if decision.type == RouteType.SINGLE:
            pool = self._pools[decision.targets[0].id]
            return await pool.execute(query, *args)
        elif decision.type == RouteType.BROADCAST:
            tasks = [self._pools[t.id].execute(query, *args) for t in decision.targets]
            res = await asyncio.gather(*tasks)
            return res[0] if res else ""
        else:
            tasks = [self._pools[t.id].execute(query, *args) for t in decision.targets]
            res = await asyncio.gather(*tasks)
            return res[-1] if res else ""

    async def fetch(self, query: str, *args: Any, shard_key: Any = None) -> list[Any]:
        """Fetch rows on the pool."""
        ctx = QueryContext(query=query, params=args, shard_key_value=shard_key)
        decision = self._db.router.route(ctx)

        if decision.type == RouteType.SINGLE:
            pool = self._pools[decision.targets[0].id]
            return await pool.fetch(query, *args)
        else:
            tasks = [self._pools[t.id].fetch(query, *args) for t in decision.targets]
            results = await asyncio.gather(*tasks)
            merged = []
            for res in results:
                merged.extend(res)
            return merged

    async def fetchrow(self, query: str, *args: Any, shard_key: Any = None) -> Any | None:
        """Fetch a single row on the pool."""
        rows = await self.fetch(query, *args, shard_key=shard_key)
        return rows[0] if rows else None

    async def fetchval(self, query: str, *args: Any, column: int = 0, shard_key: Any = None) -> Any | None:
        """Fetch a single scalar value on the pool."""
        row = await self.fetchrow(query, *args, shard_key=shard_key)
        if row:
            return row[column]
        return None

    @asynccontextmanager
    async def acquire(
        self,
        *,
        shard_key: Any = None,
        shard_id: str | None = None,
        timeout: float | None = None,
    ) -> AsyncGenerator[Any, None]:
        """Acquire a raw connection from the specified shard pool."""
        if self._closed:
            raise Exception("Pool is closed")

        if shard_id:
            target_id = shard_id
        elif shard_key is not None:
            target_id = self._db.shard_map.resolve(shard_key).id
        else:
            raise ValueError("acquire() requires either shard_key or shard_id")

        pool = self._pools.get(target_id)
        if not pool:
            raise ValueError(f"No pool found for shard '{target_id}'")

        async with pool.acquire(timeout=timeout) as conn:
            yield conn

    async def close(self) -> None:
        """Close all pools."""
        if not self._closed:
            await asyncio.gather(*(pool.close() for pool in self._pools.values()))
            self._closed = True


async def connect(
    dsn: str = "",
    *,
    config: MuxConfig | None = None,
    config_path: str | None = None,
    **kwargs: Any,
) -> MuxAsyncConnection:
    """Connect to a sharded cluster and return a MuxAsyncConnection."""
    try:
        import asyncpg
    except ImportError as exc:
        raise ImportError("asyncpg is not installed. Run: pip install asyncpg") from exc

    if config is None:
        if config_path is not None:
            config = MuxConfig.from_file(config_path)
        else:
            config = MuxConfig.from_file("muxdb.yaml")

    db = MuxDB(config)
    db._shard_map = create_shard_map(config)
    db._router = Router(config, db._shard_map)
    db._connected = True

    conns = {}
    for shard in db.shard_map.shards:
        conn = await asyncpg.connect(
            host=shard.host,
            port=shard.port,
            database=shard.database,
            user=getattr(shard, "_username", None),
            password=getattr(shard, "_password", None),
            **kwargs,
        )
        conns[shard.id] = conn

    return MuxAsyncConnection(db, conns)


async def create_pool(
    dsn: str = "",
    *,
    config: MuxConfig | None = None,
    config_path: str | None = None,
    min_size: int = 2,
    max_size: int = 10,
    **kwargs: Any,
) -> MuxAsyncPool:
    """Create a sharded pool of connections."""
    try:
        import asyncpg
    except ImportError as exc:
        raise ImportError("asyncpg is not installed. Run: pip install asyncpg") from exc

    if config is None:
        if config_path is not None:
            config = MuxConfig.from_file(config_path)
        else:
            config = MuxConfig.from_file("muxdb.yaml")

    db = MuxDB(config)
    db._shard_map = create_shard_map(config)
    db._router = Router(config, db._shard_map)
    db._connected = True

    pools = {}
    for shard in db.shard_map.shards:
        pool = await asyncpg.create_pool(
            host=shard.host,
            port=shard.port,
            database=shard.database,
            user=getattr(shard, "_username", None),
            password=getattr(shard, "_password", None),
            min_size=min_size,
            max_size=max_size,
            **kwargs,
        )
        pools[shard.id] = pool

    return MuxAsyncPool(db, pools)
