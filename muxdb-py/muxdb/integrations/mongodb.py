"""
MuxDB MongoDB Integration.

Provides MuxMongoClient (sync) and MuxMotorClient (async) drop-in wrappers,
enabling document-level sharded routing and scatter-gather query execution.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Mapping, Sequence, Generator, AsyncGenerator

from muxdb.client import MuxDB
from muxdb.config import MuxConfig


class MuxCollection:
    """Wrapped sync MongoDB collection routing operations to shards."""

    def __init__(self, db_wrapper: MuxDatabase, name: str) -> None:
        self.db_wrapper = db_wrapper
        self.name = name
        self._db = db_wrapper.client_wrapper._db
        self._shard_key = self._db.config.cluster.shard_key

    def _get_shard_collection(self, key_val: Any) -> Any:
        """Resolve shard and return the raw collection on that shard."""
        shard = self._db.router.route_key(key_val)
        client = self.db_wrapper.client_wrapper._clients[shard.id]
        return client[self.db_wrapper.name][self.name]

    def insert_one(self, document: Dict[str, Any], **kwargs: Any) -> Any:
        """Insert a document, routed by shard key."""
        val = document.get(self._shard_key)
        if val is None:
            raise ValueError(f"Document missing required shard key: {self._shard_key}")
        col = self._get_shard_collection(val)
        return col.insert_one(document, **kwargs)

    def insert_many(self, documents: Sequence[Dict[str, Any]], **kwargs: Any) -> Any:
        """Insert multiple documents, grouping them by shard target."""
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for doc in documents:
            val = doc.get(self._shard_key)
            if val is None:
                raise ValueError(f"Document missing required shard key: {self._shard_key}")
            shard_id = self._db.router.route_key(val).id
            grouped.setdefault(shard_id, []).append(doc)

        results = {}
        for shard_id, docs in grouped.items():
            client = self.db_wrapper.client_wrapper._clients[shard_id]
            col = client[self.db_wrapper.name][self.name]
            results[shard_id] = col.insert_many(docs, **kwargs)
        return results

    def find(self, filter: Dict[str, Any] | None = None, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        """Find documents, routing to single shard or performing scatter-gather."""
        filter_dict = filter or {}
        val = filter_dict.get(self._shard_key)

        if val is not None:
            col = self._get_shard_collection(val)
            return list(col.find(filter_dict, *args, **kwargs))

        # Scatter-gather across all shards
        results = []
        for client in self.db_wrapper.client_wrapper._clients.values():
            col = client[self.db_wrapper.name][self.name]
            results.extend(list(col.find(filter_dict, *args, **kwargs)))
        return results

    def find_one(self, filter: Dict[str, Any] | None = None, *args: Any, **kwargs: Any) -> Dict[str, Any] | None:
        """Find a single document."""
        filter_dict = filter or {}
        val = filter_dict.get(self._shard_key)

        if val is not None:
            col = self._get_shard_collection(val)
            return col.find_one(filter_dict, *args, **kwargs)

        # Scatter-gather first match
        for client in self.db_wrapper.client_wrapper._clients.values():
            col = client[self.db_wrapper.name][self.name]
            res = col.find_one(filter_dict, *args, **kwargs)
            if res:
                return res
        return None

    def update_one(self, filter: Dict[str, Any], update: Dict[str, Any], **kwargs: Any) -> Any:
        """Update a single document."""
        val = filter.get(self._shard_key)
        if val is None:
            raise ValueError(f"Filter missing required shard key: {self._shard_key}")
        col = self._get_shard_collection(val)
        return col.update_one(filter, update, **kwargs)

    def delete_one(self, filter: Dict[str, Any], **kwargs: Any) -> Any:
        """Delete a single document."""
        val = filter.get(self._shard_key)
        if val is None:
            raise ValueError(f"Filter missing required shard key: {self._shard_key}")
        col = self._get_shard_collection(val)
        return col.delete_one(filter, **kwargs)


class MuxDatabase:
    """Wrapped sync MongoDB database routing collections to MuxCollections."""

    def __init__(self, client_wrapper: MuxMongoClient, name: str) -> None:
        self.client_wrapper = client_wrapper
        self.name = name

    def __getitem__(self, name: str) -> MuxCollection:
        return MuxCollection(self, name)

    def __getattr__(self, name: str) -> MuxCollection:
        return self[name]


class MuxMongoClient:
    """Sharded sync MongoDB Client."""

    def __init__(self, db: MuxDB, clients: Dict[str, Any]) -> None:
        self._db = db
        self._clients = clients  # shard_id -> pymongo.MongoClient

    @classmethod
    def from_config(cls, config: MuxConfig, **kwargs: Any) -> MuxMongoClient:
        """Create a client directly from a MuxConfig."""
        try:
            import pymongo
        except ImportError as exc:
            raise ImportError("pymongo is not installed. Run: pip install pymongo") from exc

        db = MuxDB(config)
        db.connect()

        clients = {}
        for shard in db.shard_map.shards:
            # Construct connection string or connect directly
            uri = f"mongodb://{shard.host}:{shard.port}"
            clients[shard.id] = pymongo.MongoClient(uri, **kwargs)

        return cls(db, clients)

    def __getitem__(self, name: str) -> MuxDatabase:
        return MuxDatabase(self, name)

    def __getattr__(self, name: str) -> MuxDatabase:
        return self[name]

    def close(self) -> None:
        """Close connections to all shards."""
        for client in self._clients.values():
            client.close()
        self._db.close()


# ---------------------------------------------------------------------------
# Async Motor Client Wrapper
# ---------------------------------------------------------------------------

class MuxMotorCollection:
    """Wrapped async Motor collection routing operations to shards."""

    def __init__(self, db_wrapper: MuxMotorDatabase, name: str) -> None:
        self.db_wrapper = db_wrapper
        self.name = name
        self._db = db_wrapper.client_wrapper._db
        self._shard_key = self._db.config.cluster.shard_key

    def _get_shard_collection(self, key_val: Any) -> Any:
        shard = self._db.router.route_key(key_val)
        client = self.db_wrapper.client_wrapper._clients[shard.id]
        return client[self.db_wrapper.name][self.name]

    async def insert_one(self, document: Dict[str, Any], **kwargs: Any) -> Any:
        val = document.get(self._shard_key)
        if val is None:
            raise ValueError(f"Document missing required shard key: {self._shard_key}")
        col = self._get_shard_collection(val)
        return await col.insert_one(document, **kwargs)

    async def find_one(self, filter: Dict[str, Any] | None = None, *args: Any, **kwargs: Any) -> Dict[str, Any] | None:
        filter_dict = filter or {}
        val = filter_dict.get(self._shard_key)

        if val is not None:
            col = self._get_shard_collection(val)
            return await col.find_one(filter_dict, *args, **kwargs)

        # Scatter-gather async
        for client in self.db_wrapper.client_wrapper._clients.values():
            col = client[self.db_wrapper.name][self.name]
            res = await col.find_one(filter_dict, *args, **kwargs)
            if res:
                return res
        return None

    async def find(self, filter: Dict[str, Any] | None = None, *args: Any, **kwargs: Any) -> AsyncGenerator[Dict[str, Any], None]:
        """Async find returning merged list or generator."""
        filter_dict = filter or {}
        val = filter_dict.get(self._shard_key)

        if val is not None:
            col = self._get_shard_collection(val)
            cursor = col.find(filter_dict, *args, **kwargs)
            async for doc in cursor:
                yield doc
        else:
            # Parallel query gathers
            async def fetch_all(col_instance: Any) -> List[Dict[str, Any]]:
                cursor = col_instance.find(filter_dict, *args, **kwargs)
                return [doc async for doc in cursor]

            tasks = [
                fetch_all(client[self.db_wrapper.name][self.name])
                for client in self.db_wrapper.client_wrapper._clients.values()
            ]
            results = await asyncio.gather(*tasks)
            for res in results:
                for doc in res:
                    yield doc


class MuxMotorDatabase:
    """Wrapped async Motor database routing collections to MuxMotorCollections."""

    def __init__(self, client_wrapper: MuxMotorClient, name: str) -> None:
        self.client_wrapper = client_wrapper
        self.name = name

    def __getitem__(self, name: str) -> MuxMotorCollection:
        return MuxMotorCollection(self, name)

    def __getattr__(self, name: str) -> MuxMotorCollection:
        return self[name]


class MuxMotorClient:
    """Sharded async Motor Client."""

    def __init__(self, db: MuxDB, clients: Dict[str, Any]) -> None:
        self._db = db
        self._clients = clients  # shard_id -> motor.AsyncIOMotorClient

    @classmethod
    def from_config(cls, config: MuxConfig, **kwargs: Any) -> MuxMotorClient:
        """Create an async motor client from config."""
        try:
            from motor.motor_asyncio import AsyncIOMotorClient
        except ImportError as exc:
            raise ImportError("motor is not installed. Run: pip install motor") from exc

        db = MuxDB(config)
        db.connect()

        clients = {}
        for shard in db.shard_map.shards:
            uri = f"mongodb://{shard.host}:{shard.port}"
            clients[shard.id] = AsyncIOMotorClient(uri, **kwargs)

        return cls(db, clients)

    def __getitem__(self, name: str) -> MuxMotorDatabase:
        return MuxMotorDatabase(self, name)

    def __getattr__(self, name: str) -> MuxMotorDatabase:
        return self[name]

    def close(self) -> None:
        """Close connections."""
        for client in self._clients.values():
            client.close()
        self._db.close()
