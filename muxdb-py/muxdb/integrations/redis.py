"""
MuxDB Redis Integration.

Provides a sharded Redis client (MuxRedis) that implements transparent
consistent-hash key routing, support for hash tags (e.g., '{tag}key'),
and multi-key command grouping.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Sequence

from muxdb.client import MuxDB
from muxdb.config import MuxConfig

# Match content inside curly braces: e.g., "{user_123}:profile" -> "user_123"
HASH_TAG_PATTERN = re.compile(r"\{([^}]+)\}")


def extract_routing_key(key: Any) -> str:
    """Extract the routing key from a Redis key, supporting hash tags ({tag})."""
    key_str = str(key)
    match = HASH_TAG_PATTERN.search(key_str)
    if match:
        return match.group(1)
    return key_str


class MuxRedis:
    """
    Sharded Redis client.
    
    Transparently distributes keys across multiple Redis backends using MuxDB's Consistent Hash ShardMap.
    Supports standard Redis commands and groups multi-key operations (e.g. MGET, MSET, DEL) by shard.
    """

    def __init__(self, db: MuxDB, clients: Dict[str, Any]) -> None:
        self._db = db
        self._clients = clients  # shard_id -> redis.Redis

    @classmethod
    def from_config(cls, config: MuxConfig, **kwargs: Any) -> MuxRedis:
        """Create a MuxRedis client directly from a MuxConfig."""
        try:
            import redis
        except ImportError as exc:
            raise ImportError("redis is not installed. Run: pip install redis") from exc

        db = MuxDB(config)
        db.connect()

        clients = {}
        for shard in db.shard_map.shards:
            # Connect using host, port, db
            clients[shard.id] = redis.Redis(
                host=shard.host,
                port=shard.port,
                db=int(shard.database) if shard.database.isdigit() else 0,
                **kwargs,
            )

        return cls(db, clients)

    def _get_client(self, key: Any) -> Any:
        """Resolve a key to its corresponding shard client."""
        routing_key = extract_routing_key(key)
        shard = self._db.router.route_key(routing_key)
        return self._clients[shard.id]

    def get(self, key: Any) -> Any:
        """GET key from routed shard."""
        client = self._get_client(key)
        return client.get(key)

    def set(self, key: Any, value: Any, *args: Any, **kwargs: Any) -> Any:
        """SET key value on routed shard."""
        client = self._get_client(key)
        return client.set(key, value, *args, **kwargs)

    def delete(self, *keys: Any) -> int:
        """DEL keys from routed shards, grouping keys by shard to minimize network calls."""
        if not keys:
            return 0

        # Group keys by shard ID
        grouped: Dict[str, List[Any]] = {}
        for key in keys:
            routing_key = extract_routing_key(key)
            shard_id = self._db.router.route_key(routing_key).id
            grouped.setdefault(shard_id, []).append(key)

        total_deleted = 0
        for shard_id, shard_keys in grouped.items():
            client = self._clients[shard_id]
            total_deleted += client.delete(*shard_keys)

        return total_deleted

    def exists(self, *keys: Any) -> int:
        """EXISTS keys on routed shards."""
        if not keys:
            return 0

        grouped: Dict[str, List[Any]] = {}
        for key in keys:
            routing_key = extract_routing_key(key)
            shard_id = self._db.router.route_key(routing_key).id
            grouped.setdefault(shard_id, []).append(key)

        total_exists = 0
        for shard_id, shard_keys in grouped.items():
            client = self._clients[shard_id]
            total_exists += client.exists(*shard_keys)

        return total_exists

    def mget(self, keys: Sequence[Any]) -> List[Any]:
        """MGET keys by grouping them across shards and combining results in original order."""
        if not keys:
            return []

        # Map key to its index to reconstruct original order
        key_indices = {key: idx for idx, key in enumerate(keys)}
        results: List[Any] = [None] * len(keys)

        grouped: Dict[str, List[Any]] = {}
        for key in keys:
            routing_key = extract_routing_key(key)
            shard_id = self._db.router.route_key(routing_key).id
            grouped.setdefault(shard_id, []).append(key)

        for shard_id, shard_keys in grouped.items():
            client = self._clients[shard_id]
            shard_results = client.mget(shard_keys)
            for key, val in zip(shard_keys, shard_results):
                idx = key_indices[key]
                results[idx] = val

        return results

    def mset(self, mapping: Mapping[Any, Any]) -> bool:
        """MSET mapping by grouping keys across shards."""
        if not mapping:
            return True

        grouped: Dict[str, Dict[Any, Any]] = {}
        for key, val in mapping.items():
            routing_key = extract_routing_key(key)
            shard_id = self._db.router.route_key(routing_key).id
            grouped.setdefault(shard_id, {})[key] = val

        all_ok = True
        for shard_id, shard_mapping in grouped.items():
            client = self._clients[shard_id]
            ok = client.mset(shard_mapping)
            if not ok:
                all_ok = False

        return all_ok

    def flushall(self) -> bool:
        """Flush all keys across all shards (broadcast)."""
        all_ok = True
        for client in self._clients.values():
            ok = client.flushdb()
            if not ok:
                all_ok = False
        return all_ok

    def close(self) -> None:
        """Close connections to all shards."""
        for client in self._clients.values():
            try:
                # Connection pool is closed
                client.close()
            except Exception:
                pass
        self._db.close()
