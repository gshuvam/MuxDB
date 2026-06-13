from __future__ import annotations

from typing import Set

class PlacementEngine:
    """Manages E-Store style two-tier placement and dynamic hot-key caching in Redis."""

    def __init__(self, cache_shard_id: str = "redis-cache") -> None:
        self.cache_shard_id = cache_shard_id
        self._cached_keys: Set[str] = set()

    def is_cached(self, key: str) -> bool:
        """Check if a key is currently promoted to the cache tier."""
        return key in self._cached_keys

    def promote_to_cache(self, key: str) -> None:
        """Promote a hot key to the cache tier."""
        self._cached_keys.add(key)

    def evict_from_cache(self, key: str) -> None:
        """Evict a key from the cache tier."""
        self._cached_keys.discard(key)

    def resolve_read_target(self, key: str, default_shard_id: str) -> str:
        """Resolve the read target for a key. Returns cache ID if hot, else default shard ID."""
        if self.is_cached(key):
            return self.cache_shard_id
        return default_shard_id

    def resolve_write_target(self, key: str, default_shard_id: str) -> str:
        """Resolve write target (backend) and evict key from cache for consistency."""
        self.evict_from_cache(key)
        return default_shard_id
