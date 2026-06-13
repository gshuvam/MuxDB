"""
Shard map — the in-memory routing table.

Implements three strategies for mapping keys to shards:
  - **Hash**: Simple modulo hashing (fast, uniform distribution)
  - **Range**: Explicit key ranges per shard (ordered data, range queries)
  - **Consistent Hash**: Virtual-node consistent hashing (minimal disruption on topology change)

The shard map supports atomic topology swaps via versioned snapshots,
implementing the Slicer bridge-lease model where unchanged key assignments
remain live during transitions.
"""

from __future__ import annotations

import hashlib
import threading
from bisect import bisect_right, insort
from dataclasses import dataclass, field
from typing import Sequence

import structlog

from muxdb.config import MuxConfig, ShardConfig
from muxdb.errors import ShardNotFoundError, ConfigError

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class ShardInfo:
    """Snapshot of a shard's identity and connection metadata."""

    id: str
    backend: str
    host: str
    port: int
    database: str
    weight: int = 1
    dsn: str = ""

    @classmethod
    def from_config(cls, cfg: ShardConfig) -> ShardInfo:
        return cls(
            id=cfg.id,
            backend=cfg.backend,
            host=cfg.host,
            port=cfg.port,
            database=cfg.database,
            weight=cfg.weight,
            dsn=cfg.dsn,
        )


class ShardMap:
    """
    In-memory routing table mapping keys to shards.

    Thread-safe. Supports atomic topology swaps via :meth:`swap`.

    Example::

        shard_map = ShardMap.from_config(config)
        target = shard_map.resolve(user_id=42)
        print(target.id)  # "shard_2"
    """

    def __init__(
        self,
        shards: list[ShardInfo],
        strategy: str,
        virtual_nodes: int = 256,
    ) -> None:
        self._lock = threading.RLock()
        self._version = 0
        self._strategy = strategy
        self._virtual_nodes = virtual_nodes
        self._shards: list[ShardInfo] = list(shards)
        self._shard_index: dict[str, ShardInfo] = {s.id: s for s in shards}

        # Strategy-specific structures
        self._ring: list[tuple[int, str]] = []  # (hash_position, shard_id)
        self._ring_positions: list[int] = []  # sorted positions for bisect

        if strategy == "consistent_hash":
            self._build_ring()

    @classmethod
    def from_config(cls, config: MuxConfig) -> ShardMap:
        """Build a shard map from MuxConfig."""
        shards = [ShardInfo.from_config(s) for s in config.shards]
        return cls(
            shards=shards,
            strategy=config.cluster.strategy,
            virtual_nodes=config.cluster.virtual_nodes,
        )

    # --- Ring management (consistent hashing) ---

    def _build_ring(self) -> None:
        """Build the consistent hash ring with virtual nodes."""
        self._ring.clear()
        self._ring_positions.clear()

        for shard in self._shards:
            for vn in range(shard.weight * self._virtual_nodes):
                token = f"{shard.id}:vn{vn}"
                pos = self._hash(token)
                insort(self._ring, (pos, shard.id))

        self._ring_positions = [pos for pos, _ in self._ring]

    @staticmethod
    def _hash(key: str | int) -> int:
        """Deterministic hash using MD5 (not security-sensitive — just distribution)."""
        raw = str(key).encode("utf-8")
        digest = hashlib.md5(raw, usedforsecurity=False).hexdigest()
        return int(digest[:8], 16)

    # --- Resolution ---

    def resolve(self, key: str | int) -> ShardInfo:
        """Resolve a shard key to its owning shard.

        Args:
            key: The shard key value (e.g. user_id=42).

        Returns:
            The :class:`ShardInfo` that owns this key.

        Raises:
            ShardNotFoundError: If no shard can be resolved.
        """
        with self._lock:
            if self._strategy == "hash":
                return self._resolve_hash(key)
            elif self._strategy == "range":
                return self._resolve_range(key)
            elif self._strategy == "consistent_hash":
                return self._resolve_consistent(key)
            else:
                raise ConfigError(f"Unknown strategy: {self._strategy}")

    def _resolve_hash(self, key: str | int) -> ShardInfo:
        """Simple modulo hash across shards."""
        if not self._shards:
            raise ShardNotFoundError(key)

        h = self._hash(key)
        # Weight-aware: build weighted list
        weighted: list[ShardInfo] = []
        for shard in self._shards:
            weighted.extend([shard] * shard.weight)

        if not weighted:
            raise ShardNotFoundError(key)

        idx = h % len(weighted)
        return weighted[idx]

    def _resolve_range(self, key: str | int) -> ShardInfo:
        """Range-based resolution: find the shard whose range contains the key."""
        try:
            numeric_key = int(key)
        except (ValueError, TypeError) as exc:
            raise ShardNotFoundError(
                key,
                details={"reason": "Range strategy requires an integer key"},
            ) from exc

        for shard_info in self._shards:
            # Look up the range from the shard config mapping
            cfg_shard = self._find_config_shard(shard_info.id)
            if cfg_shard and cfg_shard.range and cfg_shard.range.contains(numeric_key):
                return shard_info

        raise ShardNotFoundError(key, details={"reason": "Key not in any shard range"})

    def _find_config_shard(self, shard_id: str) -> ShardConfig | None:
        """Find the ShardConfig for range lookups (stored separately)."""
        # Range data is stored on ShardConfig but ShardInfo is lightweight.
        # We store range data on _range_map during construction.
        return self._range_map.get(shard_id)

    def _resolve_consistent(self, key: str | int) -> ShardInfo:
        """Consistent hash ring lookup."""
        if not self._ring:
            raise ShardNotFoundError(key)

        h = self._hash(key)
        idx = bisect_right(self._ring_positions, h) % len(self._ring)
        _, shard_id = self._ring[idx]
        return self._shard_index[shard_id]

    # --- Multi-key resolution ---

    def resolve_many(self, keys: Sequence[str | int]) -> dict[str, list[str | int]]:
        """Resolve multiple keys, grouping them by target shard.

        Returns:
            A dict mapping shard_id → list of keys that route to that shard.
        """
        groups: dict[str, list[str | int]] = {}
        for key in keys:
            shard = self.resolve(key)
            groups.setdefault(shard.id, []).append(key)
        return groups

    # --- Topology management ---

    def swap(self, new_shards: list[ShardInfo]) -> int:
        """Atomically swap the shard topology.

        Implements the Slicer bridge-lease model: the swap is instantaneous
        from the perspective of concurrent :meth:`resolve` calls. Unchanged
        key assignments remain valid; only reassigned keys experience the brief
        routing update.

        Returns:
            The new version number.
        """
        with self._lock:
            self._version += 1
            old_ids = {s.id for s in self._shards}
            new_ids = {s.id for s in new_shards}

            self._shards = list(new_shards)
            self._shard_index = {s.id: s for s in new_shards}

            if self._strategy == "consistent_hash":
                self._build_ring()

            added = new_ids - old_ids
            removed = old_ids - new_ids

            logger.info(
                "shard_map.swap",
                version=self._version,
                total_shards=len(new_shards),
                added=list(added) if added else None,
                removed=list(removed) if removed else None,
            )

            return self._version

    @property
    def version(self) -> int:
        """Current topology version number."""
        with self._lock:
            return self._version

    @property
    def shards(self) -> list[ShardInfo]:
        """List of current shards (snapshot)."""
        with self._lock:
            return list(self._shards)

    @property
    def shard_ids(self) -> list[str]:
        """List of current shard IDs."""
        with self._lock:
            return [s.id for s in self._shards]

    def get_shard(self, shard_id: str) -> ShardInfo:
        """Look up a shard by ID."""
        with self._lock:
            shard = self._shard_index.get(shard_id)
            if shard is None:
                raise ShardNotFoundError(
                    shard_id, details={"reason": "Shard ID not in shard map"}
                )
            return shard

    def __len__(self) -> int:
        with self._lock:
            return len(self._shards)

    def __repr__(self) -> str:
        return (
            f"ShardMap(strategy={self._strategy!r}, "
            f"shards={len(self._shards)}, "
            f"version={self._version})"
        )


class RangeShardMap(ShardMap):
    """ShardMap subclass with explicit range data stored alongside shard info.

    Used internally when strategy='range' to keep range metadata accessible
    for resolution without coupling ShardInfo to config details.
    """

    def __init__(
        self,
        shards: list[ShardInfo],
        shard_configs: list[ShardConfig],
        virtual_nodes: int = 256,
    ) -> None:
        self._range_map: dict[str, ShardConfig] = {s.id: s for s in shard_configs}
        super().__init__(shards=shards, strategy="range", virtual_nodes=virtual_nodes)

    @classmethod
    def from_config(cls, config: MuxConfig) -> RangeShardMap:  # type: ignore[override]
        shards = [ShardInfo.from_config(s) for s in config.shards]
        return cls(
            shards=shards,
            shard_configs=list(config.shards),
            virtual_nodes=config.cluster.virtual_nodes,
        )


def create_shard_map(config: MuxConfig) -> ShardMap:
    """Factory: create the appropriate ShardMap for the configured strategy."""
    if config.cluster.strategy == "range":
        return RangeShardMap.from_config(config)
    return ShardMap.from_config(config)
