"""
MuxDB Qdrant Integration.

Provides MuxQdrantClient to shard vector points across Qdrant instances,
supporting point-level payload routing and scatter-gather search aggregation.
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Sequence

from muxdb.client import MuxDB
from muxdb.config import MuxConfig


class MuxQdrantClient:
    """
    Sharded Qdrant Client.
    
    Routes points dynamically during upserts using point IDs or payload metadata,
    and merges vector search results from multiple shards using score-based sorting.
    """

    def __init__(self, db: MuxDB, clients: Dict[str, Any]) -> None:
        self._db = db
        self._clients = clients  # shard_id -> qdrant_client.QdrantClient
        self._shard_key = db.config.cluster.shard_key

    @classmethod
    def from_config(cls, config: MuxConfig, **kwargs: Any) -> MuxQdrantClient:
        """Create client from configuration."""
        try:
            import qdrant_client
        except ImportError as exc:
            raise ImportError("qdrant-client is not installed. Run: pip install qdrant-client") from exc

        db = MuxDB(config)
        db.connect()

        clients = {}
        for shard in db.shard_map.shards:
            clients[shard.id] = qdrant_client.QdrantClient(
                host=shard.host,
                port=shard.port,
                **kwargs,
            )

        return cls(db, clients)

    def _resolve_shard(self, point_id: Any, payload: Dict[str, Any] | None = None) -> str:
        """Resolve which shard a point belongs to."""
        # 1. Try to route by payload shard key
        if payload and self._shard_key in payload:
            return self._db.router.route_key(payload[self._shard_key]).id

        # 2. Fallback: hash the point ID
        # Convert ID to string for hashing
        hasher = hashlib.md5(str(point_id).encode("utf-8"))
        hashed_int = int(hasher.hexdigest(), 16)
        return self._db.router.route_key(hashed_int).id

    def upsert(self, collection_name: str, points: Any, **kwargs: Any) -> Any:
        """Upsert points, routing each point to its assigned shard."""
        # Handle Qdrant PointStruct or Batch
        # Differentiate between Batch objects and list of PointStructs
        grouped_points: Dict[str, List[Any]] = {}

        # Differentiate list/Sequence from Batch
        if isinstance(points, list) or isinstance(points, tuple):
            for point in points:
                # Retrieve payload from point
                payload = getattr(point, "payload", None)
                pid = getattr(point, "id", None)
                shard_id = self._resolve_shard(pid, payload)
                grouped_points.setdefault(shard_id, []).append(point)
        else:
            # If batch, extract ids and payloads
            ids = getattr(points, "ids", [])
            payloads = getattr(points, "payloads", None) or [None] * len(ids)
            vectors = getattr(points, "vectors", [])
            
            for idx, pid in enumerate(ids):
                payload = payloads[idx]
                shard_id = self._resolve_shard(pid, payload)
                # Reconstruct point representation or store batch mappings
                # For simplicity, convert batch to individual entries to upload
                try:
                    from qdrant_client.models import PointStruct
                    p = PointStruct(
                        id=pid,
                        vector=vectors[idx],
                        payload=payload,
                    )
                    grouped_points.setdefault(shard_id, []).append(p)
                except ImportError:
                    pass

        results = {}
        for shard_id, shard_points in grouped_points.items():
            client = self._clients[shard_id]
            results[shard_id] = client.upsert(
                collection_name=collection_name,
                points=shard_points,
                **kwargs,
            )
        return results

    def search(
        self,
        collection_name: str,
        query_vector: Sequence[float] | Any,
        query_filter: Any | None = None,
        limit: int = 10,
        **kwargs: Any,
    ) -> List[Any]:
        """
        Search for vectors, executing search on all shards and merging/sorting results by score.
        """
        # Check if the query_filter has field condition matching the shard key
        # In Qdrant, filter matches have a specific structure.
        # If we can find the shard key condition, we route directly to one shard!
        routed_shard_id = None
        if query_filter:
            # Simple metadata filter checking for shard key match conditions
            must = getattr(query_filter, "must", None)
            if must:
                for cond in must:
                    if getattr(cond, "key", None) == self._shard_key:
                        match = getattr(cond, "match", None)
                        val = getattr(match, "value", None)
                        if val is not None:
                            routed_shard_id = self._db.router.route_key(val).id
                            break

        if routed_shard_id:
            client = self._clients[routed_shard_id]
            return client.search(
                collection_name=collection_name,
                query_vector=query_vector,
                query_filter=query_filter,
                limit=limit,
                **kwargs,
            )

        # Scatter-gather across all shards
        all_results = []
        for client in self._clients.values():
            try:
                res = client.search(
                    collection_name=collection_name,
                    query_vector=query_vector,
                    query_filter=query_filter,
                    limit=limit,
                    **kwargs,
                )
                all_results.extend(res)
            except Exception:
                pass

        # Merge, sort by score descending, and limit
        # ScoredPoint objects have a `score` attribute
        all_results.sort(key=lambda p: getattr(p, "score", 0.0), reverse=True)
        return all_results[:limit]

    def recreate_collection(self, collection_name: str, **kwargs: Any) -> bool:
        """Create or recreate a collection on all shards (broadcast)."""
        ok = True
        for client in self._clients.values():
            try:
                res = client.recreate_collection(
                    collection_name=collection_name,
                    **kwargs,
                )
                if not res:
                    ok = False
            except Exception:
                ok = False
        return ok

    def delete_collection(self, collection_name: str) -> bool:
        """Delete collection on all shards (broadcast)."""
        ok = True
        for client in self._clients.values():
            try:
                res = client.delete_collection(collection_name)
                if not res:
                    ok = False
            except Exception:
                ok = False
        return ok

    def close(self) -> None:
        """Close MuxDB."""
        self._db.close()
