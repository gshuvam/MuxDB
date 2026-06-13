"""
MuxDB Elasticsearch Integration.

Provides MuxElasticsearch to route indexing and search operations across
sharded Elasticsearch instances, including bulk operation splitting.
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Sequence


class MuxElasticsearch:
    """
    Sharded Elasticsearch Client.
    
    Routes documents by shard key or ID, groups bulk operations,
    and aggregates score-sorted hits from multiple Elasticsearch shards.
    """

    def __init__(self, db: Any, clients: Dict[str, Any]) -> None:
        self._db = db
        self._clients = clients  # shard_id -> elasticsearch.Elasticsearch
        self._shard_key = db.config.cluster.shard_key

    @classmethod
    def from_config(cls, config: Any, **kwargs: Any) -> MuxElasticsearch:
        """Create client from configuration."""
        try:
            import elasticsearch
        except ImportError as exc:
            raise ImportError("elasticsearch is not installed. Run: pip install elasticsearch") from exc

        db = Any
        # Fallback to local import to avoid circular dependency
        from muxdb.client import MuxDB
        db = MuxDB(config)
        db.connect()

        clients = {}
        for shard in db.shard_map.shards:
            hosts = [f"http://{shard.host}:{shard.port}"]
            clients[shard.id] = elasticsearch.Elasticsearch(hosts, **kwargs)

        return cls(db, clients)

    def _resolve_shard(self, doc_id: Any, doc_body: Dict[str, Any] | None = None) -> str:
        """Determine which shard a document belongs to."""
        if doc_body and self._shard_key in doc_body:
            return self._db.router.route_key(doc_body[self._shard_key]).id

        if doc_id is not None:
            hasher = hashlib.md5(str(doc_id).encode("utf-8"))
            hashed_int = int(hasher.hexdigest(), 16)
            return self._db.router.route_key(hashed_int).id

        # Return default first shard if no key/id is present
        return self._db.shard_map.shard_ids[0]

    def index(self, index: str, document: Dict[str, Any], id: Any = None, **kwargs: Any) -> Any:
        """Index a document, routed by shard key or ID."""
        shard_id = self._resolve_shard(id, document)
        client = self._clients[shard_id]
        return client.index(index=index, document=document, id=id, **kwargs)

    def get(self, index: str, id: Any, **kwargs: Any) -> Any:
        """Get a document by ID."""
        shard_id = self._resolve_shard(id)
        client = self._clients[shard_id]
        return client.get(index=index, id=id, **kwargs)

    def delete(self, index: str, id: Any, **kwargs: Any) -> Any:
        """Delete a document by ID."""
        shard_id = self._resolve_shard(id)
        client = self._clients[shard_id]
        return client.delete(index=index, id=id, **kwargs)

    def search(
        self,
        index: str,
        query: Dict[str, Any] | None = None,
        body: Dict[str, Any] | None = None,
        size: int = 10,
        **kwargs: Any,
    ) -> Any:
        """
        Search for documents, routing to a single shard if a shard key filter is matched,
        otherwise scatter-gathering hits across all shards and sorting by score.
        """
        routed_shard_id = None
        search_query = query or body or {}

        # Look for term filter on shard key: e.g. {"term": {"userId": 42}} or similar
        try:
            # Simple recursive search for term/match filters matching the shard key
            val = _find_shard_key_in_query(search_query, self._shard_key)
            if val is not None:
                routed_shard_id = self._db.router.route_key(val).id
        except Exception:
            pass

        if routed_shard_id:
            client = self._clients[routed_shard_id]
            return client.search(index=index, query=query, body=body, size=size, **kwargs)

        # Scatter-gather
        all_hits = []
        max_score = 0.0
        total_value = 0

        for client in self._clients.values():
            try:
                res = client.search(index=index, query=query, body=body, size=size, **kwargs)
                hits_wrapper = res.get("hits", {})
                hits = hits_wrapper.get("hits", [])
                all_hits.extend(hits)
                
                # Aggregate totals
                total_wrapper = hits_wrapper.get("total", {})
                if isinstance(total_wrapper, dict):
                    total_value += total_wrapper.get("value", 0)
                else:
                    total_value += total_wrapper

                ms = hits_wrapper.get("max_score")
                if ms is not None:
                    max_score = max(max_score, float(ms))
            except Exception:
                pass

        # Sort combined hits by score descending
        all_hits.sort(key=lambda h: h.get("_score") or 0.0, reverse=True)
        sliced_hits = all_hits[:size]

        return {
            "took": 0,
            "timed_out": False,
            "hits": {
                "total": {"value": total_value, "relation": "eq"},
                "max_score": max_score,
                "hits": sliced_hits,
            },
        }

    def bulk(self, operations: Sequence[Dict[str, Any]], **kwargs: Any) -> Any:
        """
        Execute bulk actions, grouping operations by target shard connection to minimize network hops.
        """
        grouped_ops: Dict[str, List[Any]] = {}

        # Parse bulk format (list of actions/documents)
        # Standard bulk expects: [{"index": {"_index": "test", "_id": "1"}}, {"field": "value"}]
        # We group pairs of actions by shard.
        idx = 0
        while idx < len(operations):
            op = operations[idx]
            # Bulk action keys: index, create, update, delete
            action_type = list(op.keys())[0]
            meta = op[action_type]
            doc_id = meta.get("_id")

            # Look ahead for document body if not a delete action
            doc_body = None
            if action_type != "delete" and idx + 1 < len(operations):
                doc_body = operations[idx + 1]

            shard_id = self._resolve_shard(doc_id, doc_body)
            shard_list = grouped_ops.setdefault(shard_id, [])
            shard_list.append(op)
            if doc_body is not None:
                shard_list.append(doc_body)
                idx += 2
            else:
                idx += 1

        results = {}
        for shard_id, ops in grouped_ops.items():
            client = self._clients[shard_id]
            results[shard_id] = client.bulk(operations=ops, **kwargs)

        return results

    def close(self) -> None:
        """Close client pools."""
        for client in self._clients.values():
            try:
                client.close()
            except Exception:
                pass
        self._db.close()


def _find_shard_key_in_query(query: Any, key_name: str) -> Any | None:
    """Helper to search Elasticsearch query DSL for term/match filters on the shard key."""
    if not isinstance(query, dict):
        return None

    for k, v in query.items():
        if k in ("term", "match", "terms"):
            if isinstance(v, dict) and key_name in v:
                val = v[key_name]
                # terms filter can take a list
                return val[0] if isinstance(val, list) else val
        
        # Recurse into sub-objects
        if isinstance(v, dict):
            res = _find_shard_key_in_query(v, key_name)
            if res is not None:
                return res
        elif isinstance(v, list):
            for item in v:
                res = _find_shard_key_in_query(item, key_name)
                if res is not None:
                    return res

    return None
