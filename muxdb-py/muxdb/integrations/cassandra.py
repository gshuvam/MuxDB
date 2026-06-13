"""
MuxDB Cassandra Integration.

Provides MuxCassandraSession wrapper to route queries across sharded
Cassandra sessions and keyspaces.
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence

from muxdb.client import MuxDB
from muxdb.config import MuxConfig


class MuxCassandraSession:
    """
    Sharded Cassandra Session.
    
    Routes CQL statements to specific Cassandra cluster sessions using MuxDB routing
    and merges result sets for multi-shard operations.
    """

    def __init__(self, db: MuxDB, sessions: Dict[str, Any]) -> None:
        self._db = db
        self._sessions = sessions  # shard_id -> cassandra.cluster.Session
        self._shard_key = db.config.cluster.shard_key

    @classmethod
    def from_config(cls, config: MuxConfig, **kwargs: Any) -> MuxCassandraSession:
        """Create sharded session from config."""
        try:
            from cassandra.cluster import Cluster
        except ImportError as exc:
            raise ImportError("cassandra-driver is not installed. Run: pip install cassandra-driver") from exc

        db = MuxDB(config)
        db.connect()

        sessions = {}
        for shard in db.shard_map.shards:
            cluster = Cluster([shard.host], port=shard.port, **kwargs)
            session = cluster.connect(shard.database)
            sessions[shard.id] = session

        return cls(db, sessions)

    def execute(self, query: str, parameters: Sequence[Any] | Dict[str, Any] | None = None, **kwargs: Any) -> Any:
        """
        Execute CQL statement on routed Cassandra session(s).
        """
        # Try to find the shard key in parameters
        routed_shard_id = None
        
        if parameters:
            if isinstance(parameters, dict):
                val = parameters.get(self._shard_key)
                if val is not None:
                    routed_shard_id = self._db.router.route_key(val).id
            elif isinstance(parameters, (list, tuple)):
                # If positional params, check if we can inspect query structure
                # For simplicity, if we pass shard_key parameter, use it
                pass

        # Check if query contains: WHERE user_id = ...
        # (Very basic regex/string checks in router query parsing)
        if not routed_shard_id:
            try:
                # Compile parameters to tuple
                params_list = list(parameters.values()) if isinstance(parameters, dict) else (parameters or [])
                decision = self._db.router.route(query, tuple(params_list))
                if len(decision.targets) == 1:
                    routed_shard_id = decision.targets[0].id
            except Exception:
                pass

        if routed_shard_id:
            session = self._sessions[routed_shard_id]
            return session.execute(query, parameters, **kwargs)

        # Scatter-gather across all shards
        all_rows = []
        for session in self._sessions.values():
            try:
                rs = session.execute(query, parameters, **kwargs)
                all_rows.extend(list(rs))
            except Exception:
                pass
        return all_rows

    def close(self) -> None:
        """Close sessions."""
        for session in self._sessions.values():
            try:
                session.shutdown()
                session.cluster.shutdown()
            except Exception:
                pass
        self._db.close()
