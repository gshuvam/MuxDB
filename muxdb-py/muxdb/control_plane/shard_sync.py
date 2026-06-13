from __future__ import annotations

from typing import Any, Dict

class ShardMapSynchronizer:
    """Handles propagation and convergence of shard maps across cluster nodes."""

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.current_version = 0
        self.topology: Dict[str, Any] = {}

    def broadcast_map_update(self, new_version: int, new_topology: Dict[str, Any]) -> int:
        """Initiate map update and bump state version if newer."""
        if new_version > self.current_version:
            self.current_version = new_version
            self.topology = new_topology
            return new_version
        return self.current_version

    def accept_map_update(self, sender_node_id: str, new_version: int, new_topology: Dict[str, Any]) -> bool:
        """Validate and apply a map update received from a peer node."""
        if new_version > self.current_version:
            self.current_version = new_version
            self.topology = new_topology
            return True
        return False
