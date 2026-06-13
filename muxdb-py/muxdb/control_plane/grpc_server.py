from __future__ import annotations

from typing import Any, Dict
from muxdb.control_plane.lease_manager import BridgeLeaseManager
from muxdb.control_plane.shard_sync import ShardMapSynchronizer

class ControlPlaneGRPCServer:
    """Mock-backed gRPC server handler coordinating cluster control plane RPC endpoints."""

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.port: int | None = None
        self.is_running = False
        self.lease_manager = BridgeLeaseManager()
        self.synchronizer = ShardMapSynchronizer(node_id)

    def start(self, port: int) -> None:
        """Start the RPC service listening on the given port."""
        self.port = port
        self.is_running = True

    def stop(self) -> None:
        """Shut down the RPC service."""
        self.is_running = False
        self.port = None

    # --- RPC Service Methods ---

    def SyncShardMap(self, sender_id: str, version: int, topology: Dict[str, Any]) -> bool:
        """RPC: Broadcast or replicate a shard map version to this node."""
        if not self.is_running:
            return False
        return self.synchronizer.accept_map_update(sender_id, version, topology)

    def AcquireLease(self, lease_id: str, client_id: str, duration_s: float = 5.0) -> bool:
        """RPC: Request a bridge lease/lock on a migrating range."""
        if not self.is_running:
            return False
        return self.lease_manager.acquire_lease(lease_id, client_id, duration_s)

    def ReleaseLease(self, lease_id: str, client_id: str) -> bool:
        """RPC: Relinquish an active range lease."""
        if not self.is_running:
            return False
        return self.lease_manager.release_lease(lease_id, client_id)
