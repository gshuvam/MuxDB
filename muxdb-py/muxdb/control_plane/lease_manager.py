from __future__ import annotations

import time
from typing import Dict, Optional

class BridgeLeaseManager:
    """Coordinates ranges and shard lease states to prevent duplicate operations."""

    def __init__(self) -> None:
        # Structure: lease_id -> {"client_id": str, "expires_at": float}
        self._leases: Dict[str, dict] = {}

    def acquire_lease(self, lease_id: str, client_id: str, duration_s: float = 5.0) -> bool:
        """Acquire a lease lock for a resource."""
        now = time.time()
        lease = self._leases.get(lease_id)
        
        if lease is None or lease["expires_at"] < now or lease["client_id"] == client_id:
            self._leases[lease_id] = {
                "client_id": client_id,
                "expires_at": now + duration_s,
            }
            return True
        return False

    def renew_lease(self, lease_id: str, client_id: str, duration_s: float = 5.0) -> bool:
        """Renew an active lease."""
        return self.acquire_lease(lease_id, client_id, duration_s)

    def release_lease(self, lease_id: str, client_id: str) -> bool:
        """Release a lease lock."""
        lease = self._leases.get(lease_id)
        if lease and lease["client_id"] == client_id:
            del self._leases[lease_id]
            return True
        return False

    def is_lease_active(self, lease_id: str) -> bool:
        """Check if lease is currently valid and unexpired."""
        now = time.time()
        lease = self._leases.get(lease_id)
        return lease is not None and lease["expires_at"] >= now

    def get_holder(self, lease_id: str) -> Optional[str]:
        """Get the identifier of the lease holder if it is active."""
        if self.is_lease_active(lease_id):
            return self._leases[lease_id]["client_id"]
        return None
