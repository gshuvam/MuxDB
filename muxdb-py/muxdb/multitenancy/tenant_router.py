from __future__ import annotations

from typing import Dict, List
from muxdb.errors import RoutingError

class TenantRouter:
    """Routes client connections and requests to dedicated shard groups based on Tenant ID."""

    def __init__(self, mapping: Dict[str, List[str]] | None = None) -> None:
        # Maps tenant_id -> list of shard_ids
        self._mapping = mapping or {}

    def register_tenant(self, tenant_id: str, shard_ids: List[str]) -> None:
        """Register or update a tenant's target shard mapping."""
        self._mapping[tenant_id] = list(shard_ids)

    def resolve_tenant_shards(self, tenant_id: str) -> List[str]:
        """Get the shard IDs assigned to the given tenant ID."""
        if tenant_id not in self._mapping:
            raise RoutingError(f"No shard group mapping found for tenant: '{tenant_id}'")
        return self._mapping[tenant_id]
