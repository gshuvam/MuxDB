from __future__ import annotations

import time
from typing import List

from muxdb.errors import MigrationError
from muxdb.multitenancy.tenant_router import TenantRouter
from muxdb.security.audit import log_audit_event

class TenantMigrator:
    """Orchestrates tenant data migration processes and atomic routing switches."""

    def __init__(self, router: TenantRouter) -> None:
        self.router = router

    def migrate_tenant(
        self,
        tenant_id: str,
        dest_shard_ids: List[str],
        actor: str = "orchestrator",
    ) -> None:
        """Simulate and orchestrate moving a tenant to a new set of destination shards."""
        if not dest_shard_ids:
            raise MigrationError("Migration destination shards list cannot be empty.")

        log_audit_event(
            actor=actor,
            action="tenant_migration_start",
            target=tenant_id,
            details={"destination_shards": dest_shard_ids},
        )
        
        try:
            # Simulate replication / copy delay
            time.sleep(0.01)
            
            # Atomic swap of tenant routing
            self.router.register_tenant(tenant_id, dest_shard_ids)
            
            log_audit_event(
                actor=actor,
                action="tenant_migration_complete",
                target=tenant_id,
                status="success",
                details={"destination_shards": dest_shard_ids},
            )
        except Exception as e:
            log_audit_event(
                actor=actor,
                action="tenant_migration_failed",
                target=tenant_id,
                status="error",
                details={"error": str(e)},
            )
            raise MigrationError(f"Migration failed for tenant '{tenant_id}': {e}")

