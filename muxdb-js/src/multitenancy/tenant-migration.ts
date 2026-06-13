import { MigrationError } from '../errors';
import { TenantRouter } from './tenant-router';
import { logAuditEvent } from '../security/audit';

export class TenantMigrator {
  constructor(private readonly router: TenantRouter) {}

  public async migrateTenant(
    tenantId: string,
    destShardIds: string[],
    actor: string = 'orchestrator'
  ): Promise<void> {
    if (!destShardIds || destShardIds.length === 0) {
      throw new MigrationError('Migration destination shards list cannot be empty.');
    }

    logAuditEvent(actor, 'tenant_migration_start', tenantId, 'success', {
      destination_shards: destShardIds,
    });

    try {
      // Simulate data copy phase delay
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Update tenant-to-shard mapping
      this.router.registerTenant(tenantId, destShardIds);

      logAuditEvent(actor, 'tenant_migration_complete', tenantId, 'success', {
        destination_shards: destShardIds,
      });
    } catch (error: any) {
      logAuditEvent(actor, 'tenant_migration_failed', tenantId, 'error', {
        error: error.message,
      });
      throw new MigrationError(`Migration failed for tenant '${tenantId}': ${error.message}`);
    }
  }
}
