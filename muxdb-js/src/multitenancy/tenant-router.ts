import { RoutingError } from '../errors';

export class TenantRouter {
  private mapping: Map<string, string[]> = new Map();

  constructor(initialMapping?: Record<string, string[]>) {
    if (initialMapping) {
      for (const [tenantId, shardIds] of Object.entries(initialMapping)) {
        this.mapping.set(tenantId, shardIds);
      }
    }
  }

  public registerTenant(tenantId: string, shardIds: string[]): void {
    this.mapping.set(tenantId, [...shardIds]);
  }

  public resolveTenantShards(tenantId: string): string[] {
    const shardIds = this.mapping.get(tenantId);
    if (!shardIds) {
      throw new RoutingError(`No shard group mapping found for tenant: '${tenantId}'`);
    }
    return shardIds;
  }
}
