export class BridgeLeaseManager {
  private leases: Map<string, { clientId: string; expiresAt: number }> = new Map();

  public acquireLease(leaseId: string, clientId: string, durationS: number = 5): boolean {
    /** Attempt to acquire lease lock for a resource. */
    const now = Date.now() / 1000;
    const lease = this.leases.get(leaseId);

    if (!lease || lease.expiresAt < now || lease.clientId === clientId) {
      this.leases.set(leaseId, {
        clientId,
        expiresAt: now + durationS,
      });
      return true;
    }
    return false;
  }

  public renewLease(leaseId: string, clientId: string, durationS: number = 5): boolean {
    /** Renew lease lock duration. */
    return this.acquireLease(leaseId, clientId, durationS);
  }

  public releaseLease(leaseId: string, clientId: string): boolean {
    /** Release lease lock. */
    const lease = this.leases.get(leaseId);
    if (lease && lease.clientId === clientId) {
      this.leases.delete(leaseId);
      return true;
    }
    return false;
  }

  public isLeaseActive(leaseId: string): boolean {
    /** Verify lease exists and remains unexpired. */
    const now = Date.now() / 1000;
    const lease = this.leases.get(leaseId);
    return !!lease && lease.expiresAt >= now;
  }

  public getHolder(leaseId: string): string | null {
    /** Get the owner identifier of the lease. */
    const now = Date.now() / 1000;
    const lease = this.leases.get(leaseId);
    if (lease && lease.expiresAt >= now) {
      return lease.clientId;
    }
    return null;
  }
}
