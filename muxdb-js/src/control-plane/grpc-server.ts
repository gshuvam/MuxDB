import { BridgeLeaseManager } from './lease-manager';
import { ShardMapSynchronizer } from './shard-sync';

export class ControlPlaneGRPCServer {
  public port: number | null = null;
  public isRunning: boolean = false;
  public leaseManager = new BridgeLeaseManager();
  public synchronizer: ShardMapSynchronizer;

  constructor(public readonly nodeId: string) {
    this.synchronizer = new ShardMapSynchronizer(nodeId);
  }

  public start(port: number): void {
    /** Start listening on the given port. */
    this.port = port;
    this.isRunning = true;
  }

  public stop(): void {
    /** Stop the server. */
    this.isRunning = false;
    this.port = null;
  }

  // --- Mock RPC Service Handlers ---

  public syncShardMap(senderId: string, version: number, topology: Record<string, any>): boolean {
    if (!this.isRunning) return false;
    return this.synchronizer.acceptMapUpdate(senderId, version, topology);
  }

  public acquireLease(leaseId: string, clientId: string, durationS: number = 5): boolean {
    if (!this.isRunning) return false;
    return this.leaseManager.acquireLease(leaseId, clientId, durationS);
  }

  public releaseLease(leaseId: string, clientId: string): boolean {
    if (!this.isRunning) return false;
    return this.leaseManager.releaseLease(leaseId, clientId);
  }
}
