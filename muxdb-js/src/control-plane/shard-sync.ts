export class ShardMapSynchronizer {
  public currentVersion: number = 0;
  public topology: Record<string, any> = {};

  constructor(public readonly nodeId: string) {}

  public broadcastMapUpdate(newVersion: number, newTopology: Record<string, any>): number {
    /** Propagate map version updates to active peers. */
    if (newVersion > this.currentVersion) {
      this.currentVersion = newVersion;
      this.topology = newTopology;
      return newVersion;
    }
    return this.currentVersion;
  }

  public acceptMapUpdate(senderNodeId: string, newVersion: number, newTopology: Record<string, any>): boolean {
    /** Receive and synchronize maps from peers. */
    if (newVersion > this.currentVersion) {
      this.currentVersion = newVersion;
      this.topology = newTopology;
      return true;
    }
    return false;
  }
}
