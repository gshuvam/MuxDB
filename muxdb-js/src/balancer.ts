export interface BalancingAction {
  actionType: 'L1_LEASE' | 'L2_SPLIT' | 'L3_REPLICA';
  shardId: string;
  details: Record<string, any>;
}

export class Balancer {
  private lastBalanceTime: number = 0;

  constructor(
    public readonly cooldownSeconds: number = 10,
    public readonly qpsSplitThreshold: number = 100,
    public readonly qpsReplicaThreshold: number = 50,
    public readonly writeSuppressionThreshold: number = 80
  ) {}

  public evaluate(metrics: Record<string, { read_qps: number; write_qps: number }>): BalancingAction[] {
    /** Evaluate metrics and trigger range splits, replica scales, or lease transfers. */
    const now = Date.now() / 1000;
    if (now - this.lastBalanceTime < this.cooldownSeconds) {
      return [];
    }

    // Write load suppression check
    const totalWriteQps = Object.values(metrics).reduce((sum, m) => sum + (m.write_qps || 0), 0);
    if (totalWriteQps >= this.writeSuppressionThreshold) {
      return [];
    }

    const actions: BalancingAction[] = [];

    for (const [shardId, data] of Object.entries(metrics)) {
      const readQps = data.read_qps || 0;
      const writeQps = data.write_qps || 0;
      const totalQps = readQps + writeQps;

      // L2 Split check
      if (totalQps >= this.qpsSplitThreshold) {
        actions.push({
          actionType: 'L2_SPLIT',
          shardId,
          details: { qps: totalQps, splitThreshold: this.qpsSplitThreshold }
        });
        continue;
      }

      // L3 Replica scaling check
      if (readQps >= this.qpsReplicaThreshold) {
        actions.push({
          actionType: 'L3_REPLICA',
          shardId,
          details: { readQps, replicaThreshold: this.qpsReplicaThreshold }
        });
        continue;
      }

      // L1 Lease Transfer check
      if (writeQps > 10 && readQps > 20) {
        actions.push({
          actionType: 'L1_LEASE',
          shardId,
          details: { readQps, writeQps }
        });
      }
    }

    if (actions.length > 0) {
      this.lastBalanceTime = now;
    }

    return actions;
  }
}
