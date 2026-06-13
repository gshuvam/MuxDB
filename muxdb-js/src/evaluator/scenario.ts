export interface ClusterChangeEvent {
  step: number;
  actionType: 'ADD_NODE' | 'REMOVE_NODE' | 'SLOW_NODE' | 'FAIL_NODE';
  targetShardId: string;
  parameter?: any;
}

export class WorkloadScenario {
  private numKeys: number;
  private defaultTenant: string;

  constructor(numKeys = 500, defaultTenant = 'tenant_0') {
    this.numKeys = numKeys;
    this.defaultTenant = defaultTenant;
  }

  public stableBaseline(steps: number, qps = 50): Record<string, any>[][] {
    const workload: Record<string, any>[][] = [];
    for (let step = 0; step < steps; step++) {
      const stepOps: Record<string, any>[] = [];
      for (let i = 0; i < qps; i++) {
        const key = `key_${Math.floor(Math.random() * this.numKeys)}`;
        const isWrite = Math.random() < 0.2;
        stepOps.push({
          op: isWrite ? 'write' : 'read',
          key,
          tenantId: this.defaultTenant,
        });
      }
      workload.push(stepOps);
    }
    return workload;
  }

  public pointHotspot(
    steps: number,
    qps = 50,
    hotspotKey = 'key_hot',
    hotspotStartStep = 10,
    hotspotRatio = 0.7,
  ): Record<string, any>[][] {
    const workload: Record<string, any>[][] = [];
    for (let step = 0; step < steps; step++) {
      const stepOps: Record<string, any>[] = [];
      for (let i = 0; i < qps; i++) {
        let key = '';
        if (step >= hotspotStartStep && Math.random() < hotspotRatio) {
          key = hotspotKey;
        } else {
          key = `key_${Math.floor(Math.random() * this.numKeys)}`;
        }
        const isWrite = Math.random() < 0.2;
        stepOps.push({
          op: isWrite ? 'write' : 'read',
          key,
          tenantId: this.defaultTenant,
        });
      }
      workload.push(stepOps);
    }
    return workload;
  }

  public diurnalSpikes(
    steps: number,
    baseQps = 30,
    amplitude = 25,
    periodSteps = 24,
  ): Record<string, any>[][] {
    const workload: Record<string, any>[][] = [];
    for (let step = 0; step < steps; step++) {
      const qps = Math.max(
        5,
        Math.floor(baseQps + amplitude * Math.sin((2 * Math.PI * step) / periodSteps)),
      );

      const stepOps: Record<string, any>[] = [];
      for (let i = 0; i < qps; i++) {
        // Zipfian approximation using pareto random draw
        const u = Math.random() || 1e-9;
        const keyIdx = Math.floor((1.0 / Math.pow(u, 1.0 / 1.5)) * 10) % this.numKeys;
        const key = `key_${keyIdx}`;
        const isWrite = Math.random() < 0.2;
        stepOps.push({
          op: isWrite ? 'write' : 'read',
          key,
          tenantId: this.defaultTenant,
        });
      }
      workload.push(stepOps);
    }
    return workload;
  }

  public mixedOltpOlap(
    steps: number,
    qps = 40,
    olapStartStep = 15,
    scanSize = 20,
  ): Record<string, any>[][] {
    const workload: Record<string, any>[][] = [];
    for (let step = 0; step < steps; step++) {
      const stepOps: Record<string, any>[] = [];
      for (let i = 0; i < qps; i++) {
        const key = `key_${Math.floor(Math.random() * this.numKeys)}`;
        const isWrite = Math.random() < 0.15;
        stepOps.push({
          op: isWrite ? 'write' : 'read',
          key,
          tenantId: this.defaultTenant,
        });
      }

      if (step >= olapStartStep && step % 10 === 0) {
        const startIdx = Math.floor(Math.random() * (this.numKeys - scanSize - 1));
        for (let i = 0; i < scanSize; i++) {
          stepOps.push({
            op: 'read',
            key: `key_${startIdx + i}`,
            tenantId: this.defaultTenant,
            isScan: true,
          });
        }
      }
      workload.push(stepOps);
    }
    return workload;
  }
}
