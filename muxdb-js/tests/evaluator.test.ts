import { describe, it, expect } from 'vitest';
import {
  WorkloadScenario,
  comparePolicies,
} from '../src/index';

describe('Node.js Evaluation Harness Layer Tests', () => {

  const scenario = new WorkloadScenario(100);

  it('should generate stable baseline workload batches', () => {
    const workload = scenario.stableBaseline(5, 10);
    expect(workload.length).toBe(5);
    for (const stepOps of workload) {
      expect(stepOps.length).toBe(10);
      for (const op of stepOps) {
        expect(['read', 'write']).toContain(op.op);
        expect(op.key).toContain('key_');
      }
    }
  });

  it('should generate point hotspot workload batches with viral key scaling', () => {
    const hotspotKey = 'key_viral';
    const workload = scenario.pointHotspot(10, 20, hotspotKey, 4, 0.8);
    expect(workload.length).toBe(10);

    // Before step 4: no viral key should be present
    for (let step = 0; step < 4; step++) {
      for (const op of workload[step]!) {
        expect(op.key).not.toBe(hotspotKey);
      }
    }

    // After/at step 4: viral key should be active
    let hotspotCount = 0;
    let totalCount = 0;
    for (let step = 4; step < 10; step++) {
      for (const op of workload[step]!) {
        totalCount++;
        if (op.key === hotspotKey) {
          hotspotCount++;
        }
      }
    }
    expect(hotspotCount).toBeGreaterThan(0);
    expect(hotspotCount / totalCount).toBeGreaterThan(0.5);
  });

  it('should generate diurnal spikes with fluctuating sizes', () => {
    const workload = scenario.diurnalSpikes(20, 30, 15, 10);
    expect(workload.length).toBe(20);

    const sizes = workload.map((w) => w.length);
    expect(Math.max(...sizes)).toBeGreaterThan(Math.min(...sizes));
  });

  it('should generate mixed OLTP/OLAP workload with scan segments', () => {
    const workload = scenario.mixedOltpOlap(12, 10, 2, 5);
    expect(workload.length).toBe(12);

    const step10Scans = workload[10]!.filter((op) => op.isScan);
    expect(step10Scans.length).toBe(5);
  });

  it('should run multi-policy comparisons with scheduled cluster events', async () => {
    const shardIds = ['s0', 's1', 's2'];
    const workload = scenario.stableBaseline(5, 10);

    const events = [
      { step: 2, actionType: 'SLOW_NODE' as const, targetShardId: 's1', parameter: 10.0 },
      { step: 3, actionType: 'FAIL_NODE' as const, targetShardId: 's2' },
    ];

    const results = await comparePolicies(shardIds, workload, events);

    expect(results.static).toBeDefined();
    expect(results.reactive).toBeDefined();
    expect(results.adaptive).toBeDefined();

    for (const policy of ['static', 'reactive', 'adaptive'] as const) {
      const res = results[policy];
      expect(res.policy).toBe(policy);
      expect(res.totalQueries).toBeGreaterThan(0);
      expect(res.avgLatency).toBeGreaterThan(0.0);
      expect(res.p99Latency).toBeGreaterThan(0.0);
      expect(res.stepMetrics.length).toBe(5);

      for (const m of res.stepMetrics) {
        expect(m.step).toBeDefined();
        expect(m.throughput).toBeDefined();
        expect(m.avgLatency).toBeDefined();
        expect(m.imbalanceRatio).toBeDefined();
      }
    }

    expect(results.reactive.totalKeysMigrated).toBeGreaterThanOrEqual(0);
    expect(results.adaptive.totalKeysMigrated).toBeGreaterThanOrEqual(0);
  });

});
