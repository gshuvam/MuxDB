import { Router } from '../router';
import { TelemetryCollector } from '../telemetry';
import { Balancer } from '../balancer';
import { BanditSolver } from '../ml/bandit';
import { ClusterChangeEvent } from './scenario';

export interface SimulationResult {
  policy: string;
  totalQueries: number;
  totalErrors: number;
  avgLatency: number;
  p99Latency: number;
  totalKeysMigrated: number;
  averageImbalanceRatio: number;
  stepMetrics: {
    step: number;
    throughput: number;
    errors: number;
    avgLatency: number;
    imbalanceRatio: number;
  }[];
}

export class SimulationRunner {
  private router: Router;
  private telemetry: TelemetryCollector;
  private balancer: Balancer;
  private banditSolver?: BanditSolver;

  private shardLatencies: Record<string, number> = {};
  private shardCapacities: Record<string, number> = {};
  private shardSlowdowns: Record<string, number> = {};
  private failedShards = new Set<string>();
  private totalKeysMigrated = 0;

  constructor(
    router: Router,
    telemetry: TelemetryCollector,
    balancer: Balancer,
    banditSolver?: BanditSolver,
  ) {
    this.router = router;
    this.telemetry = telemetry;
    this.balancer = balancer;
    this.banditSolver = banditSolver;

    const shards = this.router.shardMap.shards;
    for (const s of shards) {
      this.shardLatencies[s.id] = 5.0;
      this.shardCapacities[s.id] = 100.0;
      this.shardSlowdowns[s.id] = 1.0;
    }
  }

  public applyEvent(event: ClusterChangeEvent): void {
    const shardId = event.targetShardId;
    if (event.actionType === 'ADD_NODE') {
      const currentShards = [...this.router.shardMap.shards];
      if (!currentShards.some((s) => s.id === shardId)) {
        currentShards.push({
          id: shardId,
          backend: 'postgresql',
          host: 'localhost',
          port: 5432,
          database: `db_${shardId}`,
          weight: 1,
          dsn: `postgresql://localhost:5432/db_${shardId}`,
        });
        this.router.shardMap.swap(currentShards);
      }
      this.shardLatencies[shardId] = 5.0;
      this.shardCapacities[shardId] = 100.0;
      this.shardSlowdowns[shardId] = 1.0;
    } else if (event.actionType === 'REMOVE_NODE') {
      const currentShards = this.router.shardMap.shards.filter((s) => s.id !== shardId);
      this.router.shardMap.swap(currentShards);
      this.failedShards.delete(shardId);
    } else if (event.actionType === 'SLOW_NODE') {
      const multiplier = event.parameter !== undefined ? event.parameter : 5.0;
      this.shardSlowdowns[shardId] = multiplier;
    } else if (event.actionType === 'FAIL_NODE') {
      this.failedShards.add(shardId);
    }
  }

  public async run(
    workload: Record<string, any>[][],
    events?: ClusterChangeEvent[],
    policy: 'static' | 'reactive' | 'adaptive' = 'adaptive',
  ): Promise<SimulationResult> {
    const eventsMap: Record<number, ClusterChangeEvent[]> = {};
    if (events) {
      for (const ev of events) {
        if (!eventsMap[ev.step]) {
          eventsMap[ev.step] = [];
        }
        eventsMap[ev.step]!.push(ev);
      }
    }

    const stepMetrics: SimulationResult['stepMetrics'] = [];
    let overallQueries = 0;
    let overallErrors = 0;
    const overallLatencies: number[] = [];

    for (let step = 0; step < workload.length; step++) {
      const stepOps = workload[step] as Record<string, any>[];

      // 1. Apply events
      if (eventsMap[step]) {
        for (const ev of eventsMap[step]!) {
          this.applyEvent(ev);
        }
      }

      const stepLatencies: number[] = [];
      let stepErrors = 0;

      const shardQueriesInStep: Record<string, number> = {};
      for (const sid of Object.keys(this.shardLatencies)) {
        shardQueriesInStep[sid] = 0;
      }

      // 2. Process query ops
      for (const op of stepOps) {
        const key = op.key as string;
        const isWrite = op.op === 'write';

        let targetShardId = '';
        if (policy === 'adaptive' && this.banditSolver) {
          targetShardId = this.banditSolver.selectArm({ type: op.op });
        } else {
          try {
            const shard = this.router.routeKey(key);
            targetShardId = shard.id;
          } catch {
            targetShardId = Object.keys(this.shardLatencies)[0] as string;
          }
        }

        shardQueriesInStep[targetShardId] = (shardQueriesInStep[targetShardId] ?? 0) + 1;

        if (this.failedShards.has(targetShardId)) {
          stepErrors++;
          overallErrors++;
          this.telemetry.recordQuery(targetShardId, key, 1000.0, isWrite);
          continue;
        }

        const base = this.shardLatencies[targetShardId] ?? 5.0;
        const slowdown = this.shardSlowdowns[targetShardId] ?? 1.0;
        const capacity = this.shardCapacities[targetShardId] ?? 100.0;

        const currentQps = shardQueriesInStep[targetShardId] as number;
        let queueingDelay = 0.0;
        if (currentQps > 0) {
          const loadRatio = currentQps / capacity;
          queueingDelay = 10.0 * Math.pow(loadRatio, 2);
        }

        const latency = Math.max(1.0, base * slowdown + queueingDelay);
        this.telemetry.recordQuery(targetShardId, key, latency, isWrite);

        stepLatencies.push(latency);
        overallLatencies.push(latency);
        overallQueries++;

        if (policy === 'adaptive' && this.banditSolver) {
          const reward = 1.0 / (1.0 + latency / 100.0);
          this.banditSolver.updateReward(targetShardId, reward);
        }
      }

      // 3. Dynamic Balancing Decisions
      if (policy !== 'static' && step % 2 === 0) {
        const metrics: Record<string, { read_qps: number; write_qps: number }> = {};
        for (const sid of Object.keys(this.shardLatencies)) {
          metrics[sid] = {
            read_qps: this.telemetry.getQps(sid, false),
            write_qps: this.telemetry.getQps(sid, true),
          };
        }

        const decisions = this.balancer.evaluate(metrics);
        for (const action of decisions) {
          if (action.actionType === 'L2_SPLIT' || action.actionType === 'L3_REPLICA') {
            this.totalKeysMigrated += 50;
            if (action.actionType === 'L2_SPLIT' && Object.keys(this.shardLatencies).length < 8) {
              const newName = `shard_split_${Object.keys(this.shardLatencies).length}`;
              this.applyEvent({ step, actionType: 'ADD_NODE', targetShardId: newName });
            }
          }
        }
      }

      // Compile step stats
      const avgStepLat = stepLatencies.length > 0
        ? stepLatencies.reduce((a, b) => a + b, 0) / stepLatencies.length
        : 0.0;

      const loads = Object.keys(this.shardLatencies)
        .filter((s) => !this.failedShards.has(s))
        .map((s) => shardQueriesInStep[s] as number);
      const avgLoad = loads.length > 0 ? loads.reduce((a, b) => a + b, 0) / loads.length : 1.0;
      const maxLoad = loads.length > 0 ? Math.max(...loads) : 0.0;
      const imbalance = avgLoad > 0 ? maxLoad / avgLoad : 1.0;

      stepMetrics.push({
        step,
        throughput: stepOps.length - stepErrors,
        errors: stepErrors,
        avgLatency: avgStepLat,
        imbalanceRatio: imbalance,
      });
    }

    const avgLat = overallLatencies.length > 0
      ? overallLatencies.reduce((a, b) => a + b, 0) / overallLatencies.length
      : 0.0;
    const sorted = [...overallLatencies].sort((a, b) => a - b);
    const p99Lat = sorted.length > 0 ? (sorted[Math.floor(sorted.length * 0.99)] as number) : 0.0;

    return {
      policy,
      totalQueries: overallQueries,
      totalErrors: overallErrors,
      avgLatency: avgLat,
      p99Latency: p99Lat,
      totalKeysMigrated: this.totalKeysMigrated,
      averageImbalanceRatio: stepMetrics.length > 0
        ? stepMetrics.reduce((sum, m) => sum + m.imbalanceRatio, 0) / stepMetrics.length
        : 1.0,
      stepMetrics,
    };
  }
}
