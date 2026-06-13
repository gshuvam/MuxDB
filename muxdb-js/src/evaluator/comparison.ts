import { Router } from '../router';
import { MuxConfig } from '../config';
import { TelemetryCollector } from '../telemetry';
import { Balancer } from '../balancer';
import { BanditSolver } from '../ml/bandit';
import { SimulationRunner, SimulationResult } from './runner';
import { ClusterChangeEvent } from './scenario';

export async function comparePolicies(
  shardIds: string[],
  workload: Record<string, any>[][],
  events?: ClusterChangeEvent[],
): Promise<Record<string, SimulationResult>> {
  const results: Record<string, SimulationResult> = {};

  // 1. Static Policy
  results.static = await runPolicySimulation(shardIds, workload, events, 'static');

  // 2. Reactive Policy
  results.reactive = await runPolicySimulation(shardIds, workload, events, 'reactive');

  // 3. Adaptive Policy
  results.adaptive = await runPolicySimulation(shardIds, workload, events, 'adaptive');

  return results;
}

async function runPolicySimulation(
  shardIds: string[],
  workload: Record<string, any>[][],
  events: ClusterChangeEvent[] | undefined,
  policy: 'static' | 'reactive' | 'adaptive',
): Promise<SimulationResult> {
  const shardsConfig = shardIds.map((sid, idx) => ({
    id: sid,
    backend: 'postgresql',
    host: 'localhost',
    port: 5432 + idx,
    database: `db_${sid}`,
    weight: 1,
    tags: {},
  }));

  const config = new MuxConfig({
    cluster: {
      name: 'sim_cluster',
      strategy: 'consistent_hash',
      shardKey: 'key',
      virtualNodes: 256,
    },
    shards: shardsConfig,
  });

  const router = new Router(config);
  const telemetry = new TelemetryCollector(10);
  const balancer = new Balancer(0);

  let banditSolver: BanditSolver | undefined;
  if (policy === 'adaptive') {
    banditSolver = new BanditSolver(shardIds, 1.0, 2.0, 'soft');
  }

  const runner = new SimulationRunner(router, telemetry, balancer, banditSolver);

  // Deep copy workload & events
  const workloadCopy = JSON.parse(JSON.stringify(workload));
  const eventsCopy = events ? JSON.parse(JSON.stringify(events)) : undefined;

  return runner.run(workloadCopy, eventsCopy, policy);
}
