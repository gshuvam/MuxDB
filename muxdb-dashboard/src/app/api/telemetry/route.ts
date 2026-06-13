import { NextResponse } from 'next/server';

// Singleton cluster simulation state
interface ShardState {
  id: string;
  backend: string;
  host: string;
  port: number;
  database: string;
  weight: number;
  health: 'healthy' | 'slow' | 'failed';
  qps: number;
  latency: number;
  hotKeys: string[];
}

interface MigrationState {
  id: string;
  source: string;
  dest: string;
  totalKeys: number;
  migratedKeys: number;
  status: 'running' | 'completed';
}

interface ClusterState {
  shards: ShardState[];
  migrations: MigrationState[];
  tenants: Record<string, string>;
  p50Latency: number;
  p99Latency: number;
  totalQps: number;
  imbalanceRatio: number;
}

let state: ClusterState = {
  shards: [
    { id: 'shard-0', backend: 'postgresql', host: 'localhost', port: 5432, database: 'db_shard_0', weight: 1, health: 'healthy', qps: 42, latency: 4.8, hotKeys: ['user_42', 'user_102'] },
    { id: 'shard-1', backend: 'postgresql', host: 'localhost', port: 5433, database: 'db_shard_1', weight: 1, health: 'healthy', qps: 35, latency: 5.2, hotKeys: [] },
    { id: 'shard-2', backend: 'postgresql', host: 'localhost', port: 5434, database: 'db_shard_2', weight: 1, health: 'healthy', qps: 12, latency: 4.1, hotKeys: [] },
  ],
  migrations: [],
  tenants: {
    'tenant-alpha': 'shard-0',
    'tenant-beta': 'shard-1',
    'tenant-gamma': 'shard-2',
  },
  p50Latency: 4.9,
  p99Latency: 12.4,
  totalQps: 89,
  imbalanceRatio: 1.4,
};

export async function GET() {
  // Update mock QPS slightly for dynamic feeling
  state.shards = state.shards.map((s) => {
    if (s.health === 'failed') {
      return { ...s, qps: 0, latency: 1000.0, hotKeys: [] };
    }
    const qpsVariation = Math.floor(Math.random() * 9) - 4; // -4 to 4
    const newQps = Math.max(5, s.qps + qpsVariation);
    const slowMultiplier = s.health === 'slow' ? 5.0 : 1.0;
    const newLatency = Math.max(1.0, (4.5 + Math.random() * 2) * slowMultiplier + (newQps / 50));
    return { ...s, qps: newQps, latency: parseFloat(newLatency.toFixed(1)) };
  });

  // Calculate global summary stats
  const activeQps = state.shards.reduce((acc, s) => acc + s.qps, 0);
  const activeShards = state.shards.filter((s) => s.health !== 'failed');
  const avgLatency = activeShards.length > 0
    ? activeShards.reduce((acc, s) => acc + s.latency, 0) / activeShards.length
    : 0.0;
  
  state.totalQps = activeQps;
  state.p50Latency = parseFloat(avgLatency.toFixed(1));
  state.p99Latency = parseFloat((avgLatency * 2.4).toFixed(1));

  const loads = state.shards.map((s) => s.qps);
  const maxLoad = Math.max(...loads);
  const avgLoad = loads.reduce((a, b) => a + b, 0) / loads.length;
  state.imbalanceRatio = avgLoad > 0 ? parseFloat((maxLoad / avgLoad).toFixed(2)) : 1.0;

  // Advance active running migrations
  state.migrations = state.migrations.map((m) => {
    if (m.status === 'running') {
      const step = Math.min(m.totalKeys - m.migratedKeys, Math.floor(Math.random() * 5) + 1);
      const newMigrated = m.migratedKeys + step;
      const finished = newMigrated >= m.totalKeys;
      
      // If completed, update shard mapping/balancer routing
      if (finished) {
        // Mock route cutover update
        state.shards = state.shards.map((s) => {
          if (s.id === m.dest) {
            return { ...s, qps: s.qps + 15 };
          }
          if (s.id === m.source) {
            return { ...s, qps: Math.max(0, s.qps - 15) };
          }
          return s;
        });
      }

      return {
        ...m,
        migratedKeys: newMigrated,
        status: finished ? 'completed' : 'running',
      };
    }
    return m;
  });

  return NextResponse.json(state);
}

export async function POST(request: Request) {
  const body = await request.json();
  const { action, target, parameter } = body;

  if (action === 'FAIL_NODE') {
    state.shards = state.shards.map((s) => 
      s.id === target ? { ...s, health: 'failed' } : s
    );
  } else if (action === 'SLOW_NODE') {
    state.shards = state.shards.map((s) => 
      s.id === target ? { ...s, health: 'slow' } : s
    );
  } else if (action === 'RECOVER_NODE') {
    state.shards = state.shards.map((s) => 
      s.id === target ? { ...s, health: 'healthy' } : s
    );
  } else if (action === 'ADD_NODE') {
    const newId = `shard-${state.shards.length}`;
    state.shards.push({
      id: newId,
      backend: 'postgresql',
      host: 'localhost',
      port: 5432 + state.shards.length,
      database: `db_${newId.replace('-', '_')}`,
      weight: 1,
      health: 'healthy',
      qps: 10,
      latency: 4.5,
      hotKeys: [],
    });
  } else if (action === 'TRIGGER_MIGRATION') {
    const { id, source, dest, totalKeys } = parameter;
    state.migrations.push({
      id,
      source,
      dest,
      totalKeys: totalKeys || 100,
      migratedKeys: 0,
      status: 'running',
    });
  } else if (action === 'MOVE_TENANT') {
    const { tenantId, destShard } = parameter;
    state.tenants[tenantId] = destShard;
  }

  return NextResponse.json({ success: true, state });
}
