import { describe, it, expect, vi } from 'vitest';
import {
  LiveMigrator,
  PIDController,
  BridgeLeaseManager,
  ShardMapSynchronizer,
  ControlPlaneGRPCServer,
} from '../src/index';

describe('Node.js Live Migration & gRPC Control Plane Tests', () => {

  // --- Live Migration & PID Controller Tests ---

  it('should adjust PID delay values based on latency feedback updates', () => {
    const controller = new PIDController(0.5, 0.1, 0.2, 0.1);
    const initialDelay = controller.currentDelay;

    // Positive latency deviation (higher delay requested to throttle)
    const delay1 = controller.update(0.2);
    expect(delay1).toBeGreaterThan(initialDelay);

    // Negative latency deviation (lower delay to speed up)
    const delay2 = controller.update(0.0);
    expect(delay2).toBeLessThan(delay1);
  });

  it('should handle on-demand key pulling in LiveMigrator', () => {
    const migrator = new LiveMigrator();
    migrator.startMigration('mig-0', 's0', 's1', ['user_1', 'user_2']);

    expect(migrator.isMigratingKey('user_1')).toBe(true);
    expect(migrator.isMigratingKey('user_3')).toBe(false);

    // Set mock data on active migration
    const activeMig = (migrator as any).activeMigrations.get('mig-0')!;
    activeMig.dataStore.set('user_1', 'data_1');

    expect(migrator.onDemandPull('user_1')).toBe('data_1');
    // Second pull returns null (already migrated)
    expect(migrator.onDemandPull('user_1')).toBeNull();
  });

  it('should execute sequential cold push steps in LiveMigrator', async () => {
    const migrator = new LiveMigrator();
    migrator.startMigration('mig-1', 's0', 's1', ['k1', 'k2', 'k3', 'k4', 'k5']);

    // Step with batch size 3
    const hasMore1 = await migrator.coldPushStep('mig-1', 3);
    expect(hasMore1).toBe(true);

    const activeMig = (migrator as any).activeMigrations.get('mig-1')!;
    expect(activeMig.pendingKeys.size).toBe(2);
    expect(activeMig.migratedKeys.size).toBe(3);

    // Step with remaining (batch size 4)
    const hasMore2 = await migrator.coldPushStep('mig-1', 4);
    expect(hasMore2).toBe(false);
    expect(activeMig.pendingKeys.size).toBe(0);

    // Tune delay check
    const delay = migrator.adjustThrottling(150, 100);
    expect(delay).toBeGreaterThan(0);
  });

  // --- Control Plane Tests ---

  it('should coordinate lock-like leases in BridgeLeaseManager', async () => {
    const manager = new BridgeLeaseManager();

    expect(manager.acquireLease('lease-1', 'node-A', 0.1)).toBe(true);
    expect(manager.isLeaseActive('lease-1')).toBe(true);
    expect(manager.getHolder('lease-1')).toBe('node-A');

    // Refuse duplicate lease for other client
    expect(manager.acquireLease('lease-1', 'node-B', 0.1)).toBe(false);

    // Renew
    expect(manager.renewLease('lease-1', 'node-A', 0.2)).toBe(true);

    // Release
    expect(manager.releaseLease('lease-1', 'node-A')).toBe(true);
    expect(manager.isLeaseActive('lease-1')).toBe(false);
  });

  it('should synchronize shard map updates in ShardMapSynchronizer', () => {
    const sync = new ShardMapSynchronizer('node-1');

    const topo1 = { s0: 'localhost:5432' };
    const topo2 = { s0: 'localhost:5432', s1: 'localhost:5433' };

    // Reject older update
    expect(sync.acceptMapUpdate('node-2', 0, topo1)).toBe(false);

    // Accept newer update
    expect(sync.acceptMapUpdate('node-2', 5, topo2)).toBe(true);
    expect(sync.currentVersion).toBe(5);
    expect(sync.topology).toEqual(topo2);

    // Broadcast update
    sync.broadcastMapUpdate(10, topo1);
    expect(sync.currentVersion).toBe(10);
  });

  it('should coordinate calls in ControlPlaneGRPCServer', () => {
    const server = new ControlPlaneGRPCServer('node-0');

    expect(server.acquireLease('lease-x', 'node-A')).toBe(false);

    server.start(9090);
    expect(server.isRunning).toBe(true);
    expect(server.port).toBe(9090);

    expect(server.acquireLease('lease-x', 'node-A')).toBe(true);
    expect(server.syncShardMap('node-1', 2, { s0: 'localhost:5432' })).toBe(true);
    expect(server.releaseLease('lease-x', 'node-A')).toBe(true);

    server.stop();
    expect(server.isRunning).toBe(false);
    expect(server.port).toBeNull();
  });

});
