import { describe, it, expect, vi } from 'vitest';
import {
  TelemetryCollector,
  PlacementEngine,
  Balancer,
  MuxKafkaProducer,
  MuxKafkaConsumer,
} from '../src/index';

describe('Node.js Telemetry, Placement, Balancer, & Kafka CDC Tests', () => {

  it('should collect query logs and calculate QPS, latency and hot keys', async () => {
    const collector = new TelemetryCollector(10);
    
    collector.recordQuery('shard-0', 'key-1', 10, false);
    collector.recordQuery('shard-0', 'key-1', 20, false);
    collector.recordQuery('shard-0', 'key-2', 30, true);

    expect(collector.getQps('shard-0')).toBeCloseTo(0.3, 1);
    expect(collector.getQps('shard-0', false)).toBeCloseTo(0.2, 1);
    expect(collector.getQps('shard-0', true)).toBeCloseTo(0.1, 1);

    expect(collector.getLatencyPercentile('shard-0', 50)).toBe(20);
    expect(collector.getLatencyPercentile('shard-0', 90)).toBe(30);

    const hot = collector.getHotKeys('shard-0', 0.15);
    expect(hot.length).toBe(1);
    expect(hot[0][0]).toBe('key-1');
  });

  it('should resolve targets using PlacementEngine and handle eviction', () => {
    const engine = new PlacementEngine('cache-0');
    
    expect(engine.isCached('key-1')).toBe(false);
    expect(engine.resolveReadTarget('key-1', 'storage-0')).toBe('storage-0');
    
    engine.promoteToCache('key-1');
    expect(engine.isCached('key-1')).toBe(true);
    expect(engine.resolveReadTarget('key-1', 'storage-0')).toBe('cache-0');
    
    expect(engine.resolveWriteTarget('key-1', 'storage-0')).toBe('storage-0');
    expect(engine.isCached('key-1')).toBe(false);
  });

  it('should make balancer decisions and apply suppression rule and cooldown', async () => {
    const balancer = new Balancer(1); // 1s cooldown

    // Healthy metrics
    const healthyMetrics = {
      'shard-0': { read_qps: 5, write_qps: 2 },
      'shard-1': { read_qps: 4, write_qps: 1 },
    };
    expect(balancer.evaluate(healthyMetrics).length).toBe(0);

    // Read heavy triggers replica scaling
    const replicaMetrics = {
      'shard-0': { read_qps: 60, write_qps: 2 },
    };
    const decisions = balancer.evaluate(replicaMetrics);
    expect(decisions.length).toBe(1);
    expect(decisions[0].actionType).toBe('L3_REPLICA');

    // Cooldown check (should return empty list immediately after previous balance)
    expect(balancer.evaluate(replicaMetrics).length).toBe(0);

    // Balancer with no cooldown and write suppression
    const balancerNoCooldown = new Balancer(0, 100, 50, 20);

    // L2 Split check
    const splitMetrics = {
      'shard-0': { read_qps: 120, write_qps: 10 },
    };
    const splitDecisions = balancerNoCooldown.evaluate(splitMetrics);
    expect(splitDecisions.length).toBe(1);
    expect(splitDecisions[0].actionType).toBe('L2_SPLIT');

    // Write load suppression check
    const writeHeavyMetrics = {
      'shard-0': { read_qps: 120, write_qps: 30 }, // total write QPS = 30 >= 20 suppression threshold
    };
    expect(balancerNoCooldown.evaluate(writeHeavyMetrics).length).toBe(0);
  });

  it('should route CDC event partitions with Kafka Producer/Consumer', async () => {
    const mockRouter = {
      routeKey: vi.fn().mockReturnValue({ id: 's1' }),
    };

    const producer = new MuxKafkaProducer({ 'bootstrap.servers': 'localhost:9092' }, mockRouter);
    await producer.produce('cdc-topic', 'user_42', 'updated_val');

    expect(producer.sentMessages.length).toBe(1);
    const sent = producer.sentMessages[0];
    expect(sent.topic).toBe('cdc-topic');
    expect(sent.key).toBe('user_42');
    expect(sent.targetShardId).toBe('s1');

    const consumer = new MuxKafkaConsumer({ 'bootstrap.servers': 'localhost:9092', 'group.id': 'test' });
    expect(consumer).toBeDefined();
    
    let handlerCalled = false;
    consumer.registerHandler('cdc-topic', (msg) => {
      handlerCalled = true;
    });
    consumer.subscribe(['cdc-topic']);
    await consumer.poll(0.001);
    expect(handlerCalled).toBe(false); // mock poll resolves immediately to null
  });

});
