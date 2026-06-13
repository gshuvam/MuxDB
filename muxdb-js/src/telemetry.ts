export class TelemetryCollector {
  private windowSeconds: number;
  private queryLogs: Map<string, Array<{ timestamp: number; latencyMs: number; isWrite: boolean }>> = new Map();
  private keyLogs: Map<string, Map<string, number[]>> = new Map();

  constructor(windowSeconds: number = 60) {
    this.windowSeconds = windowSeconds;
  }

  public recordQuery(shardId: string, key: string | null, latencyMs: number, isWrite: boolean): void {
    const now = Date.now() / 1000;
    
    let logs = this.queryLogs.get(shardId);
    if (!logs) {
      logs = [];
      this.queryLogs.set(shardId, logs);
    }
    logs.push({ timestamp: now, latencyMs, isWrite });

    if (key) {
      let shardKeyMap = this.keyLogs.get(shardId);
      if (!shardKeyMap) {
        shardKeyMap = new Map();
        this.keyLogs.set(shardId, shardKeyMap);
      }
      let keyTimestamps = shardKeyMap.get(key);
      if (!keyTimestamps) {
        keyTimestamps = [];
        shardKeyMap.set(key, keyTimestamps);
      }
      keyTimestamps.push(now);
    }
  }

  private prune(now: number): void {
    const cutoff = now - this.windowSeconds;
    
    for (const logs of this.queryLogs.values()) {
      let firstValidIdx = 0;
      while (firstValidIdx < logs.length && (logs[firstValidIdx] as any).timestamp < cutoff) {
        firstValidIdx++;
      }
      if (firstValidIdx > 0) {
        logs.splice(0, firstValidIdx);
      }
    }

    for (const shardKeyMap of this.keyLogs.values()) {
      for (const [key, timestamps] of shardKeyMap.entries()) {
        let firstValidIdx = 0;
        while (firstValidIdx < timestamps.length && (timestamps[firstValidIdx] as any) < cutoff) {
          firstValidIdx++;
        }
        if (firstValidIdx > 0) {
          timestamps.splice(0, firstValidIdx);
        }
        if (timestamps.length === 0) {
          shardKeyMap.delete(key);
        }
      }
    }
  }

  public getQps(shardId: string, isWrite?: boolean): number {
    const now = Date.now() / 1000;
    this.prune(now);
    const logs = this.queryLogs.get(shardId);
    if (!logs || logs.length === 0) return 0;

    if (isWrite === undefined) {
      return logs.length / this.windowSeconds;
    }
    const count = logs.filter((l) => l.isWrite === isWrite).length;
    return count / this.windowSeconds;
  }

  public getLatencyPercentile(shardId: string, percentile: number): number {
    const now = Date.now() / 1000;
    this.prune(now);
    const logs = this.queryLogs.get(shardId);
    if (!logs || logs.length === 0) return 0;

    const latencies = logs.map((l) => l.latencyMs).sort((a, b) => a - b);
    const idx = Math.min(latencies.length - 1, Math.max(0, Math.floor(latencies.length * (percentile / 100))));
    return latencies[idx] as number;
  }

  public getHotKeys(shardId: string, thresholdQps: number): Array<[string, number]> {
    const now = Date.now() / 1000;
    this.prune(now);
    const shardKeyMap = this.keyLogs.get(shardId);
    if (!shardKeyMap) return [];

    const hot: Array<[string, number]> = [];
    for (const [key, timestamps] of shardKeyMap.entries()) {
      const qps = timestamps.length / this.windowSeconds;
      if (qps >= thresholdQps) {
        hot.push([key, qps]);
      }
    }
    return hot.sort((a, b) => b[1] - a[1]);
  }

  public getMetrics(): Record<string, any> {
    const now = Date.now() / 1000;
    this.prune(now);
    const metrics: Record<string, any> = {};
    for (const shardId of this.queryLogs.keys()) {
      metrics[shardId] = {
        read_qps: this.getQps(shardId, false),
        write_qps: this.getQps(shardId, true),
        p50_latency_ms: this.getLatencyPercentile(shardId, 50),
        p90_latency_ms: this.getLatencyPercentile(shardId, 90),
        p99_latency_ms: this.getLatencyPercentile(shardId, 99),
      };
    }
    return metrics;
  }
}
