/**
 * Adaptive connection pool — Citus-style 10ms slow-start.
 *
 * Manages per-shard connection pools with slow-start scaling,
 * transaction pinning, health checks, and idle eviction.
 */

import { PoolExhaustedError } from "./errors.js";
import type { PoolConfig, ShardConfig } from "./config.js";
import type { ShardInfo } from "./shard-map.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface PoolStats {
  shardId: string;
  active: number;
  idle: number;
  total: number;
  maxSize: number;
  waiting: number;
  totalAcquired: number;
  totalReleased: number;
  totalTimeouts: number;
  slowStartCurrent: number;
}

export interface ConnectionWrapper<T = unknown> {
  raw: T;
  shardId: string;
  createdAt: number;
  lastUsedAt: number;
  inTransaction: boolean;
  closed: boolean;
}

function createConnectionWrapper<T>(raw: T, shardId: string): ConnectionWrapper<T> {
  const now = performance.now();
  return {
    raw,
    shardId,
    createdAt: now,
    lastUsedAt: now,
    inTransaction: false,
    closed: false,
  };
}

function closeWrapper(conn: ConnectionWrapper): void {
  if (!conn.closed) {
    conn.closed = true;
    const raw = conn.raw as Record<string, unknown>;
    if (typeof raw["end"] === "function") {
      try {
        (raw["end"] as () => void)();
      } catch {
        // ignore
      }
    } else if (typeof raw["close"] === "function") {
      try {
        (raw["close"] as () => void)();
      } catch {
        // ignore
      }
    }
  }
}

// ---------------------------------------------------------------------------
// ShardPool
// ---------------------------------------------------------------------------

export class ShardPool {
  private readonly _shard: ShardInfo;
  private readonly _config: PoolConfig;
  private readonly _factory?: (shard: ShardInfo) => unknown;

  private _idle: ConnectionWrapper[] = [];
  private _active: Set<ConnectionWrapper> = new Set();
  private _slowStartCurrent: number;
  private _waiters: number = 0;

  // Stats
  private _totalAcquired: number = 0;
  private _totalReleased: number = 0;
  private _totalTimeouts: number = 0;

  constructor(shard: ShardInfo, config: PoolConfig, factory?: (shard: ShardInfo) => unknown) {
    this._shard = shard;
    this._config = config;
    this._factory = factory;
    this._slowStartCurrent = config.minSize;
  }

  get shardId(): string {
    return this._shard.id;
  }

  async acquire(timeoutMs: number = 5000): Promise<ConnectionWrapper> {
    const deadline = performance.now() + timeoutMs;

    while (true) {
      // 1. Try idle
      while (this._idle.length > 0) {
        const conn = this._idle.pop()!;
        if (!conn.closed) {
          conn.lastUsedAt = performance.now();
          this._active.add(conn);
          this._totalAcquired++;
          return conn;
        }
      }

      const total = this._active.size + this._idle.length;

      // 2. Create if within slow-start window
      if (total < this._slowStartCurrent && total < this._config.maxSize) {
        const conn = this._createConnection();
        this._active.add(conn);
        this._totalAcquired++;
        return conn;
      }

      // 3. Check absolute max
      if (total >= this._config.maxSize) {
        if (performance.now() >= deadline) {
          this._totalTimeouts++;
          throw new PoolExhaustedError(this._shard.id, this._config.maxSize);
        }
      }

      // Wait slow-start interval
      const waitMs = Math.min(this._config.slowStartMs, Math.max(0, deadline - performance.now()));
      await new Promise((resolve) => setTimeout(resolve, waitMs));

      // Increment slow-start
      const currentTotal = this._active.size + this._idle.length;
      if (currentTotal < this._config.maxSize) {
        this._slowStartCurrent = Math.min(this._slowStartCurrent + 1, this._config.maxSize);
        const conn = this._createConnection();
        this._active.add(conn);
        this._totalAcquired++;
        return conn;
      }

      if (performance.now() >= deadline) {
        this._totalTimeouts++;
        throw new PoolExhaustedError(this._shard.id, this._config.maxSize);
      }
    }
  }

  release(conn: ConnectionWrapper): void {
    this._active.delete(conn);
    conn.lastUsedAt = performance.now();
    conn.inTransaction = false;

    if (conn.closed) return;

    const total = this._active.size + this._idle.length;
    if (total >= this._config.maxSize) {
      closeWrapper(conn);
      return;
    }

    this._idle.push(conn);
    this._totalReleased++;
  }

  evictIdle(): number {
    const cutoffTime = performance.now() - this._config.idleTimeoutS * 1000;
    const stillIdle: ConnectionWrapper[] = [];
    let evicted = 0;

    for (const conn of this._idle) {
      if (conn.lastUsedAt < cutoffTime) {
        closeWrapper(conn);
        evicted++;
      } else {
        stillIdle.push(conn);
      }
    }

    this._idle = stillIdle;
    return evicted;
  }

  closeAll(): void {
    for (const conn of this._idle) closeWrapper(conn);
    for (const conn of this._active) closeWrapper(conn);
    this._idle = [];
    this._active.clear();
    this._slowStartCurrent = this._config.minSize;
  }

  stats(): PoolStats {
    return {
      shardId: this._shard.id,
      active: this._active.size,
      idle: this._idle.length,
      total: this._active.size + this._idle.length,
      maxSize: this._config.maxSize,
      waiting: this._waiters,
      totalAcquired: this._totalAcquired,
      totalReleased: this._totalReleased,
      totalTimeouts: this._totalTimeouts,
      slowStartCurrent: this._slowStartCurrent,
    };
  }

  private _createConnection(): ConnectionWrapper {
    let raw: unknown;
    if (this._factory) {
      raw = this._factory(this._shard);
    } else {
      raw = { _placeholder: true, shard: this._shard.id };
    }
    return createConnectionWrapper(raw, this._shard.id);
  }
}

// ---------------------------------------------------------------------------
// AdaptivePool
// ---------------------------------------------------------------------------

export class AdaptivePool {
  private readonly _config: PoolConfig;
  private readonly _pools: Map<string, ShardPool> = new Map();
  private readonly _factory?: (shard: ShardInfo) => unknown;

  constructor(
    config: PoolConfig,
    shards: readonly ShardInfo[],
    factory?: (shard: ShardInfo) => unknown,
  ) {
    this._config = config;
    this._factory = factory;

    for (const shard of shards) {
      this._pools.set(shard.id, new ShardPool(shard, config, factory));
    }
  }

  async acquire(shardId: string, timeoutMs: number = 5000): Promise<ConnectionWrapper> {
    const pool = this._pools.get(shardId);
    if (!pool) throw new PoolExhaustedError(shardId, 0);
    return pool.acquire(timeoutMs);
  }

  release(conn: ConnectionWrapper): void {
    const pool = this._pools.get(conn.shardId);
    if (pool) pool.release(conn);
  }

  addShard(shard: ShardInfo): void {
    if (!this._pools.has(shard.id)) {
      this._pools.set(shard.id, new ShardPool(shard, this._config, this._factory));
    }
  }

  removeShard(shardId: string): void {
    const pool = this._pools.get(shardId);
    if (pool) {
      pool.closeAll();
      this._pools.delete(shardId);
    }
  }

  evictAllIdle(): number {
    let total = 0;
    for (const pool of this._pools.values()) {
      total += pool.evictIdle();
    }
    return total;
  }

  closeAll(): void {
    for (const pool of this._pools.values()) {
      pool.closeAll();
    }
  }

  stats(): Map<string, PoolStats> {
    const result = new Map<string, PoolStats>();
    for (const [id, pool] of this._pools) {
      result.set(id, pool.stats());
    }
    return result;
  }
}
