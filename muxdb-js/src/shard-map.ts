/**
 * Shard map — in-memory routing table.
 *
 * Implements three strategies for mapping keys to shards:
 *   - Hash: Simple modulo hashing
 *   - Range: Explicit key ranges per shard
 *   - Consistent Hash: Virtual-node consistent hashing
 *
 * Supports atomic topology swaps via versioned snapshots (bridge-lease model).
 */

import { createHash } from "node:crypto";
import { ConfigError, ShardNotFoundError } from "./errors.js";
import type { MuxConfig, ShardConfig } from "./config.js";

// ---------------------------------------------------------------------------
// ShardInfo
// ---------------------------------------------------------------------------

export interface ShardInfo {
  readonly id: string;
  readonly backend: string;
  readonly host: string;
  readonly port: number;
  readonly database: string;
  readonly weight: number;
  readonly dsn: string;
}

function shardInfoFromConfig(cfg: ShardConfig): ShardInfo {
  let userPart = "";
  if (cfg.username) {
    userPart = cfg.username;
    if (cfg.password) userPart += `:${cfg.password}`;
    userPart += "@";
  }
  return {
    id: cfg.id,
    backend: cfg.backend,
    host: cfg.host,
    port: cfg.port,
    database: cfg.database,
    weight: cfg.weight,
    dsn: `${cfg.backend}://${userPart}${cfg.host}:${cfg.port}/${cfg.database}`,
  };
}

// ---------------------------------------------------------------------------
// Hash utility
// ---------------------------------------------------------------------------

function hashKey(key: string | number): number {
  const raw = String(key);
  const digest = createHash("md5").update(raw).digest("hex");
  return parseInt(digest.substring(0, 8), 16);
}

// ---------------------------------------------------------------------------
// ShardMap
// ---------------------------------------------------------------------------

export class ShardMap {
  private _shards: ShardInfo[];
  private _shardIndex: Map<string, ShardInfo>;
  private _strategy: string;
  private _virtualNodes: number;
  private _version: number;

  // Consistent hash ring
  private _ring: Array<{ position: number; shardId: string }>;
  private _ringPositions: number[];

  // Range data (kept separate from ShardInfo for clean separation)
  private _rangeMap: Map<string, { start: number; end: number }>;

  constructor(
    shards: ShardInfo[],
    strategy: string,
    virtualNodes: number = 256,
    rangeMap?: Map<string, { start: number; end: number }>,
  ) {
    this._shards = [...shards];
    this._shardIndex = new Map(shards.map((s) => [s.id, s]));
    this._strategy = strategy;
    this._virtualNodes = virtualNodes;
    this._version = 0;
    this._ring = [];
    this._ringPositions = [];
    this._rangeMap = rangeMap ?? new Map();

    if (strategy === "consistent_hash") {
      this._buildRing();
    }
  }

  static fromConfig(config: MuxConfig): ShardMap {
    const shards = config.shards.map(shardInfoFromConfig);
    const rangeMap = new Map<string, { start: number; end: number }>();

    for (const s of config.shards) {
      if (s.range) {
        rangeMap.set(s.id, { start: s.range.start, end: s.range.end });
      }
    }

    return new ShardMap(shards, config.cluster.strategy, config.cluster.virtualNodes, rangeMap);
  }

  // --- Ring management ---

  private _buildRing(): void {
    this._ring = [];

    for (const shard of this._shards) {
      for (let vn = 0; vn < shard.weight * this._virtualNodes; vn++) {
        const token = `${shard.id}:vn${vn}`;
        const pos = hashKey(token);
        this._ring.push({ position: pos, shardId: shard.id });
      }
    }

    this._ring.sort((a, b) => a.position - b.position);
    this._ringPositions = this._ring.map((r) => r.position);
  }

  // --- Resolution ---

  resolve(key: string | number): ShardInfo {
    switch (this._strategy) {
      case "hash":
        return this._resolveHash(key);
      case "range":
        return this._resolveRange(key);
      case "consistent_hash":
        return this._resolveConsistent(key);
      default:
        throw new ConfigError(`Unknown strategy: ${this._strategy}`);
    }
  }

  private _resolveHash(key: string | number): ShardInfo {
    if (!this._shards.length) throw new ShardNotFoundError(key);

    const h = hashKey(key);
    // Build weighted array
    const weighted: ShardInfo[] = [];
    for (const shard of this._shards) {
      for (let i = 0; i < shard.weight; i++) {
        weighted.push(shard);
      }
    }

    if (!weighted.length) throw new ShardNotFoundError(key);
    const idx = h % weighted.length;
    return weighted[idx]!;
  }

  private _resolveRange(key: string | number): ShardInfo {
    const numericKey = typeof key === "number" ? key : parseInt(String(key), 10);
    if (isNaN(numericKey)) {
      throw new ShardNotFoundError(key, { reason: "Range strategy requires a numeric key" });
    }

    for (const shard of this._shards) {
      const range = this._rangeMap.get(shard.id);
      if (range && numericKey >= range.start && numericKey <= range.end) {
        return shard;
      }
    }

    throw new ShardNotFoundError(key, { reason: "Key not in any shard range" });
  }

  private _resolveConsistent(key: string | number): ShardInfo {
    if (!this._ring.length) throw new ShardNotFoundError(key);

    const h = hashKey(key);

    // Binary search for the first position >= h
    let lo = 0;
    let hi = this._ringPositions.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this._ringPositions[mid]! < h) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    const idx = lo % this._ring.length;
    const shardId = this._ring[idx]!.shardId;
    const shard = this._shardIndex.get(shardId);
    if (!shard) throw new ShardNotFoundError(key);
    return shard;
  }

  // --- Multi-key ---

  resolveMany(keys: (string | number)[]): Map<string, (string | number)[]> {
    const groups = new Map<string, (string | number)[]>();
    for (const key of keys) {
      const shard = this.resolve(key);
      const list = groups.get(shard.id) ?? [];
      list.push(key);
      groups.set(shard.id, list);
    }
    return groups;
  }

  // --- Topology management ---

  swap(newShards: ShardInfo[]): number {
    const oldIds = new Set(this._shards.map((s) => s.id));
    const newIds = new Set(newShards.map((s) => s.id));

    this._shards = [...newShards];
    this._shardIndex = new Map(newShards.map((s) => [s.id, s]));
    this._version++;

    if (this._strategy === "consistent_hash") {
      this._buildRing();
    }

    return this._version;
  }

  // --- Accessors ---

  get version(): number {
    return this._version;
  }

  get shards(): readonly ShardInfo[] {
    return [...this._shards];
  }

  get shardIds(): string[] {
    return this._shards.map((s) => s.id);
  }

  getShard(shardId: string): ShardInfo {
    const shard = this._shardIndex.get(shardId);
    if (!shard) {
      throw new ShardNotFoundError(shardId, { reason: "Shard ID not in shard map" });
    }
    return shard;
  }

  get size(): number {
    return this._shards.length;
  }
}
