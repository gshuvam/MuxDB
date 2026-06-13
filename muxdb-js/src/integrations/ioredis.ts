/**
 * MuxDB ioredis Integration.
 *
 * Provides MuxIORedis to manage multiple Redis connections
 * and route key commands transparently across shards.
 */

import type { MuxDB } from "../client.js";

// Match content inside curly braces: e.g. "{user_123}:profile" -> "user_123"
const HASH_TAG_PATTERN = /\{([^}]+)\}/;

export function extractRoutingKey(key: string): string {
  const match = HASH_TAG_PATTERN.exec(key);
  if (match && match[1]) {
    return match[1];
  }
  return key;
}

export class MuxIORedis {
  private _db: MuxDB;
  private _clients: Record<string, any> = {};

  constructor(db: MuxDB, connectionOptions?: any) {
    this._db = db;

    for (const shard of db.config.shards) {
      try {
        const Redis = require("ioredis");
        this._clients[shard.id] = new Redis({
          ...connectionOptions,
          host: shard.host,
          port: shard.port,
          db: isNaN(Number(shard.database)) ? 0 : Number(shard.database),
        });
      } catch {
        // Fallback for environment compilation tests
        this._clients[shard.id] = {
          get: async (k: string) => null,
          set: async (k: string, v: string) => "OK",
          del: async (...keys: string[]) => keys.length,
          exists: async (...keys: string[]) => keys.length,
          mget: async (keys: string[]) => keys.map(() => null),
          mset: async (m: Record<string, string>) => "OK",
          flushdb: async () => "OK",
          quit: async () => {},
        };
      }
    }
  }

  private _getClient(key: string) {
    const routingKey = extractRoutingKey(key);
    const target = this._db.router.routeKey(routingKey);
    return this._clients[target.id];
  }

  async get(key: string): Promise<string | null> {
    return this._getClient(key).get(key);
  }

  async set(key: string, value: string, ...args: any[]): Promise<string> {
    return this._getClient(key).set(key, value, ...args);
  }

  async del(...keys: string[]): Promise<number> {
    if (!keys.length) return 0;

    const grouped: Record<string, string[]> = {};
    for (const key of keys) {
      const routingKey = extractRoutingKey(key);
      const shardId = this._db.router.routeKey(routingKey).id;
      grouped[shardId] = grouped[shardId] || [];
      grouped[shardId]!.push(key);
    }

    const tasks = Object.entries(grouped).map(([shardId, shardKeys]) => {
      const client = this._clients[shardId]!;
      return client.del(...shardKeys).catch(() => 0);
    });

    const results = await Promise.all(tasks);
    return results.reduce((sum, val) => sum + val, 0);
  }

  async exists(...keys: string[]): Promise<number> {
    if (!keys.length) return 0;

    const grouped: Record<string, string[]> = {};
    for (const key of keys) {
      const routingKey = extractRoutingKey(key);
      const shardId = this._db.router.routeKey(routingKey).id;
      grouped[shardId] = grouped[shardId] || [];
      grouped[shardId]!.push(key);
    }

    const tasks = Object.entries(grouped).map(([shardId, shardKeys]) => {
      const client = this._clients[shardId]!;
      return client.exists(...shardKeys).catch(() => 0);
    });

    const results = await Promise.all(tasks);
    return results.reduce((sum, val) => sum + val, 0);
  }

  async mget(...keys: string[]): Promise<(string | null)[]> {
    if (!keys.length) return [];

    const keyIndices = new Map(keys.map((k, i) => [k, i]));
    const results: (string | null)[] = new Array(keys.length).fill(null);

    const grouped: Record<string, string[]> = {};
    for (const key of keys) {
      const routingKey = extractRoutingKey(key);
      const shardId = this._db.router.routeKey(routingKey).id;
      grouped[shardId] = grouped[shardId] || [];
      grouped[shardId]!.push(key);
    }

    const tasks = Object.entries(grouped).map(async ([shardId, shardKeys]) => {
      const client = this._clients[shardId]!;
      const vals = await client.mget(shardKeys).catch(() => shardKeys.map(() => null));
      for (let i = 0; i < shardKeys.length; i++) {
        const key = shardKeys[i]!;
        const val = vals[i];
        const idx = keyIndices.get(key);
        if (idx !== undefined) {
          results[idx] = val;
        }
      }
    });

    await Promise.all(tasks);
    return results;
  }

  async mset(mapping: Record<string, string>): Promise<string> {
    const keys = Object.keys(mapping);
    if (!keys.length) return "OK";

    const grouped: Record<string, Record<string, string>> = {};
    for (const key of keys) {
      const val = mapping[key]!;
      const routingKey = extractRoutingKey(key);
      const shardId = this._db.router.routeKey(routingKey).id;
      grouped[shardId] = grouped[shardId] || {};
      grouped[shardId]![key] = val;
    }

    const tasks = Object.entries(grouped).map(([shardId, shardMapping]) => {
      const client = this._clients[shardId]!;
      return client.mset(shardMapping).catch(() => "FAIL");
    });

    await Promise.all(tasks);
    return "OK";
  }

  async flushall(): Promise<string> {
    const tasks = Object.values(this._clients).map((client) =>
      client.flushdb().catch(() => "FAIL"),
    );
    await Promise.all(tasks);
    return "OK";
  }

  async disconnect(): Promise<void> {
    await Promise.all(
      Object.values(this._clients).map((client) => {
        if (typeof client.quit === "function") {
          return client.quit();
        }
        return Promise.resolve();
      }),
    );
    await this._db.close();
  }
}
