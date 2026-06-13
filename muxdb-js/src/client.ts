/**
 * MuxDB client — the main entry point.
 *
 * Provides a single logical database interface that transparently routes
 * queries across shards, manages adaptive connection pools, and orchestrates
 * scatter-gather for cross-shard operations.
 */

import { MuxDBError, RoutingError, CrossShardTransactionError } from "./errors.js";
import type { MuxConfig } from "./config.js";
import type { Driver, QueryResult } from "./drivers/base.js";
import { ShardMap, type ShardInfo } from "./shard-map.js";
import { Router, RouteType, type QueryContext } from "./router.js";
import { AdaptivePool, type ConnectionWrapper } from "./pool.js";

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

export class MuxTransaction {
  private readonly _shard: ShardInfo;
  private readonly _conn: ConnectionWrapper;
  private readonly _driver: Driver;
  private readonly _router: Router;
  private _committed = false;
  private _rolledBack = false;

  constructor(shard: ShardInfo, conn: ConnectionWrapper, driver: Driver, router: Router) {
    this._shard = shard;
    this._conn = conn;
    this._driver = driver;
    this._router = router;
  }

  get shardId(): string {
    return this._shard.id;
  }

  async execute(query: string, params: unknown[] = []): Promise<QueryResult> {
    const ctx: QueryContext = {
      query,
      params,
      transactionShard: this._shard.id,
    };
    const decision = this._router.route(ctx);

    // Verify single-shard constraint
    for (const target of decision.targets) {
      if (target.id !== this._shard.id) {
        throw new CrossShardTransactionError([this._shard.id, target.id]);
      }
    }

    const result = await this._driver.execute(this._conn.raw, query, params);
    return { ...result, shardId: this._shard.id };
  }

  async commit(): Promise<void> {
    if (!this._committed && !this._rolledBack) {
      await this._driver.commit(this._conn.raw);
      this._committed = true;
    }
  }

  async rollback(): Promise<void> {
    if (!this._committed && !this._rolledBack) {
      await this._driver.rollback(this._conn.raw);
      this._rolledBack = true;
    }
  }
}

// ---------------------------------------------------------------------------
// MuxDB Client
// ---------------------------------------------------------------------------

export class MuxDB {
  private readonly _config: MuxConfig;
  private _shardMap: ShardMap | null = null;
  private _router: Router | null = null;
  private _pool: AdaptivePool | null = null;
  private _driver: Driver | null = null;
  private _connected = false;

  constructor(config: MuxConfig) {
    this._config = config;
  }

  get config(): MuxConfig {
    return this._config;
  }

  get shardMap(): ShardMap {
    if (!this._shardMap) throw new MuxDBError("MuxDB is not connected. Call db.connect() first.");
    return this._shardMap;
  }

  get router(): Router {
    if (!this._router) throw new MuxDBError("MuxDB is not connected. Call db.connect() first.");
    return this._router;
  }

  get pool(): AdaptivePool {
    if (!this._pool) throw new MuxDBError("MuxDB is not connected. Call db.connect() first.");
    return this._pool;
  }

  get isConnected(): boolean {
    return this._connected;
  }

  async connect(driver?: Driver): Promise<MuxDB> {
    if (this._connected) return this;

    this._shardMap = ShardMap.fromConfig(this._config);
    this._router = new Router(this._config, this._shardMap);

    if (driver) {
      this._driver = driver;
    } else {
      this._driver = await this._autoDetectDriver();
    }

    this._pool = new AdaptivePool(this._config.pool, this._shardMap.shards, (shard) =>
      this._driver!.connect(shard),
    );

    this._connected = true;
    return this;
  }

  async close(): Promise<void> {
    if (this._pool) {
      this._pool.closeAll();
    }
    this._connected = false;
  }

  // --- Query execution ---

  async execute(
    query: string,
    params: unknown[] = [],
    opts?: { shardKey?: string | number },
  ): Promise<QueryResult> {
    this._ensureConnected();

    const ctx: QueryContext = {
      query,
      params,
      shardKeyValue: opts?.shardKey,
    };

    const decision = this._router!.route(ctx);

    switch (decision.type) {
      case RouteType.SINGLE:
        return this._executeSingle(decision.targets[0]!, query, params);
      case RouteType.SCATTER:
        return this._executeScatter(decision.targets, query, params);
      case RouteType.BROADCAST:
        return this._executeBroadcast(decision.targets, query, params);
      default:
        throw new RoutingError(`Unknown route type: ${decision.type}`);
    }
  }

  async executeOnShard(shardId: string, query: string, params: unknown[] = []): Promise<QueryResult> {
    this._ensureConnected();
    const shard = this._shardMap!.getShard(shardId);
    return this._executeOnTarget(shard, query, params);
  }

  // --- Transaction ---

  async transaction<T>(
    shardKeyOrId: { shardKey?: string | number; shardId?: string },
    fn: (tx: MuxTransaction) => Promise<T>,
  ): Promise<T> {
    this._ensureConnected();

    let shard: ShardInfo;
    if (shardKeyOrId.shardId) {
      shard = this._shardMap!.getShard(shardKeyOrId.shardId);
    } else if (shardKeyOrId.shardKey !== undefined) {
      shard = this._shardMap!.resolve(shardKeyOrId.shardKey);
    } else {
      throw new MuxDBError(
        "transaction() requires either shardKey or shardId. " +
          "Transactions must be pinned to a single shard.",
      );
    }

    const conn = await this._pool!.acquire(shard.id);
    conn.inTransaction = true;

    try {
      await this._driver!.begin(conn.raw);
      const tx = new MuxTransaction(shard, conn, this._driver!, this._router!);
      const result = await fn(tx);
      await tx.commit();
      return result;
    } catch (err) {
      try {
        await this._driver!.rollback(conn.raw);
      } catch {
        // ignore rollback error
      }
      throw err;
    } finally {
      conn.inTransaction = false;
      this._pool!.release(conn);
    }
  }

  // --- Internal ---

  private async _executeSingle(
    target: ShardInfo,
    query: string,
    params: unknown[],
  ): Promise<QueryResult> {
    return this._executeOnTarget(target, query, params);
  }

  private async _executeScatter(
    targets: readonly ShardInfo[],
    query: string,
    params: unknown[],
  ): Promise<QueryResult> {
    const results = await Promise.all(
      targets.map((target) => this._executeOnTarget(target, query, params)),
    );

    const allRows: Record<string, unknown>[] = [];
    let totalCount = 0;
    let totalElapsed = 0;
    let columns: string[] = [];

    for (const result of results) {
      allRows.push(...result.rows);
      totalCount += result.rowCount;
      totalElapsed += result.elapsedMs;
      if (!columns.length && result.columns.length) {
        columns = result.columns;
      }
    }

    return {
      rows: allRows,
      rowCount: totalCount,
      columns,
      shardId: "scatter",
      elapsedMs: totalElapsed,
    };
  }

  private async _executeBroadcast(
    targets: readonly ShardInfo[],
    query: string,
    params: unknown[],
  ): Promise<QueryResult> {
    const results = await Promise.all(
      targets.map((target) => this._executeOnTarget(target, query, params)),
    );
    return results[results.length - 1] ?? { rows: [], rowCount: 0, columns: [], shardId: "broadcast", elapsedMs: 0 };
  }

  private async _executeOnTarget(
    target: ShardInfo,
    query: string,
    params: unknown[],
  ): Promise<QueryResult> {
    const conn = await this._pool!.acquire(target.id);
    try {
      const result = await this._driver!.execute(conn.raw, query, params);
      return { ...result, shardId: target.id };
    } finally {
      this._pool!.release(conn);
    }
  }

  private _ensureConnected(): void {
    if (!this._connected) {
      throw new MuxDBError("MuxDB is not connected. Call db.connect() first.");
    }
  }

  private async _autoDetectDriver(): Promise<Driver> {
    if (!this._config.shards.length) {
      throw new MuxDBError("No shards configured — cannot auto-detect driver");
    }

    const backend = this._config.shards[0]!.backend;

    if (backend === "postgresql") {
      const { PostgreSQLDriver } = await import("./drivers/postgresql.js");
      return new PostgreSQLDriver();
    }

    throw new MuxDBError(
      `No auto-detected driver for backend '${backend}'. ` +
        `Provide a driver explicitly via db.connect(driver)`,
    );
  }

  // --- Status ---

  status(): Record<string, unknown> {
    const result: Record<string, unknown> = {
      connected: this._connected,
      cluster: this._config.cluster.name,
      strategy: this._config.cluster.strategy,
    };

    if (this._shardMap) {
      result["shardCount"] = this._shardMap.size;
      result["shardMapVersion"] = this._shardMap.version;
      result["shardIds"] = this._shardMap.shardIds;
    }

    if (this._pool) {
      const poolStats: Record<string, unknown> = {};
      for (const [sid, s] of this._pool.stats()) {
        poolStats[sid] = {
          active: s.active,
          idle: s.idle,
          total: s.total,
          max: s.maxSize,
        };
      }
      result["poolStats"] = poolStats;
    }

    return result;
  }
}
