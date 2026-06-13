/**
 * Query router — Layer 1 core.
 *
 * Parses queries, extracts shard keys, resolves targets via ShardMap,
 * and orchestrates scatter-gather for cross-shard queries.
 */

import { RoutingError, CrossShardTransactionError } from "./errors.js";
import type { MuxConfig } from "./config.js";
import { ShardMap, type ShardInfo } from "./shard-map.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export enum RouteType {
  SINGLE = "SINGLE",
  SCATTER = "SCATTER",
  BROADCAST = "BROADCAST",
}

export interface RouteDecision {
  readonly type: RouteType;
  readonly targets: readonly ShardInfo[];
  readonly shardKeyValue: string | number | null;
  readonly query: string;
  readonly params: unknown[];
  readonly elapsedUs: number;
}

export interface QueryContext {
  readonly query: string;
  readonly params: unknown[];
  readonly shardKey?: string;
  readonly shardKeyValue?: string | number;
  readonly transactionShard?: string;
  readonly readOnly?: boolean;
}

// ---------------------------------------------------------------------------
// SQL shard key extraction
// ---------------------------------------------------------------------------

const DDL_KEYWORDS = new Set(["CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE"]);

function extractShardKey(
  query: string,
  shardKey: string,
  params: unknown[],
): string | number | null {
  const escapedKey = shardKey.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const pattern = new RegExp(
    `\\b${escapedKey}\\b\\s*=\\s*(?:'([^']*)'|(\\d+)|(\\%s|\\$(\\d+)|\\?))`,
    "i",
  );

  const match = pattern.exec(query);
  if (!match) return null;

  // String literal
  if (match[1] !== undefined) return match[1];

  // Numeric literal
  if (match[2] !== undefined) return parseInt(match[2], 10);

  // Parameterized placeholder
  if (match[3] !== undefined) {
    // $N style (asyncpg / pg)
    if (match[4] !== undefined) {
      const idx = parseInt(match[4], 10) - 1;
      if (idx >= 0 && idx < params.length) return params[idx] as string | number;
      return null;
    }

    // %s or ? — count preceding placeholders
    const prefix = query.substring(0, match.index);
    const priorCount = (prefix.match(/%s|\?/g) ?? []).length;
    if (priorCount < params.length) return params[priorCount] as string | number;
  }

  return null;
}

function isDDL(query: string): boolean {
  const firstWord = query.trim().split(/\s+/)[0]?.toUpperCase() ?? "";
  return DDL_KEYWORDS.has(firstWord);
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

export class Router {
  private readonly _shardKey: string;
  private readonly _scatterEnabled: boolean;
  private readonly _shardMap: ShardMap;

  constructor(config: MuxConfig, shardMap?: ShardMap) {
    this._shardKey = config.cluster.shardKey;
    this._scatterEnabled = config.routing.scatterGather;
    this._shardMap = shardMap ?? ShardMap.fromConfig(config);
  }

  get shardMap(): ShardMap {
    return this._shardMap;
  }

  route(ctxOrQuery: QueryContext | string, params: unknown[] = []): RouteDecision {
    const t0 = performance.now();

    const ctx: QueryContext =
      typeof ctxOrQuery === "string"
        ? { query: ctxOrQuery, params }
        : ctxOrQuery;

    const decision = this._resolve(ctx);
    const elapsedUs = (performance.now() - t0) * 1000;

    return {
      ...decision,
      query: ctx.query,
      params: ctx.params,
      elapsedUs,
    };
  }

  routeKey(key: string | number): ShardInfo {
    return this._shardMap.resolve(key);
  }

  routeKeys(keys: (string | number)[]): Map<string, (string | number)[]> {
    return this._shardMap.resolveMany(keys);
  }

  private _resolve(ctx: QueryContext): Omit<RouteDecision, "query" | "params" | "elapsedUs"> {
    // 1. DDL → broadcast
    if (isDDL(ctx.query)) {
      return {
        type: RouteType.BROADCAST,
        targets: this._shardMap.shards,
        shardKeyValue: null,
      };
    }

    // 2. Transaction pinning
    if (ctx.transactionShard) {
      const shard = this._shardMap.getShard(ctx.transactionShard);
      return {
        type: RouteType.SINGLE,
        targets: [shard],
        shardKeyValue: ctx.shardKeyValue ?? null,
      };
    }

    // 3. Explicit shard key value
    if (ctx.shardKeyValue !== undefined) {
      const shard = this._shardMap.resolve(ctx.shardKeyValue);
      return {
        type: RouteType.SINGLE,
        targets: [shard],
        shardKeyValue: ctx.shardKeyValue,
      };
    }

    // 4. Extract from query
    const effectiveKey = ctx.shardKey ?? this._shardKey;
    const extracted = extractShardKey(ctx.query, effectiveKey, ctx.params);

    if (extracted !== null) {
      const shard = this._shardMap.resolve(extracted);
      return {
        type: RouteType.SINGLE,
        targets: [shard],
        shardKeyValue: extracted,
      };
    }

    // 5. Fallback: scatter-gather
    if (this._scatterEnabled) {
      return {
        type: RouteType.SCATTER,
        targets: this._shardMap.shards,
        shardKeyValue: null,
      };
    }

    throw new RoutingError(
      `Cannot determine shard for query and scatter_gather is disabled. ` +
        `Provide a '${effectiveKey}' value in the WHERE clause or set ` +
        `routing.scatterGather=true in config.`,
      { query_prefix: ctx.query.substring(0, 80) },
    );
  }
}
