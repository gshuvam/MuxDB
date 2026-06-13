/**
 * Base driver interface.
 *
 * All MuxDB backend drivers implement this interface, allowing the core
 * router and pool to work identically regardless of database technology.
 */

import type { ShardInfo } from "../shard-map.js";

export interface QueryResult {
  rows: Record<string, unknown>[];
  rowCount: number;
  columns: string[];
  shardId: string;
  elapsedMs: number;
}

export interface Driver {
  readonly backendName: string;

  connect(shard: ShardInfo): unknown | Promise<unknown>;
  close(connection: unknown): void | Promise<void>;

  execute(
    connection: unknown,
    query: string,
    params?: unknown[],
  ): QueryResult | Promise<QueryResult>;

  begin(connection: unknown): void | Promise<void>;
  commit(connection: unknown): void | Promise<void>;
  rollback(connection: unknown): void | Promise<void>;

  ping?(connection: unknown): boolean | Promise<boolean>;
}
