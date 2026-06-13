/**
 * PostgreSQL driver using `pg` (node-postgres).
 *
 * Requires `pg` as a peer dependency: `npm install pg`
 */

import type { Driver, QueryResult } from "./base.js";
import type { ShardInfo } from "../shard-map.js";
import { DriverError, ConnectionError } from "../errors.js";

export class PostgreSQLDriver implements Driver {
  get backendName(): string {
    return "postgresql";
  }

  async connect(shard: ShardInfo): Promise<unknown> {
    try {
      const pg = await import("pg");
      const Client = pg.default?.Client ?? pg.Client;

      const client = new Client({
        host: shard.host,
        port: shard.port,
        database: shard.database,
      });

      await client.connect();
      (client as Record<string, unknown>)["_muxdb_shard_id"] = shard.id;
      return client;
    } catch (err) {
      if ((err as Error).message?.includes("Cannot find module")) {
        throw new DriverError("pg is not installed. Run: npm install pg", {
          backend: "postgresql",
          shardId: shard.id,
        });
      }
      throw new ConnectionError(
        `Failed to connect to PostgreSQL shard '${shard.id}' at ${shard.host}:${shard.port}/${shard.database}: ${err}`,
        { shardId: shard.id, backend: "postgresql" },
      );
    }
  }

  async close(connection: unknown): Promise<void> {
    try {
      await (connection as { end(): Promise<void> }).end();
    } catch {
      // ignore
    }
  }

  async execute(connection: unknown, query: string, params: unknown[] = []): Promise<QueryResult> {
    const t0 = performance.now();
    const shardId =
      ((connection as Record<string, unknown>)["_muxdb_shard_id"] as string) ?? "unknown";

    try {
      const client = connection as { query(q: string, p?: unknown[]): Promise<{ rows: Record<string, unknown>[]; rowCount: number | null; fields: Array<{ name: string }> }> };
      const result = await client.query(query, params.length ? params : undefined);

      const elapsedMs = performance.now() - t0;
      const columns = result.fields?.map((f) => f.name) ?? [];

      return {
        rows: result.rows,
        rowCount: result.rowCount ?? result.rows.length,
        columns,
        shardId,
        elapsedMs,
      };
    } catch (err) {
      throw new DriverError(`PostgreSQL query failed on shard '${shardId}': ${err}`, {
        shardId,
        backend: "postgresql",
      });
    }
  }

  async begin(connection: unknown): Promise<void> {
    const client = connection as { query(q: string): Promise<unknown> };
    await client.query("BEGIN");
  }

  async commit(connection: unknown): Promise<void> {
    const client = connection as { query(q: string): Promise<unknown> };
    await client.query("COMMIT");
  }

  async rollback(connection: unknown): Promise<void> {
    const client = connection as { query(q: string): Promise<unknown> };
    await client.query("ROLLBACK");
  }

  async ping(connection: unknown): Promise<boolean> {
    try {
      await this.execute(connection, "SELECT 1");
      return true;
    } catch {
      return false;
    }
  }
}
