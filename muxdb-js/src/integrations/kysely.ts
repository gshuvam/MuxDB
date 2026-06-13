/**
 * MuxDB Kysely Integration.
 *
 * Provides a custom MuxDialect implementing Kysely's Dialect interface,
 * enabling type-safe shard query routing at the driver execution layer.
 */

import type { MuxDB } from "../client.js";

/**
 * Custom Kysely Dialect that routes queries using MuxDB.
 */
export class MuxDialect {
  private _db: MuxDB;

  constructor(db: MuxDB) {
    this._db = db;
  }

  createDriver(): any {
    return new MuxKyselyDriver(this._db);
  }

  createQueryCompiler(): any {
    try {
      const { PostgresQueryCompiler } = require("kysely");
      return new PostgresQueryCompiler();
    } catch {
      // Mock for compilation tests
      return { compileQuery: () => ({ sql: "", parameters: [] }) };
    }
  }

  createAdapter(): any {
    try {
      const { PostgresAdapter } = require("kysely");
      return new PostgresAdapter();
    } catch {
      // Mock for compilation tests
      return {};
    }
  }

  createIntrospector(db: any): any {
    try {
      const { PostgresIntrospector } = require("kysely");
      return new PostgresIntrospector(db);
    } catch {
      // Mock for compilation tests
      return {};
    }
  }
}

/**
 * Custom Kysely Driver that manages mock connections and intercepts query execution.
 */
class MuxKyselyDriver {
  private _db: MuxDB;

  constructor(db: MuxDB) {
    this._db = db;
  }

  async init(): Promise<void> {}

  async acquireConnection(): Promise<any> {
    return new MuxKyselyConnection(this._db);
  }

  async releaseConnection(): Promise<void> {}

  async destroy(): Promise<void> {
    await this._db.close();
  }
}

/**
 * Custom Kysely DatabaseConnection.
 *
 * Intercepts executeQuery to route raw SQL queries via MuxDB.
 */
class MuxKyselyConnection {
  private _db: MuxDB;

  constructor(db: MuxDB) {
    this._db = db;
  }

  async executeQuery(compiledQuery: any): Promise<any> {
    const { sql, parameters } = compiledQuery;
    const result = await this._db.execute(sql, parameters ?? []);

    return {
      rows: result.rows,
      // Kysely expects numAffectedRows as bigint
      numAffectedRows: BigInt(result.rowCount),
    };
  }

  async streamQuery(): Promise<never> {
    throw new Error("Streaming queries are not supported in MuxDB Kysely driver yet.");
  }
}
