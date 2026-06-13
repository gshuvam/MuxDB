/**
 * MuxDB Knex Integration.
 *
 * Configures Knex with a mock connection pool and overrides its client query execution,
 * allowing Knex query builders and raw SQL executions to be routed by MuxDB.
 */

import type { MuxDB } from "../client.js";

/**
 * Creates a Knex instance configured to route all SQL executions through MuxDB.
 *
 * @param db MuxDB client instance.
 * @param knexInstance An un-initialized Knex creator instance (i.e. require("knex")).
 * @returns A sharded Knex instance.
 */
export function createMuxKnex(db: MuxDB, knex: any) {
  // Configure Knex with a mock connection pool that returns dummy connection objects
  const k = knex({
    client: "pg",
    connection: {}, // Dummy connection parameters
    pool: {
      min: 0,
      max: 1,
      create: () => Promise.resolve({ _is_mock_connection: true }),
      destroy: () => Promise.resolve(),
    },
  });

  // Override the driver's internal query runner to intercept all executions
  const client = k.client;
  if (client && typeof client === "object") {
    client.query = async (connection: any, obj: any) => {
      // obj contains { sql: string, bindings: any[] }
      const sql = obj.sql;
      const bindings = obj.bindings ?? [];

      const result = await db.execute(sql, bindings);

      // Return the result shape expected by Knex's node-postgres client adapter
      return {
        rows: result.rows,
        rowCount: result.rowCount,
        command: sql.trim().split(" ")[0]?.toUpperCase() ?? "SELECT",
        fields: result.columns.map((name) => ({ name })),
      };
    };
  }

  return k;
}
