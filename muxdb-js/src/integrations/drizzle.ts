/**
 * MuxDB Drizzle Integration.
 *
 * Exposes a pool/client wrapper compatible with Drizzle ORM's node-postgres driver,
 * enabling transparent multi-shard database query routing.
 */

import type { MuxDB } from "../client.js";

/**
 * Creates a mock database adapter that satisfies Drizzle ORM's client requirement.
 * Routes all Drizzle queries through MuxDB's router.
 */
export function createMuxDrizzle(db: MuxDB) {
  return {
    // Satisfy the node-postgres/pg driver query interface
    async query(queryText: string, values?: any[]) {
      const result = await db.execute(queryText, values ?? []);
      return {
        rows: result.rows,
        rowCount: result.rowCount,
        fields: result.columns.map((name) => ({ name })),
      };
    },

    // Satisfy connect interface
    async connect() {
      return {
        query: async (queryText: string, values?: any[]) => {
          const result = await db.execute(queryText, values ?? []);
          return {
            rows: result.rows,
            rowCount: result.rowCount,
            fields: result.columns.map((name) => ({ name })),
          };
        },
        release: () => {},
      };
    },
  };
}
