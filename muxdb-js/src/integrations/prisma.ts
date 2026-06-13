/**
 * MuxDB Prisma Integration.
 *
 * Uses Prisma Client Extensions ($extends) to intercept operations,
 * extract shard keys, and route queries to shard-specific clients.
 */

import type { MuxDB } from "../client.js";

export interface MuxPrismaOptions {
  db: MuxDB;
  clients: Record<string, any>; // shardId -> PrismaClient
}

/**
 * Creates a Prisma extension that routes queries dynamically.
 */
export function createMuxPrisma(opts: MuxPrismaOptions) {
  const { db, clients } = opts;
  const shardKey = db.config.cluster.shardKey;

  // We return a function that extends an existing PrismaClient
  return (prismaClient: any) => {
    return prismaClient.$extends({
      query: {
        $allModels: {
          async $allOperations({ model, operation, args, query }: any) {
            // 1. Try to extract shard key value from args
            const where = args?.where;
            const data = args?.data;
            let shardKeyValue: any = undefined;

            if (where && where[shardKey] !== undefined) {
              shardKeyValue = where[shardKey];
            } else if (data && data[shardKey] !== undefined) {
              shardKeyValue = data[shardKey];
            }

            // Route to single shard if key is resolved
            if (shardKeyValue !== undefined) {
              const target = db.router.routeKey(shardKeyValue);
              const shardClient = clients[target.id];
              if (!shardClient) {
                throw new Error(`Prisma client for shard '${target.id}' is not configured.`);
              }
              // Execute query using the target shard client
              return shardClient[model][operation](args);
            }

            // 2. Fallback: Scatter-gather reads
            if (operation === "findMany" || operation === "findFirst" || operation === "count") {
              const tasks = Object.keys(clients).map((shardId) => {
                const client = clients[shardId]!;
                return client[model][operation](args).catch(() => []);
              });

              const results = await Promise.all(tasks);

              if (operation === "count") {
                return (results as number[]).reduce((sum, count) => sum + count, 0);
              }
              if (operation === "findFirst") {
                return results.find((r) => r !== null && r !== undefined) ?? null;
              }

              return results.flat();
            }

            // 3. Fallback: DDL/Broadcast or fallback query execution
            return query(args);
          },
        },
      },
    });
  };
}
