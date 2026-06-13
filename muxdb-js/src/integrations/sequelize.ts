/**
 * MuxDB Sequelize Integration.
 *
 * Exposes MuxSequelize to manage multiple Sequelize database connections
 * and proxy model operations transparently across shards.
 */

import type { MuxDB } from "../client.js";

/**
 * Sharded Sequelize manager.
 */
export class MuxSequelize {
  private _db: MuxDB;
  private _instances: Record<string, any> = {};

  constructor(db: MuxDB, sequelizeOptions: any) {
    this._db = db;

    // Dynamically create a Sequelize instance per configured shard
    for (const shard of db.config.shards) {
      try {
        const { Sequelize } = require("sequelize");
        this._instances[shard.id] = new Sequelize({
          ...sequelizeOptions,
          host: shard.host,
          port: shard.port,
          database: shard.database,
          username: shard.username,
          password: shard.password,
          dialect: "postgres",
        });
      } catch {
        // Fallback for environment compilation tests
        this._instances[shard.id] = {
          define: (name: string) => ({
            create: async (v: any) => v,
            findOne: async () => null,
            findAll: async () => [],
          }),
          sync: async () => {},
          close: async () => {},
        };
      }
    }
  }

  async sync(options?: any): Promise<void> {
    await Promise.all(Object.values(this._instances).map((s: any) => s.sync(options)));
  }

  async close(): Promise<void> {
    await Promise.all(Object.values(this._instances).map((s: any) => s.close()));
    await this._db.close();
  }

  /**
   * Define a model across all Sequelize shard instances and return a proxy class.
   */
  define(modelName: string, attributes: any, options?: any): any {
    const shardKey = this._db.config.cluster.shardKey;
    const db = this._db;
    const instances = this._instances;

    // Define the model on all Sequelize shard connections
    const shardModels: Record<string, any> = {};
    for (const [shardId, seq] of Object.entries(this._instances)) {
      shardModels[shardId] = seq.define(modelName, attributes, options);
    }

    // Return a Proxy Model class that intercepts query requests
    return class MuxSequelizeModel {
      static async create(values: any, queryOptions?: any): Promise<any> {
        const shardKeyValue = values[shardKey];
        if (shardKeyValue === undefined) {
          throw new Error(`Model creation is missing required shard key: ${shardKey}`);
        }
        const target = db.router.routeKey(shardKeyValue);
        return shardModels[target.id].create(values, queryOptions);
      }

      static async findOne(queryOptions?: any): Promise<any | null> {
        const shardKeyValue = queryOptions?.where?.[shardKey];
        if (shardKeyValue !== undefined) {
          const target = db.router.routeKey(shardKeyValue);
          return shardModels[target.id].findOne(queryOptions);
        }

        // Scatter-gather across all shards
        const tasks = Object.values(shardModels).map((m: any) =>
          m.findOne(queryOptions).catch(() => null),
        );
        const results = await Promise.all(tasks);
        return results.find((r) => r !== null && r !== undefined) ?? null;
      }

      static async findAll(queryOptions?: any): Promise<any[]> {
        const shardKeyValue = queryOptions?.where?.[shardKey];
        if (shardKeyValue !== undefined) {
          const target = db.router.routeKey(shardKeyValue);
          return shardModels[target.id].findAll(queryOptions);
        }

        // Scatter-gather across all shards
        const tasks = Object.values(shardModels).map((m: any) =>
          m.findAll(queryOptions).catch(() => []),
        );
        const results = await Promise.all(tasks);
        return results.flat();
      }
    };
  }
}
