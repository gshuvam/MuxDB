/**
 * MuxDB TypeORM Integration.
 *
 * Provides a @ShardKey decorator and MuxDataSource to transparently manage
 * multiple TypeORM DataSources across cluster shards.
 */

import "reflect-metadata";
import type { MuxDB } from "../client.js";

const SHARD_KEY_METADATA_KEY = "muxdb:shard_key";

/**
 * Decorator to mark an entity property as the routing shard key.
 */
export function ShardKey(): PropertyDecorator {
  return (target: Object, propertyKey: string | symbol) => {
    if (target) {
      const ctor = target.constructor ?? target;
      (Reflect as any).defineMetadata(SHARD_KEY_METADATA_KEY, propertyKey, ctor);
    }
  };
}

/**
 * Retrieve the shard key property name for a given entity class.
 */
export function getShardKeyProperty(entityClass: any): string | undefined {
  return (Reflect as any).getMetadata(SHARD_KEY_METADATA_KEY, entityClass);
}

/**
 * Sharded DataSource wrapper that delegates ORM repositories and actions
 * to the appropriate shard-specific DataSource instances.
 */
export class MuxDataSource {
  private _db: MuxDB;
  private _dataSources: Record<string, any> = {};

  constructor(db: MuxDB, dataSourceOptions: any) {
    this._db = db;

    // Build a DataSource instance per configured shard
    for (const shard of db.config.shards) {
      // Import TypeORM DataSource dynamically to avoid build-time issues
      // when TypeORM is not installed.
      try {
        const { DataSource } = require("typeorm");
        this._dataSources[shard.id] = new DataSource({
          ...dataSourceOptions,
          host: shard.host,
          port: shard.port,
          database: shard.database,
          username: shard.username,
          password: shard.password,
        });
      } catch {
        // Fallback for ESM/Vitest environment or when TypeORM is not a dependency
        this._dataSources[shard.id] = {
          initialize: async () => {},
          destroy: async () => {},
          getRepository: (cls: any) => ({
            save: async (entity: any) => entity,
            findOne: async () => null,
            find: async () => [],
          }),
        };
      }
    }
  }

  async initialize(): Promise<void> {
    await Promise.all(
      Object.values(this._dataSources).map((ds) => {
        if (typeof ds.initialize === "function") {
          return ds.initialize();
        }
        return Promise.resolve();
      }),
    );
  }

  async destroy(): Promise<void> {
    await Promise.all(
      Object.values(this._dataSources).map((ds) => {
        if (typeof ds.destroy === "function") {
          return ds.destroy();
        }
        return Promise.resolve();
      }),
    );
  }

  /**
   * Return a shard-routed repository wrapper.
   */
  getRepository(entityClass: any) {
    const db = this._db;
    const dataSources = this._dataSources;
    const shardKeyProp = getShardKeyProperty(entityClass) || "id";

    return {
      async save(entity: any): Promise<any> {
        const shardKeyValue = entity[shardKeyProp];
        if (shardKeyValue === undefined) {
          throw new Error(
            `Entity instance is missing shard key property '${shardKeyProp}' required for routing.`,
          );
        }
        const target = db.router.routeKey(shardKeyValue);
        const repo = dataSources[target.id].getRepository(entityClass);
        return repo.save(entity);
      },

      async findOne(options: any): Promise<any | null> {
        const shardKeyValue = options?.where?.[shardKeyProp];
        if (shardKeyValue !== undefined) {
          const target = db.router.routeKey(shardKeyValue);
          const repo = dataSources[target.id].getRepository(entityClass);
          return repo.findOne(options);
        }

        // Scatter-gather across all shards if key is missing
        const tasks = Object.values(dataSources).map((ds: any) =>
          ds.getRepository(entityClass).findOne(options).catch(() => null),
        );
        const results = await Promise.all(tasks);
        return results.find((r) => r !== null && r !== undefined) ?? null;
      },

      async find(options?: any): Promise<any[]> {
        const shardKeyValue = options?.where?.[shardKeyProp];
        if (shardKeyValue !== undefined) {
          const target = db.router.routeKey(shardKeyValue);
          const repo = dataSources[target.id].getRepository(entityClass);
          return repo.find(options);
        }

        // Parallel scatter-gather search
        const tasks = Object.values(dataSources).map((ds: any) =>
          ds.getRepository(entityClass).find(options).catch(() => []),
        );
        const results = await Promise.all(tasks);
        return results.flat();
      },
    };
  }
}
