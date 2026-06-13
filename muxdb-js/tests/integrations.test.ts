import { describe, it, expect } from "vitest";
import { MuxDB } from "../src/client.js";
import { MuxConfig } from "../src/config.js";
import { createMuxPrisma } from "../src/integrations/prisma.js";
import { createMuxDrizzle } from "../src/integrations/drizzle.js";
import { createMuxKnex } from "../src/integrations/knex.js";
import { ShardKey, getShardKeyProperty } from "../src/integrations/typeorm.js";

describe("Node.js Integrations Smoke Tests", () => {
  const config = new MuxConfig({
    cluster: {
      name: "test",
      strategy: "consistent_hash",
      shardKey: "userId",
      virtualNodes: 256,
    },
    shards: [
      {
        id: "s0",
        backend: "postgresql",
        host: "localhost",
        port: 5432,
        database: "db0",
        weight: 1,
        tags: {},
      },
      {
        id: "s1",
        backend: "postgresql",
        host: "localhost",
        port: 5433,
        database: "db1",
        weight: 1,
        tags: {},
      },
    ],
  });

  it("should initialize prisma extension successfully", () => {
    const db = new MuxDB(config);
    const mockPrisma = {
      $extends: (ext: any) => ext,
    };
    const extension = createMuxPrisma({ db, clients: { s0: {}, s1: {} } });
    const extended = extension(mockPrisma);
    expect(extended).toBeDefined();
  });

  it("should wrap drizzle pool adapter successfully", () => {
    const db = new MuxDB(config);
    const drizzleAdapter = createMuxDrizzle(db);
    expect(drizzleAdapter.query).toBeDefined();
    expect(drizzleAdapter.connect).toBeDefined();
  });

  it("should configure knex correctly with mock pool", () => {
    const db = new MuxDB(config);
    const mockKnexCreator = (opts: any) => {
      return {
        client: {
          query: () => {},
        },
        ...opts,
      };
    };
    const knexConfig = createMuxKnex(db, mockKnexCreator);
    expect(knexConfig.client).toBeDefined();
    expect(knexConfig.pool).toBeDefined();
  });

  it("should retrieve ShardKey property name using reflect-metadata", () => {
    class User {
      @ShardKey()
      userId!: number;
    }
    const prop = getShardKeyProperty(User);
    expect(prop).toBe("userId");
  });
});
