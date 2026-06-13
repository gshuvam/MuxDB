import { describe, it, expect } from "vitest";
import {
  MuxDB,
  MuxConfig,
  createMuxPrisma,
  createMuxDrizzle,
  createMuxKnex,
  ShardKey,
  getShardKeyProperty,
  MuxSequelize,
  MuxMikroORM,
  MuxMongooseConnection,
  MuxIORedis,
  MuxKyselyDialect,
} from "../src/index.js";
import { extractRoutingKey } from "../src/integrations/ioredis.js";

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

  it("should initialize Sequelize sharded database successfully", () => {
    const db = new MuxDB(config);
    const s = new MuxSequelize(db, {});
    const model = s.define("User", { name: "string" });
    expect(model).toBeDefined();
  });

  it("should initialize MikroORM sharded em successfully", () => {
    const db = new MuxDB(config);
    const o = new MuxMikroORM(db, {});
    expect(o.em).toBeDefined();
  });

  it("should initialize Mongoose connection successfully", () => {
    const db = new MuxDB(config);
    const conn = new MuxMongooseConnection(db);
    expect(conn.model).toBeDefined();
  });

  it("should resolve ioredis hash tag routing keys", () => {
    expect(extractRoutingKey("key123")).toBe("key123");
    expect(extractRoutingKey("{user_123}:profile")).toBe("user_123");
  });

  it("should create Kysely dialect successfully", () => {
    const db = new MuxDB(config);
    const dialect = new MuxKyselyDialect(db);
    const driver = dialect.createDriver();
    expect(driver).toBeDefined();
  });
});
