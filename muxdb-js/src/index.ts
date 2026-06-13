/**
 * MuxDB — Autonomous Data Orchestration SDK
 *
 * @packageDocumentation
 */

// Core
export { MuxDB, MuxTransaction } from "./client.js";
export { MuxConfig } from "./config.js";
export type {
  ShardConfig,
  ClusterConfig,
  PoolConfig,
  RoutingConfig,
  BalancerConfig,
  TelemetryConfig,
  TelemetryExportConfig,
  SecurityConfig,
  SecurityTLSConfig,
  SecurityAuthConfig,
  ShardRange,
} from "./config.js";

// Shard Map
export { ShardMap } from "./shard-map.js";
export type { ShardInfo } from "./shard-map.js";

// Router
export { Router, RouteType } from "./router.js";
export type { RouteDecision, QueryContext } from "./router.js";

// Pool
export { AdaptivePool, ShardPool } from "./pool.js";
export type { PoolStats, ConnectionWrapper } from "./pool.js";

// Drivers
export type { Driver, QueryResult } from "./drivers/base.js";
export { PostgreSQLDriver } from "./drivers/postgresql.js";

// Errors
export {
  MuxDBError,
  ConfigError,
  RoutingError,
  ShardNotFoundError,
  CrossShardTransactionError,
  DriverError,
  ConnectionError,
  PoolExhaustedError,
  CircuitOpenError,
  MigrationError,
  SecurityError,
  BulkheadLimitExceeded,
} from "./errors.js";

// Integrations
export { createMuxPrisma } from "./integrations/prisma.js";
export { createMuxDrizzle } from "./integrations/drizzle.js";
export { MuxDataSource, ShardKey, getShardKeyProperty } from "./integrations/typeorm.js";
export { createMuxKnex } from "./integrations/knex.js";
export { MuxSequelize } from "./integrations/sequelize.js";
export { MuxMikroORM } from "./integrations/mikro-orm.js";
export { MuxConnection as MuxMongooseConnection } from "./integrations/mongoose.js";
export { MuxIORedis } from "./integrations/ioredis.js";
export { MuxDialect as MuxKyselyDialect } from "./integrations/kysely.js";
export { MuxKafkaProducer, MuxKafkaConsumer } from "./integrations/kafka.js";

// Core Phase 5 Engines
export { TelemetryCollector } from "./telemetry.js";
export { PlacementEngine } from "./placement.js";
export { Balancer } from "./balancer.js";
export type { BalancingAction } from "./balancer.js";

// Core Phase 6 Engines
export { LiveMigrator, PIDController } from "./migrator.js";
export * from "./control-plane/index.js";

// ML
export * from "./ml/index.js";

// Enterprise Infrastructure
export * from "./observability/index.js";
export * from "./security/index.js";
export * from "./resilience/index.js";
export * from "./multitenancy/index.js";


