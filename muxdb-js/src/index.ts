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
} from "./errors.js";

// Integrations
export { createMuxPrisma } from "./integrations/prisma.js";
export { createMuxDrizzle } from "./integrations/drizzle.js";
export { MuxDataSource, ShardKey } from "./integrations/typeorm.js";
export { createMuxKnex } from "./integrations/knex.js";
