/**
 * MuxDB error hierarchy.
 *
 * All MuxDB exceptions extend MuxDBError, allowing callers to catch
 * the base class for broad handling or specific subclasses for targeted recovery.
 */

export class MuxDBError extends Error {
  public readonly details: Record<string, unknown>;

  constructor(message: string, details: Record<string, unknown> = {}) {
    super(message);
    this.name = "MuxDBError";
    this.details = details;
  }
}

// --- Configuration ---

export class ConfigError extends MuxDBError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super(message, details);
    this.name = "ConfigError";
  }
}

// --- Routing ---

export class RoutingError extends MuxDBError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super(message, details);
    this.name = "RoutingError";
  }
}

export class ShardNotFoundError extends RoutingError {
  public readonly key: string | number;

  constructor(key: string | number, details: Record<string, unknown> = {}) {
    super(`No shard found for routing key: ${JSON.stringify(key)}`, details);
    this.name = "ShardNotFoundError";
    this.key = key;
  }
}

export class CrossShardTransactionError extends RoutingError {
  public readonly shards: string[];

  constructor(shards: string[], details: Record<string, unknown> = {}) {
    super(
      `Transaction cannot span multiple shards: [${shards.join(", ")}]. ` +
        "Use db.execute() outside a transaction for cross-shard queries.",
      details,
    );
    this.name = "CrossShardTransactionError";
    this.shards = shards;
  }
}

// --- Driver & Connection ---

export class DriverError extends MuxDBError {
  public readonly shardId?: string;
  public readonly backend?: string;

  constructor(
    message: string,
    opts: { shardId?: string; backend?: string; details?: Record<string, unknown> } = {},
  ) {
    super(message, {
      ...(opts.shardId ? { shard_id: opts.shardId } : {}),
      ...(opts.backend ? { backend: opts.backend } : {}),
      ...(opts.details ?? {}),
    });
    this.name = "DriverError";
    this.shardId = opts.shardId;
    this.backend = opts.backend;
  }
}

export class ConnectionError extends DriverError {
  constructor(
    message: string,
    opts: { shardId?: string; backend?: string; details?: Record<string, unknown> } = {},
  ) {
    super(message, opts);
    this.name = "ConnectionError";
  }
}

// --- Pool ---

export class PoolExhaustedError extends MuxDBError {
  public readonly shardId: string;
  public readonly maxSize: number;

  constructor(shardId: string, maxSize: number, details: Record<string, unknown> = {}) {
    super(
      `Connection pool exhausted for shard '${shardId}' (max_size=${maxSize}). ` +
        "Consider increasing pool.maxSize or reducing query concurrency.",
      details,
    );
    this.name = "PoolExhaustedError";
    this.shardId = shardId;
    this.maxSize = maxSize;
  }
}

// --- Resilience ---

export class CircuitOpenError extends MuxDBError {
  public readonly shardId: string;
  public readonly recoveryAfterS?: number;

  constructor(
    shardId: string,
    opts: { recoveryAfterS?: number; details?: Record<string, unknown> } = {},
  ) {
    let msg = `Circuit breaker OPEN for shard '${shardId}'.`;
    if (opts.recoveryAfterS !== undefined) {
      msg += ` Recovery attempt in ${opts.recoveryAfterS.toFixed(1)}s.`;
    }
    super(msg, opts.details ?? {});
    this.name = "CircuitOpenError";
    this.shardId = shardId;
    this.recoveryAfterS = opts.recoveryAfterS;
  }
}

// --- Migration ---

export class MigrationError extends MuxDBError {
  public readonly sourceShard?: string;
  public readonly destShard?: string;

  constructor(
    message: string,
    opts: { sourceShard?: string; destShard?: string; details?: Record<string, unknown> } = {},
  ) {
    super(message, {
      ...(opts.sourceShard ? { source_shard: opts.sourceShard } : {}),
      ...(opts.destShard ? { dest_shard: opts.destShard } : {}),
      ...(opts.details ?? {}),
    });
    this.name = "MigrationError";
    this.sourceShard = opts.sourceShard;
    this.destShard = opts.destShard;
  }
}

// --- Security ---

export class SecurityError extends MuxDBError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super(message, details);
    this.name = "SecurityError";
  }
}

export class BulkheadLimitExceeded extends MuxDBError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super(message, details);
    this.name = "BulkheadLimitExceeded";
  }
}


