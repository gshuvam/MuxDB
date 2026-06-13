/**
 * MuxDB configuration loader.
 *
 * Loads cluster topology, shard definitions, pool settings, and operational
 * thresholds from YAML or JSON files. Supports environment variable
 * interpolation for secrets (e.g. `${DB_PASSWORD}`).
 */

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import yaml from "js-yaml";
import { ConfigError } from "./errors.js";

// ---------------------------------------------------------------------------
// Env var interpolation
// ---------------------------------------------------------------------------

const ENV_VAR_PATTERN = /\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g;

function interpolateEnv(value: unknown): unknown {
  if (typeof value === "string") {
    return value.replace(ENV_VAR_PATTERN, (_, varName: string) => {
      const envVal = process.env[varName];
      if (envVal === undefined) {
        throw new ConfigError(
          `Environment variable '${varName}' is not set (referenced in config as '\${${varName}}')`,
        );
      }
      return envVal;
    });
  }
  if (Array.isArray(value)) {
    return value.map(interpolateEnv);
  }
  if (value !== null && typeof value === "object") {
    const result: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) {
      result[k] = interpolateEnv(v);
    }
    return result;
  }
  return value;
}

// ---------------------------------------------------------------------------
// Config interfaces
// ---------------------------------------------------------------------------

export interface ShardRange {
  readonly start: number;
  readonly end: number;
}

export interface ShardConfig {
  readonly id: string;
  readonly backend: string;
  readonly host: string;
  readonly port: number;
  readonly database: string;
  readonly weight: number;
  readonly range?: ShardRange;
  readonly tags: Record<string, string>;
  readonly username?: string;
  readonly password?: string;
}

export interface ClusterConfig {
  readonly name: string;
  readonly strategy: "hash" | "range" | "consistent_hash";
  readonly shardKey: string;
  readonly virtualNodes: number;
}

export interface PoolConfig {
  readonly minSize: number;
  readonly maxSize: number;
  readonly slowStartMs: number;
  readonly idleTimeoutS: number;
  readonly healthCheckIntervalS: number;
}

export interface RoutingConfig {
  readonly scatterGather: boolean;
  readonly readPreference: "primary" | "replica" | "nearest";
  readonly retryOnShardError: boolean;
}

export interface BalancerConfig {
  readonly enabled: boolean;
  readonly checkIntervalS: number;
  readonly suppressionThreshold: number;
  readonly qpsSplitThreshold: number;
  readonly balanceFactorMin: number;
  readonly splitCrossingPenaltyMax: number;
}

export interface TelemetryExportConfig {
  readonly type: "prometheus" | "log" | "memory";
  readonly endpoint?: string;
}

export interface TelemetryConfig {
  readonly enabled: boolean;
  readonly windowSizeS: number;
  readonly hotKeyThresholdPct: number;
  readonly export: TelemetryExportConfig;
}

export interface SecurityTLSConfig {
  readonly enabled: boolean;
  readonly certFile?: string;
  readonly keyFile?: string;
  readonly caFile?: string;
}

export interface SecurityAuthConfig {
  readonly type: "none" | "token" | "jwt";
  readonly token?: string;
}

export interface SecurityConfig {
  readonly tls: SecurityTLSConfig;
  readonly auth: SecurityAuthConfig;
}

// ---------------------------------------------------------------------------
// MuxConfig
// ---------------------------------------------------------------------------

export interface MuxConfigOptions {
  cluster: ClusterConfig;
  shards: ShardConfig[];
  pool?: Partial<PoolConfig>;
  routing?: Partial<RoutingConfig>;
  balancer?: Partial<BalancerConfig>;
  telemetry?: {
    enabled?: boolean;
    windowSizeS?: number;
    hotKeyThresholdPct?: number;
    export?: Partial<TelemetryExportConfig>;
  };
  security?: {
    tls?: Partial<SecurityTLSConfig>;
    auth?: Partial<SecurityAuthConfig>;
  };
}

export class MuxConfig {
  readonly cluster: ClusterConfig;
  readonly shards: readonly ShardConfig[];
  readonly pool: PoolConfig;
  readonly routing: RoutingConfig;
  readonly balancer: BalancerConfig;
  readonly telemetry: TelemetryConfig;
  readonly security: SecurityConfig;

  constructor(opts: MuxConfigOptions) {
    this.cluster = opts.cluster;
    this.shards = Object.freeze([...opts.shards]);

    this.pool = {
      minSize: opts.pool?.minSize ?? 2,
      maxSize: opts.pool?.maxSize ?? 20,
      slowStartMs: opts.pool?.slowStartMs ?? 10,
      idleTimeoutS: opts.pool?.idleTimeoutS ?? 300,
      healthCheckIntervalS: opts.pool?.healthCheckIntervalS ?? 30,
    };

    this.routing = {
      scatterGather: opts.routing?.scatterGather ?? true,
      readPreference: opts.routing?.readPreference ?? "primary",
      retryOnShardError: opts.routing?.retryOnShardError ?? true,
    };

    this.balancer = {
      enabled: opts.balancer?.enabled ?? true,
      checkIntervalS: opts.balancer?.checkIntervalS ?? 60,
      suppressionThreshold: opts.balancer?.suppressionThreshold ?? 0.25,
      qpsSplitThreshold: opts.balancer?.qpsSplitThreshold ?? 5000,
      balanceFactorMin: opts.balancer?.balanceFactorMin ?? 0.3,
      splitCrossingPenaltyMax: opts.balancer?.splitCrossingPenaltyMax ?? 0.1,
    };

    this.telemetry = {
      enabled: opts.telemetry?.enabled ?? true,
      windowSizeS: opts.telemetry?.windowSizeS ?? 60,
      hotKeyThresholdPct: opts.telemetry?.hotKeyThresholdPct ?? 1.0,
      export: {
        type: opts.telemetry?.export?.type ?? "memory",
        endpoint: opts.telemetry?.export?.endpoint,
      },
    };

    this.security = {
      tls: {
        enabled: opts.security?.tls?.enabled ?? false,
        certFile: opts.security?.tls?.certFile,
        keyFile: opts.security?.tls?.keyFile,
        caFile: opts.security?.tls?.caFile,
      },
      auth: {
        type: opts.security?.auth?.type ?? "none",
        token: opts.security?.auth?.token,
      },
    };

    // Validation
    this._validate();
  }

  getShard(shardId: string): ShardConfig {
    const shard = this.shards.find((s) => s.id === shardId);
    if (!shard) {
      throw new ConfigError(`Shard '${shardId}' not found in configuration`);
    }
    return shard;
  }

  // --- Factory methods ---

  static fromFile(path: string): MuxConfig {
    const filepath = resolve(path);
    let raw: string;

    try {
      raw = readFileSync(filepath, "utf-8");
    } catch (err) {
      throw new ConfigError(`Configuration file not found: ${filepath}`);
    }

    let data: unknown;
    try {
      data = yaml.load(raw);
    } catch (err) {
      throw new ConfigError(`Failed to parse config file: ${err}`);
    }

    if (typeof data !== "object" || data === null) {
      throw new ConfigError("Config file must contain a YAML mapping (object) at the top level");
    }

    const interpolated = interpolateEnv(data) as Record<string, unknown>;
    return MuxConfig.fromObject(interpolated);
  }

  static fromObject(data: Record<string, unknown>): MuxConfig {
    const clusterData = data["cluster"] as Record<string, unknown> | undefined;
    if (!clusterData) {
      throw new ConfigError("'cluster' section is required in configuration");
    }

    const validStrategies = new Set(["hash", "range", "consistent_hash"]);
    const strategy = clusterData["strategy"] as string;
    if (!validStrategies.has(strategy)) {
      throw new ConfigError(
        `Invalid cluster strategy '${strategy}'. Must be one of: hash, range, consistent_hash`,
      );
    }

    const cluster: ClusterConfig = {
      name: clusterData["name"] as string,
      strategy: strategy as ClusterConfig["strategy"],
      shardKey: (clusterData["shard_key"] as string) ?? "id",
      virtualNodes: (clusterData["virtual_nodes"] as number) ?? 256,
    };

    // Global credentials
    const globalCreds = (data["credentials"] ?? {}) as Record<string, unknown>;
    const globalUsername = globalCreds["username"] as string | undefined;
    const globalPassword = globalCreds["password"] as string | undefined;

    const shardsData = (data["shards"] ?? []) as Record<string, unknown>[];
    if (!shardsData.length) {
      throw new ConfigError("'shards' section must contain at least one shard");
    }

    const shards: ShardConfig[] = shardsData.map((s) => {
      const rangeData = s["range"] as Record<string, number> | undefined;
      return {
        id: s["id"] as string,
        backend: s["backend"] as string,
        host: s["host"] as string,
        port: s["port"] as number,
        database: s["database"] as string,
        weight: (s["weight"] as number) ?? 1,
        range: rangeData ? { start: rangeData["start"]!, end: rangeData["end"]! } : undefined,
        tags: (s["tags"] ?? {}) as Record<string, string>,
        username: (s["username"] as string) ?? globalUsername,
        password: (s["password"] as string) ?? globalPassword,
      };
    });

    const poolData = (data["pool"] ?? {}) as Record<string, unknown>;
    const routingData = (data["routing"] ?? {}) as Record<string, unknown>;
    const balancerData = (data["balancer"] ?? {}) as Record<string, unknown>;
    const telData = (data["telemetry"] ?? {}) as Record<string, unknown>;
    const secData = (data["security"] ?? {}) as Record<string, unknown>;

    return new MuxConfig({
      cluster,
      shards,
      pool: {
        minSize: poolData["min_size"] as number | undefined,
        maxSize: poolData["max_size"] as number | undefined,
        slowStartMs: poolData["slow_start_ms"] as number | undefined,
        idleTimeoutS: poolData["idle_timeout_s"] as number | undefined,
        healthCheckIntervalS: poolData["health_check_interval_s"] as number | undefined,
      },
      routing: {
        scatterGather: routingData["scatter_gather"] as boolean | undefined,
        readPreference: routingData["read_preference"] as RoutingConfig["readPreference"],
        retryOnShardError: routingData["retry_on_shard_error"] as boolean | undefined,
      },
      balancer: {
        enabled: balancerData["enabled"] as boolean | undefined,
        checkIntervalS: balancerData["check_interval_s"] as number | undefined,
        suppressionThreshold: balancerData["suppression_threshold"] as number | undefined,
        qpsSplitThreshold: balancerData["qps_split_threshold"] as number | undefined,
        balanceFactorMin: balancerData["balance_factor_min"] as number | undefined,
        splitCrossingPenaltyMax: balancerData["split_crossing_penalty_max"] as number | undefined,
      },
      telemetry: {
        enabled: telData["enabled"] as boolean | undefined,
        windowSizeS: telData["window_size_s"] as number | undefined,
        hotKeyThresholdPct: telData["hot_key_threshold_pct"] as number | undefined,
        export: telData["export"] as Partial<TelemetryExportConfig> | undefined,
      },
      security: {
        tls: (secData["tls"] ?? {}) as Partial<SecurityTLSConfig>,
        auth: (secData["auth"] ?? {}) as Partial<SecurityAuthConfig>,
      },
    });
  }

  // --- Validation ---

  private _validate(): void {
    if (!this.shards.length) {
      throw new ConfigError("At least one shard must be defined in 'shards'");
    }

    const ids = this.shards.map((s) => s.id);
    const uniqueIds = new Set(ids);
    if (ids.length !== uniqueIds.size) {
      const dupes = ids.filter((id, i) => ids.indexOf(id) !== i);
      throw new ConfigError(`Duplicate shard IDs found: ${[...new Set(dupes)].join(", ")}`);
    }

    if (this.pool.minSize < 0) {
      throw new ConfigError(`pool.minSize must be >= 0, got ${this.pool.minSize}`);
    }
    if (this.pool.maxSize < this.pool.minSize) {
      throw new ConfigError(
        `pool.maxSize (${this.pool.maxSize}) must be >= pool.minSize (${this.pool.minSize})`,
      );
    }

    if (this.cluster.strategy === "range") {
      for (const shard of this.shards) {
        if (!shard.range) {
          throw new ConfigError(
            `Shard '${shard.id}' must define a 'range' when cluster strategy is 'range'`,
          );
        }
      }
    }
  }
}
