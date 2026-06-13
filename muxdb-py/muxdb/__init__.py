"""
MuxDB — Autonomous Data Orchestration SDK

A self-optimizing distributed database orchestration layer that provides
transparent shard routing, workload-aware placement, autonomous load
balancing, and non-disruptive live migration.

Usage:
    from muxdb import MuxDB, MuxConfig

    config = MuxConfig.from_file("muxdb.yaml")
    db = MuxDB(config)
    await db.connect()

    result = await db.execute("SELECT * FROM orders WHERE user_id = %s", [42])
    await db.close()
"""

from muxdb.client import MuxDB
from muxdb.config import MuxConfig, PoolConfig, RoutingConfig, BalancerConfig, TelemetryConfig
from muxdb.errors import (
    MuxDBError,
    ConfigError,
    ShardNotFoundError,
    RoutingError,
    CrossShardTransactionError,
    CircuitOpenError,
    MigrationError,
    DriverError,
    ConnectionError,
    PoolExhaustedError,
    SecurityError,
    BulkheadLimitExceeded,
)
from muxdb.shard_map import ShardMap, ShardInfo
from muxdb.router import Router

__version__ = "0.1.0"

__all__ = [
    # Core
    "MuxDB",
    "MuxConfig",
    "ShardMap",
    "ShardInfo",
    "Router",
    # Config
    "PoolConfig",
    "RoutingConfig",
    "BalancerConfig",
    "TelemetryConfig",
    # Errors
    "MuxDBError",
    "ConfigError",
    "ShardNotFoundError",
    "RoutingError",
    "CrossShardTransactionError",
    "CircuitOpenError",
    "MigrationError",
    "DriverError",
    "ConnectionError",
    "PoolExhaustedError",
    "SecurityError",
    "BulkheadLimitExceeded",
    # Version
    "__version__",
]

