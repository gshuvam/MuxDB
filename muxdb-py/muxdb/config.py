"""
MuxDB configuration loader.

Loads cluster topology, shard definitions, pool settings, and operational
thresholds from YAML or JSON files. Supports environment variable interpolation
for secrets (e.g. ``${DB_PASSWORD}``).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from muxdb.errors import ConfigError


_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _interpolate_env(value: Any) -> Any:
    """Recursively replace ``${VAR}`` placeholders with environment variable values."""
    if isinstance(value, str):
        def _replace(match: re.Match[str]) -> str:
            var_name = match.group(1)
            env_val = os.environ.get(var_name)
            if env_val is None:
                raise ConfigError(
                    f"Environment variable '{var_name}' is not set "
                    f"(referenced in config as '${{{var_name}}}')"
                )
            return env_val

        return _ENV_VAR_PATTERN.sub(_replace, value)
    if isinstance(value, dict):
        return {k: _interpolate_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate_env(item) for item in value]
    return value


# ---------------------------------------------------------------------------
# Sub-configs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ShardRange:
    """Defines the key range owned by a range-partitioned shard."""

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ConfigError(
                f"Shard range start ({self.start}) must be <= end ({self.end})"
            )

    def contains(self, key: int) -> bool:
        return self.start <= key <= self.end


@dataclass(frozen=True)
class ShardConfig:
    """Configuration for a single shard (backend node)."""

    id: str
    backend: str
    host: str
    port: int
    database: str
    weight: int = 1
    range: ShardRange | None = None
    tags: dict[str, str] = field(default_factory=dict)

    # Credentials (resolved from env vars or direct)
    username: str | None = None
    password: str | None = None

    @property
    def dsn(self) -> str:
        """Build a connection string for this shard."""
        user_part = ""
        if self.username:
            user_part = self.username
            if self.password:
                user_part += f":{self.password}"
            user_part += "@"
        return f"{self.backend}://{user_part}{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class ClusterConfig:
    """Top-level cluster identity and strategy."""

    name: str
    strategy: str  # "hash" | "range" | "consistent_hash"
    shard_key: str = "id"
    virtual_nodes: int = 256

    def __post_init__(self) -> None:
        valid = {"hash", "range", "consistent_hash"}
        if self.strategy not in valid:
            raise ConfigError(
                f"Invalid cluster strategy '{self.strategy}'. Must be one of: {valid}"
            )


@dataclass(frozen=True)
class PoolConfig:
    """Connection pool tuning."""

    min_size: int = 2
    max_size: int = 20
    slow_start_ms: int = 10
    idle_timeout_s: int = 300
    health_check_interval_s: int = 30

    def __post_init__(self) -> None:
        if self.min_size < 0:
            raise ConfigError(f"pool.min_size must be >= 0, got {self.min_size}")
        if self.max_size < self.min_size:
            raise ConfigError(
                f"pool.max_size ({self.max_size}) must be >= pool.min_size ({self.min_size})"
            )


@dataclass(frozen=True)
class RoutingConfig:
    """Query routing behavior."""

    scatter_gather: bool = True
    read_preference: str = "primary"
    retry_on_shard_error: bool = True

    def __post_init__(self) -> None:
        valid = {"primary", "replica", "nearest"}
        if self.read_preference not in valid:
            raise ConfigError(
                f"Invalid read_preference '{self.read_preference}'. Must be one of: {valid}"
            )


@dataclass(frozen=True)
class BalancerConfig:
    """Autonomous load balancer thresholds."""

    enabled: bool = True
    check_interval_s: int = 60
    suppression_threshold: float = 0.25
    qps_split_threshold: int = 5000
    balance_factor_min: float = 0.3
    split_crossing_penalty_max: float = 0.1


@dataclass(frozen=True)
class TelemetryExportConfig:
    """Telemetry export target."""

    type: str = "memory"  # "prometheus" | "log" | "memory"
    endpoint: str | None = None


@dataclass(frozen=True)
class TelemetryConfig:
    """Telemetry collection settings."""

    enabled: bool = True
    window_size_s: int = 60
    hot_key_threshold_pct: float = 1.0
    export: TelemetryExportConfig = field(default_factory=TelemetryExportConfig)


@dataclass(frozen=True)
class SecurityTLSConfig:
    """TLS / mTLS settings for inter-node communication."""

    enabled: bool = False
    cert_file: str | None = None
    key_file: str | None = None
    ca_file: str | None = None


@dataclass(frozen=True)
class SecurityAuthConfig:
    """Authentication settings."""

    type: str = "none"  # "none" | "token" | "jwt"
    token: str | None = None


@dataclass(frozen=True)
class SecurityConfig:
    """Security settings."""

    tls: SecurityTLSConfig = field(default_factory=SecurityTLSConfig)
    auth: SecurityAuthConfig = field(default_factory=SecurityAuthConfig)


# ---------------------------------------------------------------------------
# Main config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MuxConfig:
    """
    Complete MuxDB configuration.

    Typically loaded from a YAML or JSON file via :meth:`from_file`, but
    can also be constructed programmatically.

    Example::

        config = MuxConfig.from_file("muxdb.yaml")
        # or
        config = MuxConfig(
            cluster=ClusterConfig(name="dev", strategy="hash"),
            shards=[
                ShardConfig(id="s0", backend="postgresql",
                            host="localhost", port=5432, database="db0"),
            ],
        )
    """

    cluster: ClusterConfig
    shards: list[ShardConfig]
    pool: PoolConfig = field(default_factory=PoolConfig)
    routing: RoutingConfig = field(default_factory=RoutingConfig)
    balancer: BalancerConfig = field(default_factory=BalancerConfig)
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)

    def __post_init__(self) -> None:
        if not self.shards:
            raise ConfigError("At least one shard must be defined in 'shards'")

        shard_ids = [s.id for s in self.shards]
        if len(shard_ids) != len(set(shard_ids)):
            dupes = [sid for sid in shard_ids if shard_ids.count(sid) > 1]
            raise ConfigError(f"Duplicate shard IDs found: {set(dupes)}")

        # Validate range coverage for range strategy
        if self.cluster.strategy == "range":
            for shard in self.shards:
                if shard.range is None:
                    raise ConfigError(
                        f"Shard '{shard.id}' must define a 'range' when "
                        f"cluster strategy is 'range'"
                    )

    def get_shard(self, shard_id: str) -> ShardConfig:
        """Look up a shard by its ID."""
        for shard in self.shards:
            if shard.id == shard_id:
                return shard
        raise ConfigError(f"Shard '{shard_id}' not found in configuration")

    # --- Factory methods ---

    @classmethod
    def from_file(cls, path: str | Path) -> MuxConfig:
        """Load configuration from a YAML or JSON file.

        Environment variables in the form ``${VAR_NAME}`` are interpolated
        before parsing.
        """
        filepath = Path(path)
        if not filepath.exists():
            raise ConfigError(f"Configuration file not found: {filepath}")

        raw = filepath.read_text(encoding="utf-8")

        try:
            data = yaml.safe_load(raw)
        except yaml.YAMLError as exc:
            raise ConfigError(f"Failed to parse config file: {exc}") from exc

        if not isinstance(data, dict):
            raise ConfigError("Config file must contain a YAML mapping (dict) at the top level")

        data = _interpolate_env(data)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MuxConfig:
        """Build a MuxConfig from a raw dictionary."""
        try:
            cluster_data = data.get("cluster")
            if not cluster_data:
                raise ConfigError("'cluster' section is required in configuration")

            cluster = ClusterConfig(
                name=cluster_data["name"],
                strategy=cluster_data["strategy"],
                shard_key=cluster_data.get("shard_key", "id"),
                virtual_nodes=cluster_data.get("virtual_nodes", 256),
            )

            # Global credentials (can be overridden per-shard)
            global_creds = data.get("credentials", {})
            global_username = global_creds.get("username")
            global_password = global_creds.get("password")

            shards_data = data.get("shards", [])
            if not shards_data:
                raise ConfigError("'shards' section must contain at least one shard")

            shards: list[ShardConfig] = []
            for s in shards_data:
                shard_range = None
                if "range" in s:
                    shard_range = ShardRange(
                        start=s["range"]["start"],
                        end=s["range"]["end"],
                    )
                shards.append(
                    ShardConfig(
                        id=s["id"],
                        backend=s["backend"],
                        host=s["host"],
                        port=s["port"],
                        database=s["database"],
                        weight=s.get("weight", 1),
                        range=shard_range,
                        tags=s.get("tags", {}),
                        username=s.get("username", global_username),
                        password=s.get("password", global_password),
                    )
                )

            pool_data = data.get("pool", {})
            pool = PoolConfig(
                min_size=pool_data.get("min_size", 2),
                max_size=pool_data.get("max_size", 20),
                slow_start_ms=pool_data.get("slow_start_ms", 10),
                idle_timeout_s=pool_data.get("idle_timeout_s", 300),
                health_check_interval_s=pool_data.get("health_check_interval_s", 30),
            )

            routing_data = data.get("routing", {})
            routing = RoutingConfig(
                scatter_gather=routing_data.get("scatter_gather", True),
                read_preference=routing_data.get("read_preference", "primary"),
                retry_on_shard_error=routing_data.get("retry_on_shard_error", True),
            )

            balancer_data = data.get("balancer", {})
            balancer = BalancerConfig(
                enabled=balancer_data.get("enabled", True),
                check_interval_s=balancer_data.get("check_interval_s", 60),
                suppression_threshold=balancer_data.get("suppression_threshold", 0.25),
                qps_split_threshold=balancer_data.get("qps_split_threshold", 5000),
                balance_factor_min=balancer_data.get("balance_factor_min", 0.3),
                split_crossing_penalty_max=balancer_data.get(
                    "split_crossing_penalty_max", 0.1
                ),
            )

            tel_data = data.get("telemetry", {})
            export_data = tel_data.get("export", {})
            telemetry = TelemetryConfig(
                enabled=tel_data.get("enabled", True),
                window_size_s=tel_data.get("window_size_s", 60),
                hot_key_threshold_pct=tel_data.get("hot_key_threshold_pct", 1.0),
                export=TelemetryExportConfig(
                    type=export_data.get("type", "memory"),
                    endpoint=export_data.get("endpoint"),
                ),
            )

            sec_data = data.get("security", {})
            tls_data = sec_data.get("tls", {})
            auth_data = sec_data.get("auth", {})
            security = SecurityConfig(
                tls=SecurityTLSConfig(
                    enabled=tls_data.get("enabled", False),
                    cert_file=tls_data.get("cert_file"),
                    key_file=tls_data.get("key_file"),
                    ca_file=tls_data.get("ca_file"),
                ),
                auth=SecurityAuthConfig(
                    type=auth_data.get("type", "none"),
                    token=auth_data.get("token"),
                ),
            )

            return cls(
                cluster=cluster,
                shards=shards,
                pool=pool,
                routing=routing,
                balancer=balancer,
                telemetry=telemetry,
                security=security,
            )

        except ConfigError:
            raise
        except KeyError as exc:
            raise ConfigError(f"Missing required config key: {exc}") from exc
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"Invalid config value: {exc}") from exc
