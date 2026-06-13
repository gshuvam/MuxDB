"""
Query router — Layer 1 core.

Parses incoming queries, extracts shard keys, resolves the target shard(s)
via the ShardMap, and orchestrates scatter-gather for cross-shard queries.

Implements the Vitess VTGate / Slicer Clerk routing model: the application
sees a single logical database, the router handles all physical topology.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Sequence

import structlog

from muxdb.config import MuxConfig
from muxdb.errors import RoutingError, CrossShardTransactionError
from muxdb.shard_map import ShardMap, ShardInfo, create_shard_map

logger = structlog.get_logger(__name__)


class RouteType(Enum):
    """Classification of how a query should be executed."""

    SINGLE = auto()       # Routed to exactly one shard
    SCATTER = auto()      # Sent to all shards, results gathered
    BROADCAST = auto()    # Sent to all shards (DDL, no result merge)


@dataclass(frozen=True)
class RouteDecision:
    """The outcome of routing a query."""

    type: RouteType
    targets: list[ShardInfo]
    shard_key_value: str | int | None = None
    query: str = ""
    params: tuple[Any, ...] = ()
    elapsed_us: float = 0.0  # microseconds spent routing


@dataclass
class QueryContext:
    """Metadata about the current query for routing decisions."""

    query: str
    params: tuple[Any, ...] = ()
    shard_key: str | None = None       # Column to extract from query
    shard_key_value: Any = None        # Explicit shard key value (overrides extraction)
    transaction_shard: str | None = None  # If inside a transaction, pinned shard ID
    read_only: bool = False


# ---------------------------------------------------------------------------
# SQL shard key extraction (lightweight — not a full parser)
# ---------------------------------------------------------------------------

# Matches: WHERE shard_key = <value> or WHERE shard_key IN (...)
_WHERE_EQ_PATTERN = re.compile(
    r"WHERE\s+.*?\b{key}\b\s*=\s*(?:'([^']*)'|(\d+)|(%s|\$\d+|\?))",
    re.IGNORECASE | re.DOTALL,
)

# DDL patterns that should be broadcast
_DDL_KEYWORDS = {"CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE"}


def _extract_shard_key(query: str, shard_key: str, params: tuple[Any, ...]) -> Any | None:
    """Attempt to extract the shard key value from a SQL query.

    This is a best-effort heuristic parser. For complex queries, users
    should provide the shard key value explicitly via QueryContext.

    Returns:
        The extracted key value, or None if extraction fails.
    """
    pattern = re.compile(
        rf"\b{re.escape(shard_key)}\b\s*=\s*(?:'([^']*)'|(\d+)|(%s|\$(\d+)|\?))",
        re.IGNORECASE,
    )
    match = pattern.search(query)
    if not match:
        return None

    # String literal: 'value'
    if match.group(1) is not None:
        return match.group(1)

    # Numeric literal: 42
    if match.group(2) is not None:
        return int(match.group(2))

    # Parameterized placeholder
    if match.group(3) is not None:
        placeholder = match.group(3)

        # Positional ($1, $2, ...) — asyncpg style
        if match.group(4) is not None:
            idx = int(match.group(4)) - 1  # $1 → index 0
            if 0 <= idx < len(params):
                return params[idx]
            return None

        # %s or ? — sequential positional
        # Count how many placeholders appear before this one
        prefix = query[: match.start(3)]
        prior_count = prefix.count("%s") + prefix.count("?")
        if prior_count < len(params):
            return params[prior_count]

    return None


def _is_ddl(query: str) -> bool:
    """Check if a query is a DDL statement that should be broadcast."""
    first_word = query.strip().split()[0].upper() if query.strip() else ""
    return first_word in _DDL_KEYWORDS


class Router:
    """
    Query router implementing the MuxDB Layer 1 routing logic.

    The router inspects each query to determine which shard(s) it targets,
    using the configured shard key and the ShardMap's resolution strategy.

    For queries where the shard key cannot be extracted (e.g. complex joins),
    the router falls back to scatter-gather across all shards.

    Example::

        router = Router(config)
        decision = router.route("SELECT * FROM orders WHERE user_id = 42")
        # decision.type == RouteType.SINGLE
        # decision.targets == [ShardInfo(id='shard_2', ...)]
    """

    def __init__(self, config: MuxConfig, shard_map: ShardMap | None = None) -> None:
        self._config = config
        self._shard_key = config.cluster.shard_key
        self._scatter_enabled = config.routing.scatter_gather
        self._shard_map = shard_map or create_shard_map(config)

    @property
    def shard_map(self) -> ShardMap:
        """Access the underlying shard map."""
        return self._shard_map

    def route(self, ctx: QueryContext | str, params: tuple[Any, ...] = ()) -> RouteDecision:
        """Route a query to target shard(s).

        Args:
            ctx: A :class:`QueryContext` or a raw SQL string.
            params: Query parameters (used only when ctx is a string).

        Returns:
            A :class:`RouteDecision` describing where and how to execute.

        Raises:
            RoutingError: If routing fails and scatter-gather is disabled.
            CrossShardTransactionError: If a transaction is pinned and the
                query targets a different shard.
        """
        t0 = time.monotonic()

        if isinstance(ctx, str):
            ctx = QueryContext(query=ctx, params=params)

        decision = self._resolve(ctx)
        elapsed_us = (time.monotonic() - t0) * 1_000_000

        decision = RouteDecision(
            type=decision.type,
            targets=decision.targets,
            shard_key_value=decision.shard_key_value,
            query=ctx.query,
            params=ctx.params,
            elapsed_us=elapsed_us,
        )

        logger.debug(
            "router.route",
            route_type=decision.type.name,
            targets=[t.id for t in decision.targets],
            shard_key_value=decision.shard_key_value,
            elapsed_us=f"{elapsed_us:.1f}",
        )

        return decision

    def _resolve(self, ctx: QueryContext) -> RouteDecision:
        """Core resolution logic."""

        # 1. DDL → broadcast to all shards
        if _is_ddl(ctx.query):
            return RouteDecision(
                type=RouteType.BROADCAST,
                targets=self._shard_map.shards,
            )

        # 2. Transaction pinning — enforce single-shard
        if ctx.transaction_shard:
            shard = self._shard_map.get_shard(ctx.transaction_shard)
            return RouteDecision(
                type=RouteType.SINGLE,
                targets=[shard],
                shard_key_value=ctx.shard_key_value,
            )

        # 3. Explicit shard key value provided
        if ctx.shard_key_value is not None:
            shard = self._shard_map.resolve(ctx.shard_key_value)
            return RouteDecision(
                type=RouteType.SINGLE,
                targets=[shard],
                shard_key_value=ctx.shard_key_value,
            )

        # 4. Extract shard key from query
        effective_key = ctx.shard_key or self._shard_key
        extracted = _extract_shard_key(ctx.query, effective_key, ctx.params)

        if extracted is not None:
            shard = self._shard_map.resolve(extracted)
            return RouteDecision(
                type=RouteType.SINGLE,
                targets=[shard],
                shard_key_value=extracted,
            )

        # 5. Fallback: scatter-gather
        if self._scatter_enabled:
            return RouteDecision(
                type=RouteType.SCATTER,
                targets=self._shard_map.shards,
            )

        raise RoutingError(
            f"Cannot determine shard for query and scatter_gather is disabled. "
            f"Provide a '{effective_key}' value in the WHERE clause or set "
            f"routing.scatter_gather=true in config.",
            details={"query_prefix": ctx.query[:80]},
        )

    def route_key(self, key: str | int) -> ShardInfo:
        """Directly resolve a shard key to its owning shard.

        Convenience method that bypasses query parsing.
        """
        return self._shard_map.resolve(key)

    def route_keys(self, keys: Sequence[str | int]) -> dict[str, list[str | int]]:
        """Group multiple keys by their target shard.

        Returns:
            Dict mapping shard_id → list of keys.
        """
        return self._shard_map.resolve_many(keys)
