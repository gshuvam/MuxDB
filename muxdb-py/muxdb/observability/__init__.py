"""
MuxDB Observability Module.
Provides tracing, metrics, and structured logging tools.
"""

from muxdb.observability.logging import (
    get_correlation_id,
    set_correlation_id,
    setup_logging,
)
from muxdb.observability.metrics import (
    errors_total,
    queries_total,
    query_duration,
    rebalances_total,
    setup_metrics,
)
from muxdb.observability.tracing import get_tracer, trace_span

__all__ = [
    "get_correlation_id",
    "set_correlation_id",
    "setup_logging",
    "queries_total",
    "errors_total",
    "rebalances_total",
    "query_duration",
    "setup_metrics",
    "get_tracer",
    "trace_span",
]
