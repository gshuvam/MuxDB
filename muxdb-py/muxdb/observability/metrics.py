from __future__ import annotations

from typing import Any
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import start_http_server

# Initialize OpenTelemetry meter for MuxDB
_meter = metrics.get_meter("muxdb")

# Core metrics definitions
queries_total = _meter.create_counter(
    "muxdb_queries_total",
    description="Total number of queries routed by MuxDB",
    unit="1",
)

errors_total = _meter.create_counter(
    "muxdb_errors_total",
    description="Total number of query errors in MuxDB",
    unit="1",
)

rebalances_total = _meter.create_counter(
    "muxdb_rebalances_total",
    description="Total number of shard rebalancing events",
    unit="1",
)

query_duration = _meter.create_histogram(
    "muxdb_query_duration_seconds",
    description="Query execution latency in seconds",
    unit="s",
)

# Active/idle connections gauges can be fetched dynamically or pushed.
# OpenTelemetry allows ObservableGauge.
_pool_stats_provider = None

def register_pool_stats_callback(callback_func: Any) -> None:
    """Register a callback to yield active/idle pool stats."""
    global _pool_stats_provider
    _pool_stats_provider = callback_func

def setup_metrics(prometheus_port: int | None = None) -> None:
    """Initialize metrics provider and optionally start Prometheus exporter."""
    if prometheus_port is not None:
        reader = PrometheusMetricReader()
        provider = MeterProvider(metric_readers=[reader])
        metrics.set_meter_provider(provider)
        try:
            start_http_server(prometheus_port)
        except Exception:
            # Server might already be running, e.g. in test runs
            pass
