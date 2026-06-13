import { metrics } from '@opentelemetry/api';

const meter = metrics.getMeter('muxdb');

export const queriesTotal = meter.createCounter('muxdb_queries_total', {
  description: 'Total number of queries routed by MuxDB',
});

export const errorsTotal = meter.createCounter('muxdb_errors_total', {
  description: 'Total number of query errors in MuxDB',
});

export const rebalancesTotal = meter.createCounter('muxdb_rebalances_total', {
  description: 'Total number of shard rebalancing events',
});

export const queryDuration = meter.createHistogram('muxdb_query_duration_seconds', {
  description: 'Query execution latency in seconds',
  unit: 's',
});
