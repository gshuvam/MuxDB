# CLI & Dashboard

Inspect, manage, and monitor MuxDB clusters.

## Python CLI
Run command line metrics:
```bash
# Check status
muxdb status --config muxdb.yaml

# List shards
muxdb list --config muxdb.yaml

# Trigger key range migrations
muxdb migrate --config muxdb.yaml --id mig-123 --source shard-0 --dest shard-1 --keys user_10,user_11
```

## Admin Dashboard
The dashboard provides a visual React interface displaying:
*   Global cluster throughput QPS metrics.
*   Per-shard health grid states (healthy, slow, failed nodes).
*   Live progress percentage bars for running Zephyr migrations.
*   Simulated controller buttons (trigger failures, slow states, and manual rebalances).
