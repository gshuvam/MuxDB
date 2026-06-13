# Getting Started

Setup MuxDB client SDKs and configure database routing.

## Installation

### Python
```bash
pip install "muxdb[all]"
```

### Node.js
```bash
npm install muxdb
```

## Basic Configuration
Create a configuration file `muxdb.yaml` containing the cluster settings:
```yaml
cluster:
  name: billing_cluster
  strategy: consistent_hash
  shard_key: user_id
shards:
  - id: shard-0
    backend: postgresql
    host: localhost
    port: 5432
    database: db_shard_0
  - id: shard-1
    backend: postgresql
    host: localhost
    port: 5433
    database: db_shard_1
```
