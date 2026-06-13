# MuxDB

Autonomous Data Orchestration Platform for Distributed Databases.

MuxDB is an enterprise-grade, multi-language wrapper SDK (**Python** + **Node.js/TypeScript**) that sits on top of your existing databases and ORMs to provide transparent query routing, adaptive connection pooling, workload telemetry, and autonomous sharding.

---

## 🚀 Key Features

* **Multi-Language Equivalency:** Unified design system, configuration, and API footprint across both Python (`muxdb`) and Node.js (`muxdb`) packages.
* **Transparent Query Routing (Layer 1):** SQL parser extracts shard keys (e.g. `user_id = 42`) and routes queries to single shards, broadcasts DDL, or scatter-gathers multi-shard read operations.
* **Adaptive Connection Pooling:** Citus-style 10ms slow-start pool scaling per-shard with idle connection eviction and transaction pinning.
* **Workload-Aware Placement (Layer 2):** In-memory consistent hashing with customizable virtual-node counts, range-based routing, and hash strategies.
* **Resilient Driver Layer:** Native wrappers for PostgreSQL (`psycopg` / `pg`).

---

## 🛠️ Project Structure

```text
MuxDB/
├── docs/                      # Architectural designs and research reports
├── muxdb-py/                  # Python SDK package
│   ├── muxdb/                 # Core Python source modules
│   └── pyproject.toml         # Python packaging config
├── muxdb-js/                  # Node.js/TypeScript SDK package
│   ├── src/                   # TypeScript source modules
│   ├── tsconfig.json          # TS compilation options
│   └── package.json           # npm packaging config
├── muxdb.schema.yaml          # Canonical configuration schema
└── README.md                  # Project overview (this file)
```

---

## 📦 Installation

For development, you can install the SDK packages locally:

### Python
Ensure you are in a virtual environment (`.venv`), then install:
```bash
uv pip install -e ./muxdb-py/
```

### Node.js / TypeScript
Build the package and link or install it:
```bash
cd muxdb-js
npm install
npm run build
```

---

## ⚡ Quick Start

### 1. Define Cluster Configuration (`muxdb.yaml`)

```yaml
cluster:
  name: "production-cluster"
  strategy: "consistent_hash"
  shard_key: "user_id"
  virtual_nodes: 256

shards:
  - id: "shard-0"
    backend: "postgresql"
    host: "localhost"
    port: 5432
    database: "orders_shard_0"
  - id: "shard-1"
    backend: "postgresql"
    host: "localhost"
    port: 5433
    database: "orders_shard_1"

pool:
  min_size: 2
  max_size: 10
  slow_start_ms: 10
```

### 2. Usage in Python

```python
from muxdb import MuxDB, MuxConfig

# Load cluster configurations
config = MuxConfig.from_file("muxdb.yaml")
db = MuxDB(config)

# Connect to all shards and open connection pools
db.connect()

try:
    # 1. Single-shard query routed transparently by user_id
    result = db.execute("SELECT * FROM orders WHERE user_id = 42")
    print(f"Routed to: {result.shard_id}")
    for row in result.rows:
        print(row)

    # 2. Scatter-gather query executing in parallel across all shards
    summary = db.execute("SELECT COUNT(*) FROM orders")
    print(f"Total rows count: {summary.scalar}")
finally:
    db.close()
```

### 3. Usage in Node.js / TypeScript

```typescript
import { MuxDB, MuxConfig } from "muxdb";

// Load configurations
const config = MuxConfig.fromFile("muxdb.yaml");
const db = new MuxDB(config);

await db.connect();

try {
  // 1. Single-shard query routed by user_id
  const result = await db.execute("SELECT * FROM orders WHERE user_id = 42");
  console.log(`Routed to shard: ${result.shardId}`);
  
  // 2. Parallel scatter-gather execution
  const summary = await db.execute("SELECT COUNT(*) FROM orders");
  console.log(`Total count: ${summary.rows[0]?.count}`);
} finally {
  await db.close();
}
```

---

## 📜 License

This project is licensed under the [MIT License](LICENSE).