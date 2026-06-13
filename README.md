# MuxDB — Autonomous Data Orchestration Platform

Enterprise-grade database sharding, routing, and autonomic placement control plane across **Python** and **Node.js/TypeScript** ecosystems.

MuxDB implements the **6-layer self-optimizing database architecture** described in our [Strategy Document](file:///home/shuva/Development/Projects/MuxDB/docs/strategy.md). It sits transparently on top of existing databases (PostgreSQL, Redis, MongoDB, Qdrant) and ORMs (SQLAlchemy, Prisma, Drizzle, etc.) to manage routing, load balancing, caching, and downtime-free live migrations.

---

## 🚀 The 6-Layer Architecture

```
┌───────────────────────────────────────┐
│ Layer 1: Logical Router (MuxProxy)    │
├───────────────────────────────────────┤
│ Layer 2: Dynamic Placement (E-Store)  │
├───────────────────────────────────────┤
│ Layer 3: Load Balancer (Slicer)       │
├───────────────────────────────────────┤
│ Layer 4: Live Migrator (Zephyr)       │
├───────────────────────────────────────┤
│ Layer 5: ML Control Plane (P-Store)   │
├───────────────────────────────────────┤
│ Layer 6: Evaluation Harness (Chaos)   │
└───────────────────────────────────────┘
```

1.  **Logical Router (Layer 1):** Best-effort SQL shard key extraction, single-shard routing, transaction pinning, and parallel scatter-gather query aggregation.
2.  **Dynamic Placement (Layer 2):** Two-tier caching (Redis L2 cache + SQL persistent shard) dynamically promoting read-hot keys and invalidating cached items on write.
3.  **Load Balancer (Layer 3):** Monitors real-time telemetry to trigger L2 key range splits, L3 replica resizes, or lease transfers, enforcing cooldowns and write-load suppression rules.
4.  **Live Migrator (Layer 4):** Zephyr dual-mode replication: pulls requested keys on-demand in-flight to prevent downtime, while pushing cold keys in background batches regulated by a PID Controller.
5.  **ML Control Plane (Layer 5):**
    *   **Workload Predictor (P-Store):** Diurnal/cyclical time-series forecasting (Holt-Winters) recommending proactive scaling before spikes hit.
    *   **Bandit Solver (AW-CUCB-DP):** Contextual Multi-Armed Bandit with a Page-Hinkley Change Detector to reset or decay weights on workload shifts.
    *   **Knob Auto-Tuner (QTune/UDO):** Reinforcement Learning DQN agent implemented in pure Python to tune database configs.
6.  **Evaluation Harness (Layer 6):** Event-driven simulator validating cluster performance under stable, hotspot, and diurnal workloads combined with scheduled node failures or slowdowns.

---

## 🛠️ Project Structure

```text
MuxDB/
├── docs/                      # Strategy and deep research documents
├── deploy/                    # Kubernetes Helm charts, Dockerfiles, and Terraform VPC plans
├── .github/workflows/         # Actions workflows (CI matrix, tagged releases, security audits)
├── muxdb-py/                  # Python SDK
│   ├── muxdb/                 # Core, ML, Evaluator, and integrations module
│   └── pyproject.toml         # pyproject.toml package configuration
├── muxdb-js/                  # Node.js/TypeScript SDK
│   ├── src/                   # Core TypeScript sources
│   └── package.json           # npm packaging config
├── muxdb-cli/                 # Click-based administration Python CLI
└── muxdb-dashboard/           # Next.js administrative dashboard app
```

---

## 📦 Installation

### Python SDK
Ensure you are in a virtual environment (`.venv`), then install:
```bash
cd muxdb-py
uv pip install -e ".[all,dev]"
```

### Node.js / TypeScript SDK
Build and transpile the package:
```bash
cd muxdb-js
npm ci
npm run build
```

---

## 🖥️ Operations: CLI & Admin Dashboard

### Python CLI
MuxDB includes a terminal-friendly management CLI powered by `rich` layout rendering:
```bash
# Register MuxDB CLI in path
cd muxdb-cli
uv pip install -e . --python ../muxdb-py/.venv/bin/python

# Validate configuration
muxdb validate --config muxdb.yaml

# Check cluster health
muxdb status --config muxdb.yaml

# Trigger key range migration
muxdb migrate --config muxdb.yaml --id mig-202 --source shard-0 --dest shard-1 --keys user_10,user_12
```

### Next.js Admin Dashboard
Visual React interface displaying cluster topology state:
```bash
# Run Next.js dashboard
cd muxdb-dashboard
npm install
npm run build
npm run dev
```
*   **Visualizations:** Interactive health status indicators (healthy, slow, failed), real-time QPS, latency gauges, and live Zephyr migration progress trackers.
*   **Controls:** Live buttons to trigger simulated failures (`FAIL_NODE`), node slow states (`SLOW_NODE`), or register new database nodes (`ADD_NODE`).

---

## 📜 License

This project is licensed under the [MIT License](LICENSE).