# 6-Layer Architecture

MuxDB organizes database operations into 6 decoupled control layers:

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

1.  **Logical Router (Layer 1):** Intercepts database statements, extracts keys, and maps to target database connections.
2.  **Dynamic Placement (Layer 2):** Promotes read-hot keys into cache layers to minimize database utilization.
3.  **Balancer (Layer 3):** Analyzes sliding QPS loads to trigger replica resizes or key splits.
4.  **Live Migrator (Layer 4):** Moves ranges live between databases without downtime.
5.  **ML Predictor (Layer 5):** Forecasts cyclical demand spikes to scale resources ahead of workload spikes.
6.  **Evaluator (Layer 6):** Validates and simulates performance under failure and chaos.
