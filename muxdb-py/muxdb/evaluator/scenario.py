from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import List, Dict, Any, Optional


@dataclass
class ClusterChangeEvent:
    """Represents a scheduled cluster event during a simulation."""
    step: int
    action_type: str  # "ADD_NODE", "REMOVE_NODE", "SLOW_NODE", "FAIL_NODE"
    target_shard_id: str
    parameter: Any = None


class WorkloadScenario:
    """
    WorkloadScenario generates structured step-by-step query lists
    to simulate database workloads.
    """

    def __init__(self, num_keys: int = 500, default_tenant: str = "tenant_0") -> None:
        self.num_keys = num_keys
        self.default_tenant = default_tenant

    def stable_baseline(self, steps: int, qps: int = 50) -> List[List[Dict[str, Any]]]:
        """Generates a stable, uniform key workload over time steps."""
        workload = []
        for _ in range(steps):
            step_ops = []
            for _ in range(qps):
                key = f"key_{random.randint(0, self.num_keys - 1)}"
                is_write = random.random() < 0.2
                step_ops.append({
                    "op": "write" if is_write else "read",
                    "key": key,
                    "tenant_id": self.default_tenant,
                })
            workload.append(step_ops)
        return workload

    def point_hotspot(
        self,
        steps: int,
        qps: int = 50,
        hotspot_key: str = "key_hot",
        hotspot_start_step: int = 10,
        hotspot_ratio: float = 0.7,
    ) -> List[List[Dict[str, Any]]]:
        """Generates uniform traffic, but introduces a massive key hotspot at a specific step."""
        workload = []
        for step in range(steps):
            step_ops = []
            for _ in range(qps):
                if step >= hotspot_start_step and random.random() < hotspot_ratio:
                    key = hotspot_key
                else:
                    key = f"key_{random.randint(0, self.num_keys - 1)}"
                is_write = random.random() < 0.2
                step_ops.append({
                    "op": "write" if is_write else "read",
                    "key": key,
                    "tenant_id": self.default_tenant,
                })
            workload.append(step_ops)
        return workload

    def diurnal_spikes(
        self,
        steps: int,
        base_qps: int = 30,
        amplitude: int = 25,
        period_steps: int = 24,
    ) -> List[List[Dict[str, Any]]]:
        """Generates a fluctuating workload size following a diurnal sine-wave pattern."""
        workload = []
        for step in range(steps):
            # Compute QPS for this step
            qps = int(base_qps + amplitude * math.sin(2 * math.pi * step / period_steps))
            qps = max(5, qps)  # Ensure at least some queries

            step_ops = []
            for _ in range(qps):
                # Zipfian-like key distribution
                key_idx = int(random.paretovariate(1.5) * 10) % self.num_keys
                key = f"key_{key_idx}"
                is_write = random.random() < 0.2
                step_ops.append({
                    "op": "write" if is_write else "read",
                    "key": key,
                    "tenant_id": self.default_tenant,
                })
            workload.append(step_ops)
        return workload

    def mixed_oltp_olap(
        self,
        steps: int,
        qps: int = 40,
        olap_start_step: int = 15,
        scan_size: int = 20,
    ) -> List[List[Dict[str, Any]]]:
        """Simulates light transactional reads/writes, with large OLAP scan spikes."""
        workload = []
        for step in range(steps):
            step_ops = []
            for _ in range(qps):
                key = f"key_{random.randint(0, self.num_keys - 1)}"
                is_write = random.random() < 0.15
                step_ops.append({
                    "op": "write" if is_write else "read",
                    "key": key,
                    "tenant_id": self.default_tenant,
                })

            # Introduce OLAP scan at specific steps
            if step >= olap_start_step and step % 10 == 0:
                # Simulates scatter-gather scans across multiple keys
                start_idx = random.randint(0, self.num_keys - scan_size - 1)
                for i in range(scan_size):
                    step_ops.append({
                        "op": "read",
                        "key": f"key_{start_idx + i}",
                        "tenant_id": self.default_tenant,
                        "is_scan": True,
                    })

            workload.append(step_ops)
        return workload

    @staticmethod
    def trace_driven_replay(trace_ops: List[List[Dict[str, Any]]]) -> List[List[Dict[str, Any]]]:
        """Directly accepts a pre-defined array/list of time-step batches."""
        return trace_ops
