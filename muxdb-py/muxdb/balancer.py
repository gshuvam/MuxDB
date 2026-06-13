from __future__ import annotations

import time
from typing import Any, Dict, List

class BalancingAction:
    """Represents a load balancing action recommended by the Balancer."""

    def __init__(self, action_type: str, shard_id: str, details: Dict[str, Any]) -> None:
        self.action_type = action_type  # "L1_LEASE", "L2_SPLIT", "L3_REPLICA"
        self.shard_id = shard_id
        self.details = details

    def __repr__(self) -> str:
        return f"BalancingAction(type={self.action_type}, shard={self.shard_id}, details={self.details})"


class Balancer:
    """Evaluates telemetry metrics to trigger dynamic range splits, replica scales, or lease transfers."""

    def __init__(
        self,
        cooldown_seconds: float = 10.0,
        qps_split_threshold: float = 100.0,
        qps_replica_threshold: float = 50.0,
        write_suppression_threshold: float = 80.0,
    ) -> None:
        self.cooldown_seconds = cooldown_seconds
        self.qps_split_threshold = qps_split_threshold
        self.qps_replica_threshold = qps_replica_threshold
        self.write_suppression_threshold = write_suppression_threshold
        self.last_balance_time = 0.0

    def evaluate(self, metrics: Dict[str, Dict[str, float]]) -> List[BalancingAction]:
        """Analyze shard metrics and generate balancing actions.

        Applies suppression rule under high write-throughput or cooldown state.
        """
        now = time.time()
        if now - self.last_balance_time < self.cooldown_seconds:
            return []

        # Write load suppression check
        total_write_qps = sum(m.get("write_qps", 0.0) for m in metrics.values())
        if total_write_qps >= self.write_suppression_threshold:
            return []

        actions: List[BalancingAction] = []

        for shard_id, data in metrics.items():
            read_qps = data.get("read_qps", 0.0)
            write_qps = data.get("write_qps", 0.0)
            total_qps = read_qps + write_qps

            # Check L2 Split
            if total_qps >= self.qps_split_threshold:
                actions.append(
                    BalancingAction(
                        action_type="L2_SPLIT",
                        shard_id=shard_id,
                        details={"qps": total_qps, "split_threshold": self.qps_split_threshold},
                    )
                )
                continue

            # Check L3 Replica Scaling
            if read_qps >= self.qps_replica_threshold:
                actions.append(
                    BalancingAction(
                        action_type="L3_REPLICA",
                        shard_id=shard_id,
                        details={"read_qps": read_qps, "replica_threshold": self.qps_replica_threshold},
                    )
                )
                continue

            # Check L1 Lease Transfer (read lease offloaded if write-heavy)
            if write_qps > 10.0 and read_qps > 20.0:
                actions.append(
                    BalancingAction(
                        action_type="L1_LEASE",
                        shard_id=shard_id,
                        details={"read_qps": read_qps, "write_qps": write_qps},
                    )
                )

        if actions:
            self.last_balance_time = now

        return actions
