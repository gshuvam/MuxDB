from __future__ import annotations

import math
import random
from typing import List, Dict, Any, Optional

from muxdb.config import MuxConfig, ClusterConfig, ShardConfig
from muxdb.router import Router
from muxdb.telemetry import TelemetryCollector
from muxdb.balancer import Balancer
from muxdb.ml import BanditSolver, DRLTuner
from muxdb.shard_map import ShardInfo
from muxdb.evaluator.scenario import ClusterChangeEvent


class SimulationRunner:
    """
    SimulationRunner drives step-by-step workload replays against a simulated
    MuxDB router and cluster state, tracking observability metrics.
    """

    def __init__(
        self,
        router: Router,
        telemetry: TelemetryCollector,
        balancer: Balancer,
        bandit_solver: Optional[BanditSolver] = None,
        drl_tuner: Optional[DRLTuner] = None,
    ) -> None:
        self.router = router
        self.telemetry = telemetry
        self.balancer = balancer
        self.bandit_solver = bandit_solver
        self.drl_tuner = drl_tuner

        # Simulated cluster health and performance properties
        self.shard_latencies: Dict[str, float] = {}  # Base latency in ms
        self.shard_capacities: Dict[str, float] = {}  # Max QPS capacity
        self.shard_slowdowns: Dict[str, float] = {}  # Latency multipliers
        self.failed_shards: set[str] = set()

        self.knob_state = {"shared_buffers": 256.0, "work_mem": 16.0}
        self.total_keys_migrated = 0
        
        # Initialize defaults for router shards
        for shard in self.router.shard_map.shards:
            self.shard_latencies[shard.id] = 5.0
            self.shard_capacities[shard.id] = 100.0
            self.shard_slowdowns[shard.id] = 1.0

    def apply_event(self, event: ClusterChangeEvent) -> None:
        """Applies cluster hardware state transitions."""
        shard_id = event.target_shard_id
        if event.action_type == "ADD_NODE":
            current_shards = list(self.router.shard_map.shards)
            if not any(s.id == shard_id for s in current_shards):
                current_shards.append(ShardInfo(
                    id=shard_id,
                    backend="postgresql",
                    host="localhost",
                    port=5432,
                    database=f"db_{shard_id}",
                    weight=1,
                    dsn=f"postgresql://localhost:5432/db_{shard_id}",
                ))
                self.router.shard_map.swap(current_shards)
            self.shard_latencies[shard_id] = 5.0
            self.shard_capacities[shard_id] = 100.0
            self.shard_slowdowns[shard_id] = 1.0
        elif event.action_type == "REMOVE_NODE":
            current_shards = [s for s in self.router.shard_map.shards if s.id != shard_id]
            self.router.shard_map.swap(current_shards)
            self.failed_shards.discard(shard_id)
        elif event.action_type == "SLOW_NODE":
            multiplier = event.parameter if event.parameter is not None else 5.0
            self.shard_slowdowns[shard_id] = multiplier
        elif event.action_type == "FAIL_NODE":
            self.failed_shards.add(shard_id)

    def run(
        self,
        workload: List[List[Dict[str, Any]]],
        events: Optional[List[ClusterChangeEvent]] = None,
        policy: str = "adaptive",  # "static", "reactive", "adaptive"
    ) -> Dict[str, Any]:
        """
        Runs the simulation step-by-step.
        Returns:
            Dict: Compiled metrics summary.
        """
        events_map: Dict[int, List[ClusterChangeEvent]] = {}
        if events:
            for event in events:
                events_map.setdefault(event.step, []).append(event)

        step_metrics: List[Dict[str, Any]] = []
        overall_queries = 0
        overall_errors = 0
        overall_latencies = []

        for step, step_ops in enumerate(workload):
            # 1. Apply events scheduled for this step
            if step in events_map:
                for event in events_map[step]:
                    self.apply_event(event)

            # Reset step count
            step_latencies = []
            step_errors = 0
            
            # Temporary counters to calculate queueing delays per shard in this step
            shard_queries_in_step: Dict[str, int] = {s: 0 for s in self.shard_latencies}

            # 2. Route and execute queries
            for op in step_ops:
                key = op["key"]
                is_write = op["op"] == "write"
                
                # Determine target shard using router (static/reactive) or bandit (adaptive)
                if policy == "adaptive" and self.bandit_solver:
                    target_shard_id = self.bandit_solver.select_arm({"type": op["op"]})
                else:
                    try:
                        target_shard = self.router.route_key(key)
                        target_shard_id = target_shard.id
                    except Exception:
                        target_shard_id = list(self.shard_latencies.keys())[0]

                shard_queries_in_step[target_shard_id] = shard_queries_in_step.get(target_shard_id, 0) + 1
                
                # Handle failed shard simulating error
                if target_shard_id in self.failed_shards:
                    step_errors += 1
                    overall_errors += 1
                    # Record query failure in telemetry
                    self.telemetry.record_query(target_shard_id, key, 1000.0, is_write)
                    continue

                # Simulate execution latency: base_latency * slowdown_multiplier + queueing_delay
                base = self.shard_latencies.get(target_shard_id, 5.0)
                slowdown = self.shard_slowdowns.get(target_shard_id, 1.0)
                capacity = self.shard_capacities.get(target_shard_id, 100.0)
                
                # Estimate current QPS load on this shard
                current_qps = shard_queries_in_step[target_shard_id]
                queueing_delay = 0.0
                if current_qps > 0:
                    # Simple queue model: delay spikes when load approaches capacity
                    load_ratio = current_qps / capacity
                    queueing_delay = 10.0 * (load_ratio ** 2)

                # DRL knob adjustment effect (e.g. higher work_mem lowers latency slightly)
                knob_benefit = (self.knob_state["work_mem"] - 16.0) / 10.0
                latency = max(1.0, base * slowdown + queueing_delay - knob_benefit)
                
                # Record to telemetry
                self.telemetry.record_query(target_shard_id, key, latency, is_write)
                
                step_latencies.append(latency)
                overall_latencies.append(latency)
                overall_queries += 1

                # Update bandit reward if policy is adaptive
                if policy == "adaptive" and self.bandit_solver:
                    # Reward function: inverse of latency, normalized
                    reward = 1.0 / (1.0 + (latency / 100.0))
                    self.bandit_solver.update_reward(target_shard_id, reward)

            # 3. Dynamic Balancing Decisions
            if policy != "static" and step % 2 == 0:
                # Gather metrics from telemetry
                metrics: Dict[str, Dict[str, float]] = {}
                for shard_id in self.shard_latencies:
                    metrics[shard_id] = {
                        "read_qps": self.telemetry.get_qps(shard_id, is_write=False),
                        "write_qps": self.telemetry.get_qps(shard_id, is_write=True),
                    }
                
                decisions = self.balancer.evaluate(metrics)
                for action in decisions:
                    if action.action_type in ("L2_SPLIT", "L3_REPLICA"):
                        # Simulate database rebalance data movement
                        self.total_keys_migrated += 50
                        # Balance out: add a virtual balancer node or adjust routing rules
                        if action.action_type == "L2_SPLIT" and len(self.shard_latencies) < 8:
                            new_name = f"shard_split_{len(self.shard_latencies)}"
                            # Register split node
                            self.apply_event(ClusterChangeEvent(step, "ADD_NODE", new_name))

            # 4. Auto-tune database knobs using DRL
            if policy == "adaptive" and self.drl_tuner and step_latencies:
                avg_lat = sum(step_latencies) / len(step_latencies)
                throughput = len(step_latencies)
                
                # Reward is high when throughput is high and latency is low
                reward = (throughput / 100.0) - (avg_lat / 50.0)
                
                new_knobs, loss = self.drl_tuner.update(
                    latency_ms=avg_lat,
                    throughput_qps=float(throughput),
                    current_knobs=self.knob_state,
                    reward=reward,
                )
                self.knob_state = new_knobs

            # Compile step stats
            avg_step_lat = sum(step_latencies) / len(step_latencies) if step_latencies else 0.0
            
            # Calculate imbalance ratio (max load / average load)
            loads = [shard_queries_in_step[s] for s in self.shard_latencies if s not in self.failed_shards]
            avg_load = sum(loads) / len(loads) if loads else 1.0
            max_load = max(loads) if loads else 0.0
            imbalance = max_load / avg_load if avg_load > 0 else 1.0

            step_metrics.append({
                "step": step,
                "throughput": len(step_ops) - step_errors,
                "errors": step_errors,
                "avg_latency": avg_step_lat,
                "imbalance_ratio": imbalance,
            })

        # Compile final stats summary
        avg_lat = sum(overall_latencies) / len(overall_latencies) if overall_latencies else 0.0
        p99_lat = sorted(overall_latencies)[int(len(overall_latencies) * 0.99)] if overall_latencies else 0.0

        return {
            "policy": policy,
            "total_queries": overall_queries,
            "total_errors": overall_errors,
            "avg_latency": avg_lat,
            "p99_latency": p99_lat,
            "total_keys_migrated": self.total_keys_migrated,
            "average_imbalance_ratio": sum(m["imbalance_ratio"] for m in step_metrics) / len(step_metrics) if step_metrics else 1.0,
            "step_metrics": step_metrics,
        }
