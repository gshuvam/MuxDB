from __future__ import annotations

import copy
from typing import List, Dict, Any, Optional

from muxdb.config import MuxConfig, ClusterConfig, ShardConfig
from muxdb.router import Router
from muxdb.telemetry import TelemetryCollector
from muxdb.balancer import Balancer
from muxdb.ml import BanditSolver, DRLTuner
from muxdb.evaluator.scenario import ClusterChangeEvent, WorkloadScenario
from muxdb.evaluator.runner import SimulationRunner


def compare_policies(
    shard_ids: List[str],
    workload: List[List[Dict[str, Any]]],
    events: Optional[List[ClusterChangeEvent]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Evaluates and compares the static, reactive, and adaptive/predictive routing
    policies against the exact same workload and events.
    """
    results = {}
    
    # 1. Run Static Policy
    results["static"] = _run_policy_simulation(shard_ids, workload, events, "static")

    # 2. Run Reactive Policy (uses Balancer thresholds)
    results["reactive"] = _run_policy_simulation(shard_ids, workload, events, "reactive")

    # 3. Run Adaptive Policy (uses Balancer + Bandit placement + DRL auto-tuning)
    results["adaptive"] = _run_policy_simulation(shard_ids, workload, events, "adaptive")

    return results


def _run_policy_simulation(
    shard_ids: List[str],
    workload: List[List[Dict[str, Any]]],
    events: Optional[List[ClusterChangeEvent]],
    policy: str,
) -> Dict[str, Any]:
    # Construct a fresh router configuration
    shards_config = [
        ShardConfig(
            id=sid,
            backend="postgresql",
            host="localhost",
            port=5432 + idx,
            database=f"db_{sid}",
        )
        for idx, sid in enumerate(shard_ids)
    ]
    
    config = MuxConfig(
        cluster=ClusterConfig(name="sim_cluster", strategy="consistent_hash", shard_key="key"),
        shards=shards_config,
    )
    
    # Instantiate clean router, telemetry, and balancer
    router = Router(config)
    telemetry = TelemetryCollector(window_seconds=10.0)
    balancer = Balancer(cooldown_seconds=0.0)  # Low cooldown to see actions in short simulations

    # Instantiate ML components if policy is adaptive
    bandit_solver = None
    drl_tuner = None
    
    if policy == "adaptive":
        bandit_solver = BanditSolver(arms=list(shard_ids), change_threshold=2.0)
        drl_tuner = DRLTuner(
            knob_names=["shared_buffers", "work_mem"],
            knob_bounds={"shared_buffers": (128.0, 1024.0), "work_mem": (4.0, 64.0)},
            learning_rate=0.01,
        )

    runner = SimulationRunner(
        router=router,
        telemetry=telemetry,
        balancer=balancer,
        bandit_solver=bandit_solver,
        drl_tuner=drl_tuner,
    )

    # Make a deepcopy of workload/events to ensure simulation isolation
    workload_copy = copy.deepcopy(workload)
    events_copy = copy.deepcopy(events) if events else None

    return runner.run(workload_copy, events_copy, policy=policy)
