from __future__ import annotations

import unittest
from muxdb.evaluator import ClusterChangeEvent, WorkloadScenario, compare_policies


class TestEvaluationHarness(unittest.TestCase):

    def setUp(self) -> None:
        self.scenario = WorkloadScenario(num_keys=100)

    # --- Scenario Generator Tests ---

    def test_stable_baseline(self) -> None:
        workload = self.scenario.stable_baseline(steps=5, qps=10)
        self.assertEqual(len(workload), 5)
        for step_ops in workload:
            self.assertEqual(len(step_ops), 10)
            for op in step_ops:
                self.assertIn(op["op"], ["read", "write"])
                self.assertTrue(op["key"].startswith("key_"))

    def test_point_hotspot(self) -> None:
        hotspot_key = "key_viral"
        workload = self.scenario.point_hotspot(
            steps=10,
            qps=20,
            hotspot_key=hotspot_key,
            hotspot_start_step=4,
            hotspot_ratio=0.8,
        )
        self.assertEqual(len(workload), 10)
        
        # Before step 4: no viral key should be present
        for step in range(4):
            for op in workload[step]:
                self.assertNotEqual(op["key"], hotspot_key)

        # After/at step 4: viral key should emerge
        hotspot_count = 0
        total_count = 0
        for step in range(4, 10):
            for op in workload[step]:
                total_count += 1
                if op["key"] == hotspot_key:
                    hotspot_count += 1
                    
        self.assertTrue(hotspot_count > 0)
        self.assertTrue(hotspot_count / total_count > 0.5)

    def test_diurnal_spikes(self) -> None:
        workload = self.scenario.diurnal_spikes(steps=20, base_qps=30, amplitude=15, period_steps=10)
        self.assertEqual(len(workload), 20)
        
        # Verify sizes vary
        sizes = [len(batch) for batch in workload]
        self.assertTrue(max(sizes) > min(sizes))

    def test_mixed_oltp_olap(self) -> None:
        workload = self.scenario.mixed_oltp_olap(steps=12, qps=10, olap_start_step=2, scan_size=5)
        self.assertEqual(len(workload), 12)
        
        # Step 10 should have OLAP scan operations
        step_10_scans = [op for op in workload[10] if op.get("is_scan")]
        self.assertEqual(len(step_10_scans), 5)

    # --- Runner & Comparison Simulation Tests ---

    def test_simulation_runner_and_comparison(self) -> None:
        shard_ids = ["s0", "s1", "s2"]
        workload = self.scenario.stable_baseline(steps=5, qps=10)
        
        events = [
            ClusterChangeEvent(step=2, action_type="SLOW_NODE", target_shard_id="s1", parameter=10.0),
            ClusterChangeEvent(step=3, action_type="FAIL_NODE", target_shard_id="s2"),
        ]
        
        results = compare_policies(shard_ids, workload, events)
        
        # We expect entries for static, reactive, and adaptive
        self.assertIn("static", results)
        self.assertIn("reactive", results)
        self.assertIn("adaptive", results)

        for policy, res in results.items():
            self.assertEqual(res["policy"], policy)
            self.assertTrue(res["total_queries"] > 0)
            self.assertTrue(res["avg_latency"] > 0.0)
            self.assertTrue(res["p99_latency"] > 0.0)
            self.assertEqual(len(res["step_metrics"]), 5)
            
            # Check step metrics structure
            for m in res["step_metrics"]:
                self.assertIn("step", m)
                self.assertIn("throughput", m)
                self.assertIn("avg_latency", m)
                self.assertIn("imbalance_ratio", m)

        # Reactive or Adaptive should have resolved splits/replication if threshold hit
        # (Since threshold is low in comparison setup, rebalances might be triggered)
        self.assertTrue(results["reactive"]["total_keys_migrated"] >= 0)
        self.assertTrue(results["adaptive"]["total_keys_migrated"] >= 0)


if __name__ == "__main__":
    unittest.main()
