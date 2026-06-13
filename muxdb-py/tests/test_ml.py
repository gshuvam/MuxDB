from __future__ import annotations

import unittest
import math
from muxdb.ml import (
    WorkloadPredictor,
    PageHinkleyDetector,
    BanditSolver,
    DRLTuner,
    SimpleMLP,
    SyntheticGenerator,
)


class TestMLControlPlane(unittest.TestCase):

    # --- Predictor Tests ---

    def test_predictor_exponential_smoothing_fallback(self) -> None:
        # Create predictor with seasonal periods = 24
        predictor = WorkloadPredictor(alpha=0.3, beta=0.1, seasonal_periods=24)
        
        # Test with short history (< 2 * seasonal_periods)
        history = [10.0, 12.0, 14.0, 16.0, 18.0]
        forecast = predictor.forecast(history, horizon=3)
        
        # Double exponential smoothing should capture upward trend
        self.assertEqual(len(forecast), 3)
        self.assertTrue(forecast[0] > 18.0)
        self.assertTrue(forecast[2] > forecast[0])

    def test_predictor_holt_winters(self) -> None:
        # Create predictor with short seasonality for testing
        predictor = WorkloadPredictor(alpha=0.2, beta=0.1, gamma=0.3, seasonal_periods=4)
        
        # Pattern repeats every 4 steps with general upward trend
        history = [
            10.0, 12.0, 8.0, 15.0,  # Season 1
            11.0, 13.0, 9.0, 16.0,  # Season 2
            12.0, 14.0, 10.0, 17.0, # Season 3
        ]
        
        forecast = predictor.forecast(history, horizon=4)
        self.assertEqual(len(forecast), 4)
        # Should forecast pattern + trend: index 0 (which maps to 10/11/12 pattern) should be ~13.0
        self.assertTrue(11.0 < forecast[0] < 15.0)

    def test_predictor_proactive_scale_check(self) -> None:
        predictor = WorkloadPredictor(alpha=0.3, beta=0.1, seasonal_periods=5)
        
        # Case 1: Forecasted peak exceeds current capacity
        history_high = [100.0, 120.0, 140.0, 160.0, 180.0, 200.0]
        # forecasted peak should be > 200 QPS
        check_out = predictor.proactive_scale_check(
            history=history_high,
            lead_time_steps=3,
            threshold_qps=50.0,
            current_capacity_qps=150.0,
        )
        self.assertEqual(check_out["action"], "SCALE_OUT")
        self.assertTrue(check_out["recommended_capacity"] > 150.0)

        # Case 2: Forecasted peak is within limits
        history_stable = [100.0, 100.0, 100.0, 100.0, 100.0]
        check_noop = predictor.proactive_scale_check(
            history=history_stable,
            lead_time_steps=3,
            threshold_qps=50.0,
            current_capacity_qps=150.0,
        )
        self.assertEqual(check_noop["action"], "NO_OP")

        # Case 3: Forecasted peak warrants scale down
        history_low = [10.0, 12.0, 10.0, 8.0, 10.0]
        check_down = predictor.proactive_scale_check(
            history=history_low,
            lead_time_steps=3,
            threshold_qps=50.0,
            current_capacity_qps=150.0,
        )
        self.assertEqual(check_down["action"], "SCALE_DOWN")
        self.assertTrue(check_down["recommended_capacity"] < 150.0)

    # --- Bandit Tests ---

    def test_page_hinkley_detector(self) -> None:
        # Page-Hinkley with low threshold for fast testing
        detector = PageHinkleyDetector(delta=0.1, threshold=2.0)
        
        # Feeding normal stream
        for _ in range(20):
            self.assertFalse(detector.update(1.0))
            
        # Abrupt shift in reward
        detected = False
        for _ in range(20):
            if detector.update(5.0):
                detected = True
                break
        self.assertTrue(detected)

    def test_bandit_solver_adaptation(self) -> None:
        bandit = BanditSolver(
            arms=["shard-0", "shard-1"],
            exploration_constant=1.0,
            change_threshold=2.0,
            reset_mode="soft",
        )
        
        # Pull each arm
        arm_a = bandit.select_arm()
        bandit.update_reward(arm_a, 1.0)
        arm_b = bandit.select_arm()
        bandit.update_reward(arm_b, 1.0)
        
        # Test contextual biasing
        context_read = {"type": "read"}
        # shard-0-read would get a bonus if registered, let's verify normal selection
        chosen = bandit.select_arm(context_read)
        self.assertIn(chosen, ["shard-0", "shard-1"])

        # Feed shifted rewards to trigger change detector
        bandit.update_reward("shard-0", 1.0)
        
        # Shift
        change_triggered = False
        for _ in range(50):
            if bandit.update_reward("shard-0", 10.0):
                change_triggered = True
                break
                
        self.assertTrue(change_triggered)
        # Verify counts were decayed
        self.assertTrue(bandit.total_pulls < 52)  # would be 52+ without reset

    # --- DRL Tuner Tests ---

    def test_simple_mlp_forward_backward(self) -> None:
        mlp = SimpleMLP(input_dim=3, hidden_dim=4, output_dim=2, learning_rate=0.1)
        
        x = [0.5, 0.2, 0.8]
        out, h, z1 = mlp.forward(x)
        self.assertEqual(len(out), 2)
        self.assertEqual(len(h), 4)
        
        # Test backprop loss updates weights
        w1_before = [[val for val in row] for row in mlp.W1]
        loss = mlp.backward(x, action_idx=1, target_q=5.0, h=h, z1=z1, out=out)
        self.assertTrue(loss > 0.0)
        
        # Weights should change
        w1_after = mlp.W1
        self.assertNotEqual(w1_before, w1_after)

    def test_drl_tuner_optimization(self) -> None:
        bounds = {"shared_buffers": (128.0, 1024.0), "work_mem": (4.0, 64.0)}
        tuner = DRLTuner(
            knob_names=["shared_buffers", "work_mem"],
            knob_bounds=bounds,
            learning_rate=0.05,
            gamma=0.9,
            epsilon=0.0, # Greedy selection for deterministic testing
        )
        
        current_knobs = {"shared_buffers": 256.0, "work_mem": 16.0}
        
        # Run tuner update loop
        new_knobs, loss = tuner.update(
            latency_ms=10.0,
            throughput_qps=500.0,
            current_knobs=current_knobs,
            reward=1.0,
        )
        
        self.assertEqual(len(new_knobs), 2)
        # Ensure values are within bounds
        self.assertTrue(bounds["shared_buffers"][0] <= new_knobs["shared_buffers"] <= bounds["shared_buffers"][1])
        self.assertTrue(bounds["work_mem"][0] <= new_knobs["work_mem"] <= bounds["work_mem"][1])

    # --- Synthetic Workload Generator Tests ---

    def test_synthetic_generator(self) -> None:
        generator = SyntheticGenerator(
            num_keys=100,
            zipf_alpha=1.2,
            write_ratio=0.3,
            tenants=["t1", "t2"],
        )
        
        # Verify next key retrieval
        zipf_key = generator.next_zipf_key()
        self.assertTrue(zipf_key.startswith("key_"))
        
        pareto_key = generator.next_pareto_key()
        self.assertTrue(pareto_key.startswith("key_"))

        # Verify operation contents
        op = generator.next_op()
        self.assertIn(op["op"], ["read", "write"])
        self.assertTrue(op["key"].startswith("key_"))
        self.assertIn(op["tenant_id"], ["t1", "t2"])

        # Verify workload generation list
        workload = generator.generate_workload(size=10)
        self.assertEqual(len(workload), 10)

        # Bootstrap bandit test
        bandit = BanditSolver(arms=["s0", "s1"], change_threshold=5.0)
        rewards = generator.bootstrap_bandit(bandit, steps=10)
        self.assertEqual(len(rewards), 10)
        self.assertTrue(bandit.total_pulls > 0)


if __name__ == "__main__":
    unittest.main()
