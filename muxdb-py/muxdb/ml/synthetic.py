from __future__ import annotations

import random
from typing import List, Dict, Any, Optional


class SyntheticGenerator:
    """
    SyntheticGenerator produces Zipfian and Pareto workload distributions
    to simulate database queries (reads/writes) across tenants and keys.
    """

    def __init__(
        self,
        num_keys: int = 1000,
        zipf_alpha: float = 1.1,
        pareto_shape: float = 1.5,
        write_ratio: float = 0.2,
        tenants: Optional[List[str]] = None,
    ) -> None:
        """
        Args:
            num_keys: Size of the key space.
            zipf_alpha: Skew parameter for Zipfian distribution (alpha > 0).
            pareto_shape: Shape parameter for Pareto distribution.
            write_ratio: Proportion of write requests vs reads.
            tenants: List of tenant IDs to map queries to.
        """
        self.num_keys = num_keys
        self.zipf_alpha = zipf_alpha
        self.pareto_shape = pareto_shape
        self.write_ratio = write_ratio
        self.tenants = tenants or ["tenant_default"]

        # Precompute Zipfian CDF for fast O(log N) key generation
        self._zipf_cdf = self._precompute_zipf_cdf()

    def _precompute_zipf_cdf(self) -> List[float]:
        """Precomputes the cumulative distribution function (CDF) for Zipf distribution."""
        # P(i) proportional to 1 / i^alpha
        probabilities = [1.0 / (i ** self.zipf_alpha) for i in range(1, self.num_keys + 1)]
        total = sum(probabilities)
        normalized = [p / total for p in probabilities]
        
        # Cumulative sum
        cdf = []
        cumulative = 0.0
        for p in normalized:
            cumulative += p
            cdf.append(cumulative)
        return cdf

    def next_zipf_key(self) -> str:
        """Draws a key index from Zipfian CDF using binary search."""
        r = random.random()
        
        # Binary search
        low = 0
        high = len(self._zipf_cdf) - 1
        ans = high
        
        while low <= high:
            mid = (low + high) // 2
            if self._zipf_cdf[mid] >= r:
                ans = mid
                high = mid - 1
            else:
                low = mid + 1
                
        return f"key_{ans}"

    def next_pareto_key(self) -> str:
        """Draws a key index from Pareto distribution."""
        # Pareto distribution sample: x = x_min / (r ^ (1 / shape))
        # Map output range into [0, num_keys)
        r = random.random() or 1e-9  # Avoid divide by zero
        sample = 1.0 / (r ** (1.0 / self.pareto_shape))
        
        # Scale and wrap to stay in bounds
        key_idx = int(sample * 100) % self.num_keys
        return f"key_{key_idx}"

    def next_op(self, distribution: str = "zipf") -> Dict[str, Any]:
        """Generates a single database operation."""
        is_write = random.random() < self.write_ratio
        key = self.next_zipf_key() if distribution == "zipf" else self.next_pareto_key()
        tenant = random.choice(self.tenants)

        return {
            "op": "write" if is_write else "read",
            "key": key,
            "tenant_id": tenant,
        }

    def generate_workload(self, size: int, distribution: str = "zipf") -> List[Dict[str, Any]]:
        """Generates a batch workload of simulated database actions."""
        return [self.next_op(distribution) for _ in range(size)]

    def bootstrap_bandit(self, bandit: Any, steps: int) -> List[float]:
        """
        Active learning bootstrap helper. Simulates traffic to train a BanditSolver,
        and returns historical reward trends.
        """
        rewards = []
        
        # Simulate environment state
        # Let's say one arm is "optimal" but it changes halfway to simulate non-stationarity
        optimal_arm_1 = bandit.arms[0]
        optimal_arm_2 = bandit.arms[-1] if len(bandit.arms) > 1 else bandit.arms[0]

        for step in range(steps):
            # Non-stationary environment shift at 50% steps
            optimal_arm = optimal_arm_1 if step < (steps // 2) else optimal_arm_2
            
            # Context input
            context = {"type": "read" if random.random() > self.write_ratio else "write"}
            chosen_arm = bandit.select_arm(context)
            
            # Determine reward
            # Optimal arm gives high reward, other arms give low reward with noise
            if chosen_arm == optimal_arm:
                reward = random.gauss(1.0, 0.2)
            else:
                reward = random.gauss(0.2, 0.2)
                
            bandit.update_reward(chosen_arm, reward)
            rewards.append(reward)

        return rewards
