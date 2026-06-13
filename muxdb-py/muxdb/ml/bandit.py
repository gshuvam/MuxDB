from __future__ import annotations

import math
import random
from typing import List, Dict, Any, Optional


class PageHinkleyDetector:
    """
    Page-Hinkley Change Detector to monitor non-stationary environment rewards.
    Detects abrupt changes in mean values of a series (e.g., reward or latency).
    """

    def __init__(self, delta: float = 0.005, threshold: float = 10.0, alpha: float = 0.999) -> None:
        """
        Args:
            delta: Minimum magnitude of change to detect.
            threshold: Cumulative deviation threshold to trigger a change.
            alpha: Decay factor for the running mean.
        """
        self.delta = delta
        self.threshold = threshold
        self.alpha = alpha
        self.reset()

    def reset(self) -> None:
        """Reset the detector state."""
        self.count = 0
        self.mean = 0.0
        self.sum_high = 0.0
        self.sum_low = 0.0

    def update(self, x: float) -> bool:
        """
        Update the detector with a new sample.
        Returns:
            bool: True if a shift/change is detected, False otherwise.
        """
        self.count += 1
        
        # Incremental running mean with exponential decay
        if self.count == 1:
            self.mean = x
        else:
            self.mean = self.alpha * self.mean + (1.0 - self.alpha) * x

        # Cumulative sums of deviations
        self.sum_high = max(0.0, self.sum_high + (x - self.mean - self.delta))
        self.sum_low = max(0.0, self.sum_low + (self.mean - x - self.delta))

        # Check threshold
        if self.sum_high > self.threshold or self.sum_low > self.threshold:
            self.reset()
            return True

        return False


class BanditSolver:
    """
    BanditSolver implements the AW-CUCB-DP non-stationary bandit algorithm
    for optimal key/shard placement adaptation.
    """

    def __init__(
        self,
        arms: List[str],
        exploration_constant: float = 2.0,
        change_threshold: float = 5.0,
        reset_mode: str = "soft",
        decay_factor: float = 0.5,
    ) -> None:
        """
        Args:
            arms: List of target shard/node IDs.
            exploration_constant: Controls exploration (c parameter in UCB).
            change_threshold: Sensitivity threshold for Page-Hinkley change detector.
            reset_mode: "hard" (reset all counts to 0) or "soft" (decay counts).
            decay_factor: Multiplier to apply to counts when change is detected in "soft" mode.
        """
        self.arms = list(arms)
        self.exploration_constant = exploration_constant
        self.reset_mode = reset_mode
        self.decay_factor = decay_factor

        self.counts: Dict[str, float] = {arm: 0.0 for arm in arms}
        self.rewards: Dict[str, float] = {arm: 0.0 for arm in arms}
        self.total_pulls = 0.0

        # Change detector monitoring overall reward trend
        self.detector = PageHinkleyDetector(threshold=change_threshold)

    def select_arm(self, context: Optional[Dict[str, Any]] = None) -> str:
        """
        Selects an arm (shard ID) using the UCB algorithm.
        Supports simple context routing where context features bias initial preferences.
        """
        # Ensure all arms are pulled at least once
        for arm in self.arms:
            if self.counts[arm] == 0:
                return arm

        best_arm = self.arms[0]
        max_value = -float("inf")

        for arm in self.arms:
            average_reward = self.rewards[arm]
            
            # Contextual adjustment if present
            context_bonus = 0.0
            if context and "type" in context:
                # E.g., if context is write-heavy, bias towards nodes with high write capacity
                if context["type"] == "write" and arm.endswith("-write"):
                    context_bonus = 0.5
                elif context["type"] == "read" and arm.endswith("-read"):
                    context_bonus = 0.5

            # UCB formula
            confidence_interval = self.exploration_constant * math.sqrt(
                math.log(self.total_pulls) / self.counts[arm]
            )
            ucb_value = average_reward + confidence_interval + context_bonus

            if ucb_value > max_value:
                max_value = ucb_value
                best_arm = arm

        return best_arm

    def update_reward(self, arm: str, reward: float) -> bool:
        """
        Update the model with a reward for pulling an arm.
        Returns:
            bool: True if a change was detected and learning state was reset/decayed.
        """
        if arm not in self.counts:
            # Dynamic arm registration
            self.arms.append(arm)
            self.counts[arm] = 0.0
            self.rewards[arm] = 0.0

        self.counts[arm] += 1.0
        self.total_pulls += 1.0

        # Update running reward average for the arm
        n = self.counts[arm]
        self.rewards[arm] = ((n - 1.0) / n) * self.rewards[arm] + (1.0 / n) * reward

        # Feed the reward into the Page-Hinkley detector
        change_detected = self.detector.update(reward)

        if change_detected:
            self._handle_change()
            return True

        return False

    def _handle_change(self) -> None:
        """Resets or decays state in response to environment changes."""
        if self.reset_mode == "hard":
            for arm in self.arms:
                self.counts[arm] = 0.0
                self.rewards[arm] = 0.0
            self.total_pulls = 0.0
        else:  # "soft" decay
            for arm in self.arms:
                self.counts[arm] = max(1.0, self.counts[arm] * self.decay_factor)
                # Keep rewards but increase uncertainty by reducing counts
            self.total_pulls = sum(self.counts.values())
