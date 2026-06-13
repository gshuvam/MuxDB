from __future__ import annotations

import random
from typing import List, Dict, Any, Tuple


class SimpleMLP:
    """
    A lightweight Multi-Layer Perceptron (MLP) implemented in pure Python
    for neural function approximation in Reinforcement Learning.
    Supports a single hidden layer with ReLU activation and MSE loss backpropagation.
    """

    def __init__(self, input_dim: int, hidden_dim: int, output_dim: int, learning_rate: float = 0.01) -> None:
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.output_dim = output_dim
        self.lr = learning_rate

        # Initialize weights and biases randomly using Xavier/He-like distribution
        scale_1 = (2.0 / input_dim) ** 0.5
        self.W1 = [[random.gauss(0.0, 1.0) * scale_1 for _ in range(input_dim)] for _ in range(hidden_dim)]
        self.b1 = [0.0] * hidden_dim

        scale_2 = (2.0 / hidden_dim) ** 0.5
        self.W2 = [[random.gauss(0.0, 1.0) * scale_2 for _ in range(hidden_dim)] for _ in range(output_dim)]
        self.b2 = [0.0] * output_dim

    def forward(self, x: List[float]) -> Tuple[List[float], List[float], List[float]]:
        """
        Forward pass.
        Returns:
            Tuple: (output_Q_values, hidden_activations, pre_activation_hidden)
        """
        # Input to Hidden Layer
        z1 = [0.0] * self.hidden_dim
        h = [0.0] * self.hidden_dim
        for i in range(self.hidden_dim):
            z1[i] = sum(x[j] * self.W1[i][j] for j in range(self.input_dim)) + self.b1[i]
            h[i] = max(0.0, z1[i])  # ReLU activation

        # Hidden to Output Layer
        out = [0.0] * self.output_dim
        for i in range(self.output_dim):
            out[i] = sum(h[j] * self.W2[i][j] for j in range(self.hidden_dim)) + self.b2[i]

        return out, h, z1

    def backward(self, x: List[float], action_idx: int, target_q: float, h: List[float], z1: List[float], out: List[float]) -> float:
        """
        Backward pass (SGD update) targeting a single action value (Q-learning).
        Returns:
            float: MSE loss for this update step.
        """
        # Calculate loss (MSE) and output gradient
        error = out[action_idx] - target_q
        loss = 0.5 * (error ** 2)

        # Gradient at output layer
        dy = [0.0] * self.output_dim
        dy[action_idx] = error  # Only backpropagate error for the chosen action

        # Gradients for W2 and b2
        dW2 = [[0.0] * self.hidden_dim for _ in range(self.output_dim)]
        db2 = [0.0] * self.output_dim

        db2[action_idx] = dy[action_idx]
        for j in range(self.hidden_dim):
            dW2[action_idx][j] = dy[action_idx] * h[j]

        # Backpropagate to hidden layer
        dh = [0.0] * self.hidden_dim
        for j in range(self.hidden_dim):
            dh[j] = sum(dy[i] * self.W2[i][j] for i in range(self.output_dim))

        # Gradient through ReLU activation
        dz1 = [0.0] * self.hidden_dim
        for j in range(self.hidden_dim):
            dz1[j] = dh[j] if z1[j] > 0.0 else 0.0

        # Gradients for W1 and b1
        dW1 = [[0.0] * self.input_dim for _ in range(self.hidden_dim)]
        db1 = [0.0] * self.hidden_dim

        for i in range(self.hidden_dim):
            db1[i] = dz1[i]
            for j in range(self.input_dim):
                dW1[i][j] = dz1[i] * x[j]

        # Update weights and biases using SGD
        for i in range(self.output_dim):
            self.b2[i] -= self.lr * db2[i]
            for j in range(self.hidden_dim):
                self.W2[i][j] -= self.lr * dW2[i][j]

        for i in range(self.hidden_dim):
            self.b1[i] -= self.lr * db1[i]
            for j in range(self.input_dim):
                self.W1[i][j] -= self.lr * dW1[i][j]

        return loss


class DRLTuner:
    """
    DRLTuner implements database instance knob auto-tuning (QTune/UDO style)
    using Deep Q-Learning with neural function approximation in pure Python.
    """

    def __init__(
        self,
        knob_names: List[str],
        knob_bounds: Dict[str, Tuple[float, float]],
        learning_rate: float = 0.01,
        gamma: float = 0.9,
        epsilon: float = 0.1,
    ) -> None:
        """
        Args:
            knob_names: List of database knob names (e.g., ["shared_buffers", "work_mem"])
            knob_bounds: Dict mapping knob name to (min_value, max_value) tuple
            learning_rate: Learning rate for neural network updates
            gamma: Discount factor for Q-learning
            epsilon: Epsilon-greedy exploration rate
        """
        self.knob_names = list(knob_names)
        self.knob_bounds = knob_bounds
        self.gamma = gamma
        self.epsilon = epsilon

        # State includes: [avg_latency, qps, *current_knob_values_normalized]
        self.state_dim = 2 + len(knob_names)
        
        # Action space: for each knob, we can either: -10% (0), +0% (1), or +10% (2) of its range
        # We index the combinations. To keep it simple, we have 2 * len(knobs) + 1 actions:
        # Action index 0: NO_OP
        # Action index 2k+1: decrease knob k by 10%
        # Action index 2k+2: increase knob k by 10%
        self.action_space = ["NO_OP"]
        for knob in knob_names:
            self.action_space.append(f"DECREASE_{knob}")
            self.action_space.append(f"INCREASE_{knob}")
        
        self.output_dim = len(self.action_space)

        # Initialize lightweight neural network model
        self.model = SimpleMLP(
            input_dim=self.state_dim,
            hidden_dim=8,
            output_dim=self.output_dim,
            learning_rate=learning_rate,
        )

        self.last_state: Optional[List[float]] = None
        self.last_action_idx: Optional[int] = None
        self.last_q_values: Optional[List[float]] = None
        self.last_h: Optional[List[float]] = None
        self.last_z1: Optional[List[float]] = None

    def calculate_reward(self, latency_ms: float, throughput_qps: float, cost_utilization: float) -> float:
        """
        Reward function based on UDO/QTune: minimize latency, maximize throughput, minimize resource cost.
        """
        # Higher reward is better
        reward = (throughput_qps / 100.0) - (latency_ms / 50.0) - (cost_utilization * 1.5)
        return reward

    def select_action(self, state: List[float]) -> int:
        """Select action using epsilon-greedy policy."""
        if random.random() < self.epsilon:
            return random.randint(0, self.output_dim - 1)

        q_vals, _, _ = self.model.forward(state)
        # Find index of max Q-value
        best_idx = 0
        max_val = q_vals[0]
        for i in range(1, self.output_dim):
            if q_vals[i] > max_val:
                max_val = q_vals[i]
                best_idx = i
        return best_idx

    def apply_action(self, current_knobs: Dict[str, float], action_idx: int) -> Dict[str, float]:
        """
        Calculate new knob values based on selected action.
        Clamps values to configuration bounds.
        """
        new_knobs = dict(current_knobs)
        if action_idx == 0:  # NO_OP
            return new_knobs

        # Decode action
        knob_idx = (action_idx - 1) // 2
        is_increase = (action_idx - 1) % 2 == 1
        knob_name = self.knob_names[knob_idx]

        min_val, max_val = self.knob_bounds[knob_name]
        step = (max_val - min_val) * 0.1  # 10% steps
        
        current_val = current_knobs.get(knob_name, min_val)
        if is_increase:
            new_val = current_val + step
        else:
            new_val = current_val - step

        # Clamp value to bounds
        new_knobs[knob_name] = max(min_val, min(max_val, new_val))
        return new_knobs

    def normalize_state(self, latency_ms: float, throughput_qps: float, current_knobs: Dict[str, float]) -> List[float]:
        """Normalize environment inputs into state vector."""
        state = [
            min(1.0, latency_ms / 1000.0),  # Clamped relative latency
            min(1.0, throughput_qps / 10000.0),  # Clamped relative throughput
        ]
        for knob in self.knob_names:
            min_val, max_val = self.knob_bounds[knob]
            val = current_knobs.get(knob, min_val)
            normalized = (val - min_val) / (max_val - min_val + 1e-9)
            state.append(normalized)
            
        return state

    def update(
        self,
        latency_ms: float,
        throughput_qps: float,
        current_knobs: Dict[str, float],
        reward: float,
    ) -> Tuple[Dict[str, float], float]:
        """
        DQN update step. Trains on last step, chooses and applies next action.
        Returns:
            Tuple: (new_knobs, loss)
        """
        current_state = self.normalize_state(latency_ms, throughput_qps, current_knobs)
        loss = 0.0

        # Perform training step if we have a prior memory
        if self.last_state is not None and self.last_action_idx is not None:
            # Predict Q values for current state to compute temporal difference target
            next_q_vals, _, _ = self.model.forward(current_state)
            max_next_q = max(next_q_vals)
            
            # Bellman Equation target
            target_q = reward + self.gamma * max_next_q
            
            # Backpropagation using stored feedforward values
            loss = self.model.backward(
                x=self.last_state,
                action_idx=self.last_action_idx,
                target_q=target_q,
                h=self.last_h,  # type: ignore
                z1=self.last_z1,  # type: ignore
                out=self.last_q_values,  # type: ignore
            )

        # Select next action
        action_idx = self.select_action(current_state)
        
        # Store state for next step training
        q_vals, h, z1 = self.model.forward(current_state)
        self.last_state = current_state
        self.last_action_idx = action_idx
        self.last_q_values = q_vals
        self.last_h = h
        self.last_z1 = z1

        # Apply action
        new_knobs = self.apply_action(current_knobs, action_idx)
        return new_knobs, loss
