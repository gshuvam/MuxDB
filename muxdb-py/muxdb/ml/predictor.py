from __future__ import annotations

import math
from typing import List, Dict, Any, Optional


class WorkloadPredictor:
    """
    WorkloadPredictor implements predictive workload forecasting (P-Store style)
    using double and triple (Holt-Winters) exponential smoothing in pure Python.
    """

    def __init__(
        self,
        alpha: float = 0.3,
        beta: float = 0.1,
        gamma: float = 0.2,
        seasonal_periods: int = 24,
    ) -> None:
        """
        Args:
            alpha: Smoothing factor for level (0 < alpha < 1)
            beta: Smoothing factor for trend (0 < beta < 1)
            gamma: Smoothing factor for seasonality (0 < gamma < 1)
            seasonal_periods: Number of steps in a seasonal cycle (e.g., 24 for hourly diurnal cycle)
        """
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.seasonal_periods = seasonal_periods

    def _initial_seasonal_indices(self, history: List[float]) -> List[float]:
        """Calculate initial seasonal indices for Holt-Winters."""
        season_averages = []
        n_seasons = len(history) // self.seasonal_periods
        
        for i in range(n_seasons):
            start = self.seasonal_periods * i
            season_averages.append(
                sum(history[start : start + self.seasonal_periods]) / self.seasonal_periods
            )

        seasonal_indices = [0.0] * self.seasonal_periods
        for i in range(self.seasonal_periods):
            sum_val = 0.0
            for j in range(n_seasons):
                sum_val += history[j * self.seasonal_periods + i] - season_averages[j]
            seasonal_indices[i] = sum_val / n_seasons
            
        return seasonal_indices

    def _initial_trend(self, history: List[float]) -> float:
        """Calculate initial trend factor."""
        sum_val = 0.0
        for i in range(self.seasonal_periods):
            sum_val += (
                history[i + self.seasonal_periods] - history[i]
            ) / self.seasonal_periods
        return sum_val / self.seasonal_periods

    def forecast(self, history: List[float], horizon: int = 1) -> List[float]:
        """
        Predict future values given a historical time series.
        Falls back to double or single exponential smoothing if history is short.
        """
        if not history:
            return [0.0] * horizon

        n = len(history)
        if horizon <= 0:
            return []

        # Fallback to single exponential smoothing if history is extremely short
        if n < 3:
            last_val = history[-1]
            return [last_val] * horizon

        # Fallback to double exponential smoothing if we have trend but not enough seasonal data
        if n < 2 * self.seasonal_periods:
            return self._double_exponential_smoothing(history, horizon)

        # Triple Exponential Smoothing (Holt-Winters Additive)
        seasonals = self._initial_seasonal_indices(history)
        level = sum(history[: self.seasonal_periods]) / self.seasonal_periods
        trend = self._initial_trend(history)

        # Smoothing historical data
        for i in range(n):
            val = history[i]
            last_level = level
            # Seasonal index index
            s_idx = i % self.seasonal_periods
            
            level = self.alpha * (val - seasonals[s_idx]) + (1 - self.alpha) * (
                level + trend
            )
            trend = self.beta * (level - last_level) + (1 - self.beta) * trend
            seasonals[s_idx] = self.gamma * (val - level) + (1 - self.gamma) * seasonals[s_idx]

        # Forecast future values
        predictions = []
        for m in range(1, horizon + 1):
            s_idx = (n + m - 1) % self.seasonal_periods
            pred = level + m * trend + seasonals[s_idx]
            predictions.append(max(0.0, pred))  # Workload metric cannot be negative

        return predictions

    def _double_exponential_smoothing(self, history: List[float], horizon: int) -> List[float]:
        """Double Exponential Smoothing (Holt's Linear) for non-seasonal forecasting."""
        n = len(history)
        level = history[0]
        trend = history[1] - history[0]

        for i in range(1, n):
            val = history[i]
            last_level = level
            level = self.alpha * val + (1 - self.alpha) * (level + trend)
            trend = self.beta * (level - last_level) + (1 - self.beta) * trend

        predictions = []
        for m in range(1, horizon + 1):
            pred = level + m * trend
            predictions.append(max(0.0, pred))
            
        return predictions

    def proactive_scale_check(
        self,
        history: List[float],
        lead_time_steps: int,
        threshold_qps: float,
        current_capacity_qps: float,
        scale_down_threshold_ratio: float = 0.3,
    ) -> Dict[str, Any]:
        """
        P-Store predictive scale checker. Calculates lead time, forecasts demand,
        and returns recommended scaling actions.
        """
        if not history or lead_time_steps <= 0:
            return {"action": "NO_OP", "forecasted_qps": 0.0, "reason": "Insufficient data/lead time"}

        # Forecast up to lead time steps in the future
        predictions = self.forecast(history, horizon=lead_time_steps)
        max_forecast = max(predictions)
        avg_forecast = sum(predictions) / len(predictions)

        # Check if forecasted QPS exceeds current capacity
        if max_forecast > current_capacity_qps:
            # Recommend Scale Out
            required_capacity = math.ceil(max_forecast / threshold_qps) * threshold_qps
            return {
                "action": "SCALE_OUT",
                "forecasted_qps": max_forecast,
                "current_capacity": current_capacity_qps,
                "recommended_capacity": required_capacity,
                "reason": f"Forecasted peak {max_forecast:.2f} QPS exceeds current capacity {current_capacity_qps:.2f} QPS",
            }

        # Check if forecasted QPS is consistently low (e.g. less than 30% of current capacity)
        if max_forecast < current_capacity_qps * scale_down_threshold_ratio and current_capacity_qps > threshold_qps:
            recommended_capacity = max(threshold_qps, math.ceil(max_forecast / threshold_qps) * threshold_qps)
            if recommended_capacity < current_capacity_qps:
                return {
                    "action": "SCALE_DOWN",
                    "forecasted_qps": max_forecast,
                    "current_capacity": current_capacity_qps,
                    "recommended_capacity": recommended_capacity,
                    "reason": f"Forecasted peak {max_forecast:.2f} QPS is below scale down threshold",
                }

        return {
            "action": "NO_OP",
            "forecasted_qps": avg_forecast,
            "current_capacity": current_capacity_qps,
            "reason": "Forecasted demand remains within safe capacity limits",
        }
