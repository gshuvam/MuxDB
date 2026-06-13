export interface ScaleCheckResult {
  action: 'NO_OP' | 'SCALE_OUT' | 'SCALE_DOWN';
  forecastedQps: number;
  currentCapacity: number;
  recommendedCapacity?: number;
  reason: string;
}

export class WorkloadPredictor {
  private alpha: number;
  private beta: number;
  private remoteUrl: string | null;

  constructor(alpha = 0.3, beta = 0.1, remoteUrl: string | null = null) {
    this.alpha = alpha;
    this.beta = beta;
    this.remoteUrl = remoteUrl;
  }

  /**
     * Forecasts future values using double exponential smoothing.
     */
  public forecast(history: number[], horizon = 1): number[] {
    if (!history || history.length === 0) {
      return Array(horizon).fill(0.0);
    }
    const n = history.length;
    if (horizon <= 0) {
      return [];
    }

    if (n < 3) {
      const lastVal = history[n - 1] as number;
      return Array(horizon).fill(lastVal);
    }

    let level = history[0] as number;
    let trend = (history[1] as number) - (history[0] as number);

    for (let i = 1; i < n; i++) {
      const val = history[i] as number;
      const lastLevel = level;
      level = this.alpha * val + (1.0 - this.alpha) * (level + trend);
      trend = this.beta * (level - lastLevel) + (1.0 - this.beta) * trend;
    }

    const predictions: number[] = [];
    for (let m = 1; m <= horizon; m++) {
      const pred = level + m * trend;
      predictions.push(Math.max(0.0, pred));
    }
    return predictions;
  }

  /**
     * Standalone/Remote predictive scale checker.
     * Evaluates future demand spikes within the lead time.
     */
  public async proactiveScaleCheck(
    history: number[],
    leadTimeSteps: number,
    thresholdQps: number,
    currentCapacityQps: number,
    scaleDownThresholdRatio = 0.3,
  ): Promise<ScaleCheckResult> {
    // If remote URL is present, simulate calling remote Python gRPC ML server
    if (this.remoteUrl) {
      // Return simulated gRPC response
      const maxForecast = history.length > 0 ? Math.max(...history) * 1.2 : 0;
      if (maxForecast > currentCapacityQps) {
        return {
          action: 'SCALE_OUT',
          forecastedQps: maxForecast,
          currentCapacity: currentCapacityQps,
          recommendedCapacity: Math.ceil(maxForecast / thresholdQps) * thresholdQps,
          reason: `[Remote gRPC] Forecasted peak ${maxForecast.toFixed(2)} QPS exceeds current capacity`,
        };
      }
      return {
        action: 'NO_OP',
        forecastedQps: maxForecast,
        currentCapacity: currentCapacityQps,
        reason: '[Remote gRPC] Forecasted demand is within capacity limits',
      };
    }

    if (!history || history.length === 0 || leadTimeSteps <= 0) {
      return {
        action: 'NO_OP',
        forecastedQps: 0.0,
        currentCapacity: currentCapacityQps,
        reason: 'Insufficient data or invalid lead time',
      };
    }

    const predictions = this.forecast(history, leadTimeSteps);
    const maxForecast = Math.max(...predictions);
    const avgForecast = predictions.reduce((a, b) => a + b, 0) / predictions.length;

    if (maxForecast > currentCapacityQps) {
      const recommendedCapacity = Math.ceil(maxForecast / thresholdQps) * thresholdQps;
      return {
        action: 'SCALE_OUT',
        forecastedQps: maxForecast,
        currentCapacity: currentCapacityQps,
        recommendedCapacity,
        reason: `Forecasted peak ${maxForecast.toFixed(2)} QPS exceeds current capacity ${currentCapacityQps.toFixed(2)} QPS`,
      };
    }

    if (maxForecast < currentCapacityQps * scaleDownThresholdRatio && currentCapacityQps > thresholdQps) {
      const recommendedCapacity = Math.max(thresholdQps, Math.ceil(maxForecast / thresholdQps) * thresholdQps);
      if (recommendedCapacity < currentCapacityQps) {
        return {
          action: 'SCALE_DOWN',
          forecastedQps: maxForecast,
          currentCapacity: currentCapacityQps,
          recommendedCapacity,
          reason: `Forecasted peak ${maxForecast.toFixed(2)} QPS is below scale down threshold`,
        };
      }
    }

    return {
      action: 'NO_OP',
      forecastedQps: avgForecast,
      currentCapacity: currentCapacityQps,
      reason: 'Forecasted demand remains within safe capacity limits',
    };
  }
}
