export class PageHinkleyDetector {
  private delta: number;
  private threshold: number;
  private alpha: number;
  private count = 0;
  private mean = 0.0;
  private sumHigh = 0.0;
  private sumLow = 0.0;

  constructor(delta = 0.005, threshold = 10.0, alpha = 0.999) {
    this.delta = delta;
    this.threshold = threshold;
    this.alpha = alpha;
    this.reset();
  }

  public reset(): void {
    this.count = 0;
    this.mean = 0.0;
    this.sumHigh = 0.0;
    this.sumLow = 0.0;
  }

  public update(x: number): boolean {
    this.count++;

    if (this.count === 1) {
      this.mean = x;
    } else {
      this.mean = this.alpha * this.mean + (1.0 - this.alpha) * x;
    }

    this.sumHigh = Math.max(0.0, this.sumHigh + (x - this.mean - this.delta));
    this.sumLow = Math.max(0.0, this.sumLow + (this.mean - x - this.delta));

    if (this.sumHigh > this.threshold || this.sumLow > this.threshold) {
      this.reset();
      return true;
    }

    return false;
  }
}

export class BanditSolver {
  public arms: string[];
  private explorationConstant: number;
  private resetMode: 'hard' | 'soft';
  private decayFactor: number;
  private counts: Record<string, number> = {};
  private rewards: Record<string, number> = {};
  private totalPulls = 0.0;
  private detector: PageHinkleyDetector;
  private remoteUrl: string | null;

  constructor(
    arms: string[],
    explorationConstant = 2.0,
    changeThreshold = 5.0,
    resetMode: 'hard' | 'soft' = 'soft',
    decayFactor = 0.5,
    remoteUrl: string | null = null,
  ) {
    this.arms = [...arms];
    this.explorationConstant = explorationConstant;
    this.resetMode = resetMode;
    this.decayFactor = decayFactor;
    this.remoteUrl = remoteUrl;

    for (const arm of arms) {
      this.counts[arm] = 0.0;
      this.rewards[arm] = 0.0;
    }

    this.detector = new PageHinkleyDetector(0.005, changeThreshold);
  }

  public selectArm(context?: Record<string, any>): string {
    // If remote connection exists, simulate fetching optimal placement from Python
    if (this.remoteUrl) {
      return this.arms[0] || '';
    }

    // Ensure all arms are pulled at least once
    for (const arm of this.arms) {
      if ((this.counts[arm] ?? 0) === 0) {
        return arm;
      }
    }

    let bestArm = this.arms[0] as string;
    let maxValue = -Infinity;

    for (const arm of this.arms) {
      const averageReward = this.rewards[arm] ?? 0.0;
      const count = this.counts[arm] ?? 0.0;

      let contextBonus = 0.0;
      if (context && context.type) {
        if (context.type === 'write' && arm.endsWith('-write')) {
          contextBonus = 0.5;
        } else if (context.type === 'read' && arm.endsWith('-read')) {
          contextBonus = 0.5;
        }
      }

      const confidenceInterval = this.explorationConstant * Math.sqrt(
        Math.log(this.totalPulls) / count,
      );
      const ucbValue = averageReward + confidenceInterval + contextBonus;

      if (ucbValue > maxValue) {
        maxValue = ucbValue;
        bestArm = arm;
      }
    }

    return bestArm;
  }

  public updateReward(arm: string, reward: number): boolean {
    if (!(arm in this.counts)) {
      this.arms.push(arm);
      this.counts[arm] = 0.0;
      this.rewards[arm] = 0.0;
    }

    this.counts[arm] = (this.counts[arm] ?? 0.0) + 1.0;
    this.totalPulls += 1.0;

    const n = this.counts[arm] as number;
    const currentReward = this.rewards[arm] ?? 0.0;
    this.rewards[arm] = ((n - 1.0) / n) * currentReward + (1.0 / n) * reward;

    const changeDetected = this.detector.update(reward);

    if (changeDetected) {
      this.handleChange();
      return true;
    }

    return false;
  }

  private handleChange(): void {
    if (this.resetMode === 'hard') {
      for (const arm of this.arms) {
        this.counts[arm] = 0.0;
        this.rewards[arm] = 0.0;
      }
      this.totalPulls = 0.0;
    } else {
      this.totalPulls = 0.0;
      for (const arm of this.arms) {
        const c = this.counts[arm] ?? 0.0;
        this.counts[arm] = Math.max(1.0, c * this.decayFactor);
        this.totalPulls += this.counts[arm] as number;
      }
    }
  }
}
