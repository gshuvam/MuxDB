export class TokenBucketRateLimiter {
  private tokens: number;
  private lastRefill: number = Date.now();

  constructor(
    private readonly rate: number, // Replenish rate per second
    private readonly capacity: number
  ) {
    this.tokens = capacity;
  }

  private refill(): void {
    const now = Date.now();
    const elapsed = (now - this.lastRefill) / 1000;
    if (elapsed > 0) {
      const refilled = elapsed * this.rate;
      this.tokens = Math.min(this.capacity, this.tokens + refilled);
      this.lastRefill = now;
    }
  }

  public acquire(tokens: number = 1): boolean {
    /** Attempt to acquire tokens. Returns true if allowed, false otherwise. */
    this.refill();
    if (this.tokens >= tokens) {
      this.tokens -= tokens;
      return true;
    }
    return false;
  }
}
