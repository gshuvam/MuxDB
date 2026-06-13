import { CircuitOpenError } from '../errors';

export enum State {
  CLOSED,
  OPEN,
  HALF_OPEN
}

export class CircuitBreaker {
  private state: State = State.CLOSED;
  private failureCount: number = 0;
  private lastStateChange: number = Date.now();

  constructor(
    public readonly shardId: string,
    private readonly failureThreshold: number = 5,
    private readonly recoveryTimeoutMs: number = 10000
  ) {}

  private onSuccess(): void {
    if (this.state === State.HALF_OPEN) {
      this.state = State.CLOSED;
      this.failureCount = 0;
      this.lastStateChange = Date.now();
    } else if (this.state === State.CLOSED) {
      this.failureCount = 0;
    }
  }

  private onFailure(): void {
    this.failureCount++;
    if (this.state === State.CLOSED && this.failureCount >= this.failureThreshold) {
      this.state = State.OPEN;
      this.lastStateChange = Date.now();
    } else if (this.state === State.HALF_OPEN) {
      this.state = State.OPEN;
      this.lastStateChange = Date.now();
    }
  }

  public checkState(): void {
    if (this.state === State.OPEN) {
      const elapsed = Date.now() - this.lastStateChange;
      if (elapsed >= this.recoveryTimeoutMs) {
        this.state = State.HALF_OPEN;
        this.lastStateChange = Date.now();
      } else {
        throw new CircuitOpenError(this.shardId, {
          recoveryAfterS: (this.recoveryTimeoutMs - elapsed) / 1000,
        });
      }
    }
  }

  public async call<T>(fn: () => Promise<T>): Promise<T> {
    this.checkState();
    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }
}
