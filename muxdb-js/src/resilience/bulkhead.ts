import { BulkheadLimitExceeded } from '../errors';

export class Bulkhead {
  private activeCount: number = 0;

  constructor(public readonly maxConcurrency: number) {}

  public async call<T>(fn: () => Promise<T>): Promise<T> {
    /** Run a function wrapped inside the bulkhead, checking concurrency boundaries. */
    if (this.activeCount >= this.maxConcurrency) {
      throw new BulkheadLimitExceeded(
        `Bulkhead concurrency limit of ${this.maxConcurrency} exceeded.`
      );
    }
    this.activeCount++;
    try {
      return await fn();
    } finally {
      this.activeCount--;
    }
  }
}
