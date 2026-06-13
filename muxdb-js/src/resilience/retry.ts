export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  baseDelayMs: number = 100,
  maxDelayMs: number = 2000,
  exponentialBase: number = 2
): Promise<T> {
  /** Retry an asynchronous task using exponential backoff with jitter. */
  let lastError: any;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (attempt === maxRetries) {
        break;
      }
      const delay = Math.min(maxDelayMs, baseDelayMs * Math.pow(exponentialBase, attempt));
      const jitter = Math.random() * 0.1 * delay;
      await new Promise((resolve) => setTimeout(resolve, delay + jitter));
    }
  }
  throw lastError || new Error('Retry failed');
}
