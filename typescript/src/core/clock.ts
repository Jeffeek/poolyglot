/** Time source abstraction so every time-dependent rule is deterministic in tests. */

export interface Clock {
  /** Monotonic time in seconds (only differences are meaningful). */
  monotonic(): number;
  /** Async sleep for `seconds`. */
  asyncSleep(seconds: number): Promise<void>;
}

/** Real time. The default. */
export class SystemClock implements Clock {
  monotonic(): number {
    return performance.now() / 1000;
  }

  asyncSleep(seconds: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, Math.max(0, seconds) * 1000));
  }
}

/** Fake clock for tests: time moves only when you say so; sleeping just advances it. */
export class ManualClock implements Clock {
  private now: number;

  constructor(start = 1000.0) {
    this.now = start;
  }

  monotonic(): number {
    return this.now;
  }

  advance(seconds: number): void {
    this.now += seconds;
  }

  async asyncSleep(seconds: number): Promise<void> {
    this.now += seconds;
    await Promise.resolve();
  }
}
