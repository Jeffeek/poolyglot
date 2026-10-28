/**
 * Async synchronisation primitives the AsyncConnectionPool needs: an await-able condition
 * variable (the twin of Python's `asyncio.Condition` / C#'s `AsyncCondition`) and a "shield"
 * helper (the twin of `asyncio.shield`). Cancellation is cooperative via `AbortSignal`.
 */

/** Thrown when an operation is cancelled via its {@link AbortSignal}. */
export class CancelledError extends Error {
  constructor(message = "operation cancelled") {
    super(message);
    this.name = "CancelledError";
  }
}

/** The value to throw when `signal` has fired (its `.reason`, or a {@link CancelledError}). */
export function abortError(signal: AbortSignal): unknown {
  return signal.reason ?? new CancelledError();
}

interface LockEntry {
  resolve: () => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal | undefined;
  onAbort?: (() => void) | undefined;
}

interface CondWaiter {
  wake?: () => void;
}

/**
 * Await-able condition variable: owns a mutex, lets a holder release it while waiting to be
 * notified (or time out / be cancelled), then re-acquires it before returning.
 */
export class AsyncCondition {
  private locked = false;
  private readonly lockQueue: LockEntry[] = [];
  private readonly condWaiters = new Set<CondWaiter>();

  /** Acquire the mutex. Rejects (without acquiring) if `signal` fires first. */
  async lock(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) {
      throw abortError(signal);
    }
    if (!this.locked) {
      this.locked = true;
      return;
    }
    await new Promise<void>((resolve, reject) => {
      const entry: LockEntry = { resolve, reject, signal };
      if (signal) {
        entry.onAbort = () => {
          const i = this.lockQueue.indexOf(entry);
          if (i >= 0) {
            this.lockQueue.splice(i, 1);
          }
          reject(abortError(signal));
        };
        signal.addEventListener("abort", entry.onAbort, { once: true });
      }
      this.lockQueue.push(entry);
    });
  }

  /** Release the mutex, handing it to the next queued locker (if any). */
  unlock(): void {
    const next = this.lockQueue.shift();
    if (next) {
      if (next.signal && next.onAbort) {
        next.signal.removeEventListener("abort", next.onAbort);
      }
      next.resolve(); // lock stays held, handed to `next`
    } else {
      this.locked = false;
    }
  }

  /** Reacquire the mutex, never cancellable (disposal/wakeup must always complete). */
  private async reacquire(): Promise<void> {
    if (!this.locked) {
      this.locked = true;
      return;
    }
    await new Promise<void>((resolve, reject) => {
      this.lockQueue.push({ resolve, reject });
    });
  }

  /**
   * Release the lock, wait to be notified (or time out / be cancelled), then re-acquire it.
   * Returns true if notified, false if `timeoutSeconds` elapsed; throws if `signal` fires.
   * Precondition: the caller holds the lock.
   */
  async wait(timeoutSeconds: number | null, signal?: AbortSignal): Promise<boolean> {
    const waiter: CondWaiter = {};
    this.condWaiters.add(waiter);
    this.unlock();

    let timer: ReturnType<typeof setTimeout> | undefined;
    let onAbort: (() => void) | undefined;
    let notified = false;
    let abortedSignal: AbortSignal | undefined;
    try {
      await new Promise<void>((resolve) => {
        waiter.wake = () => {
          notified = true;
          resolve();
        };
        if (timeoutSeconds !== null) {
          timer = setTimeout(resolve, Math.max(0, timeoutSeconds) * 1000);
        }
        if (signal) {
          const sig = signal;
          if (sig.aborted) {
            abortedSignal = sig;
            resolve();
            return;
          }
          onAbort = () => {
            abortedSignal = sig;
            resolve();
          };
          sig.addEventListener("abort", onAbort, { once: true });
        }
      });
    } finally {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
      if (signal && onAbort) {
        signal.removeEventListener("abort", onAbort);
      }
      this.condWaiters.delete(waiter);
      await this.reacquire();
    }
    if (abortedSignal) {
      throw abortError(abortedSignal);
    }
    return notified;
  }

  /** Wake up to `n` waiters. Precondition: the caller holds the lock. */
  notify(n = 1): void {
    let remaining = n;
    for (const waiter of this.condWaiters) {
      if (remaining <= 0) {
        break;
      }
      this.condWaiters.delete(waiter);
      waiter.wake?.();
      remaining -= 1;
    }
  }

  notifyAll(): void {
    this.notify(Number.POSITIVE_INFINITY);
  }
}

/**
 * Await `work`, but if `signal` fires first, throw while `work` keeps running to completion in
 * the background. The twin of `asyncio.shield`.
 */
export async function awaitShielded(work: Promise<void>, signal?: AbortSignal): Promise<void> {
  if (!signal) {
    await work;
    return;
  }
  // Don't let the backgrounded work surface as an unhandled rejection if we bail out early.
  const guarded = work.catch(() => {});
  await new Promise<void>((resolve, reject) => {
    if (signal.aborted) {
      reject(abortError(signal));
      return;
    }
    const onAbort = (): void => reject(abortError(signal));
    signal.addEventListener("abort", onAbort, { once: true });
    work.then(
      () => {
        signal.removeEventListener("abort", onAbort);
        resolve();
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
  await guarded;
}
