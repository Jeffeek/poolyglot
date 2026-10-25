/**
 * Circuit breaker (single-threaded; the event loop serialises access, so no lock is needed —
 * the Python/C# ports use one only because their pools are multi-threaded).
 *
 *     CLOSED --(N consecutive failures)--> OPEN --(recovery_timeout)--> HALF_OPEN
 *        ^                                   ^                              |
 *        |                                   +------ any probe fails -------+
 *        +------------ enough probes succeed ---------------------------------+
 */
import { type Clock, SystemClock } from "../core/clock.js";
import { CircuitOpenError, PoolConfigError } from "../core/errors.js";

/** Breaker state. The values are the shared snake_case wire strings. */
export const BreakerState = {
  CLOSED: "closed",
  OPEN: "open",
  HALF_OPEN: "half_open",
} as const;

export type BreakerState = (typeof BreakerState)[keyof typeof BreakerState];

export interface BreakerConfigOptions {
  failureThreshold?: number;
  recoveryTimeout?: number;
  halfOpenMaxCalls?: number;
  successThreshold?: number;
}

export class BreakerConfig {
  readonly failureThreshold: number;
  readonly recoveryTimeout: number;
  readonly halfOpenMaxCalls: number;
  readonly successThreshold: number;

  constructor(options: BreakerConfigOptions = {}) {
    this.failureThreshold = options.failureThreshold ?? 5;
    this.recoveryTimeout = options.recoveryTimeout ?? 30.0;
    this.halfOpenMaxCalls = options.halfOpenMaxCalls ?? 1;
    this.successThreshold = options.successThreshold ?? 1;

    if (this.failureThreshold < 1 || this.halfOpenMaxCalls < 1 || this.successThreshold < 1) {
      throw new PoolConfigError("breaker thresholds must be >= 1");
    }
    if (this.recoveryTimeout < 0) {
      throw new PoolConfigError("breaker.recoveryTimeout must be >= 0");
    }
  }
}

type Transition = readonly [BreakerState, BreakerState] | null;

export type StateChangeListener = (oldState: BreakerState, newState: BreakerState) => void;

export interface CircuitBreakerOptions {
  clock?: Clock;
  onStateChange?: StateChangeListener;
}

/** Usage: `beforeCall()` -> do work -> `recordSuccess()` / `recordFailure()`. */
export class CircuitBreaker {
  private readonly cfg: BreakerConfig;
  private readonly clock: Clock;
  private readonly onChange: StateChangeListener | undefined;

  private _state: BreakerState = BreakerState.CLOSED;
  private failures = 0;
  private openedAt = 0.0;
  private probesInflight = 0;
  private probeSuccesses = 0;

  constructor(config?: BreakerConfig, options: CircuitBreakerOptions = {}) {
    this.cfg = config ?? new BreakerConfig();
    this.clock = options.clock ?? new SystemClock();
    this.onChange = options.onStateChange;
  }

  get state(): BreakerState {
    const transition = this.refresh();
    const state = this._state;
    this.emit(transition);
    return state;
  }

  /** Throw {@link CircuitOpenError} if the call must not be attempted. */
  beforeCall(): void {
    const transition = this.refresh();
    let error: CircuitOpenError | null = null;
    if (this._state === BreakerState.OPEN) {
      const wait = this.cfg.recoveryTimeout - (this.clock.monotonic() - this.openedAt);
      error = new CircuitOpenError(Math.max(0.0, wait));
    } else if (this._state === BreakerState.HALF_OPEN) {
      if (this.probesInflight >= this.cfg.halfOpenMaxCalls) {
        error = new CircuitOpenError(0.0);
      } else {
        this.probesInflight += 1;
      }
    }
    this.emit(transition);
    if (error) {
      throw error;
    }
  }

  recordSuccess(): void {
    let transition: Transition = null;
    if (this._state === BreakerState.CLOSED) {
      this.failures = 0;
    } else if (this._state === BreakerState.HALF_OPEN) {
      this.probesInflight = Math.max(0, this.probesInflight - 1);
      this.probeSuccesses += 1;
      if (this.probeSuccesses >= this.cfg.successThreshold) {
        transition = this.setState(BreakerState.CLOSED);
      }
    }
    this.emit(transition);
  }

  recordFailure(): void {
    let transition: Transition = null;
    if (this._state === BreakerState.CLOSED) {
      this.failures += 1;
      if (this.failures >= this.cfg.failureThreshold) {
        transition = this.setState(BreakerState.OPEN);
      }
    } else if (this._state === BreakerState.HALF_OPEN) {
      transition = this.setState(BreakerState.OPEN);
    }
    // OPEN: a late failure from a call started before tripping; ignore.
    this.emit(transition);
  }

  /**
   * The call neither succeeded nor failed (e.g. cancelled or timed out before the backend
   * answered). Release a half-open probe slot without counting it either way.
   */
  recordAbort(): void {
    if (this._state === BreakerState.HALF_OPEN) {
      this.probesInflight = Math.max(0, this.probesInflight - 1);
    }
  }

  reset(): void {
    const transition = this.setState(BreakerState.CLOSED);
    this.emit(transition);
  }

  private refresh(): Transition {
    if (
      this._state === BreakerState.OPEN &&
      this.clock.monotonic() - this.openedAt >= this.cfg.recoveryTimeout
    ) {
      return this.setState(BreakerState.HALF_OPEN);
    }
    return null;
  }

  private setState(next: BreakerState): Transition {
    const old = this._state;
    if (old === next) {
      return null;
    }
    this._state = next;
    this.failures = 0;
    this.probesInflight = 0;
    this.probeSuccesses = 0;
    if (next === BreakerState.OPEN) {
      this.openedAt = this.clock.monotonic();
    }
    return [old, next];
  }

  private emit(transition: Transition): void {
    if (transition && this.onChange) {
      this.onChange(transition[0], transition[1]);
    }
  }
}
