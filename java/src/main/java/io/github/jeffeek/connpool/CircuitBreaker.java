package io.github.jeffeek.connpool;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Circuit breaker (thread-safe; the lock is never held across user callbacks).
 *
 * <pre>
 *     CLOSED --(N consecutive failures)--&gt; OPEN --(recovery_timeout)--&gt; HALF_OPEN
 *        ^                                   ^                              |
 *        |                                   +------ any probe fails -------+
 *        +------------ enough probes succeed ---------------------------------+
 * </pre>
 *
 * Usage: {@code beforeCall()} -&gt; do work -&gt; {@code recordSuccess()} / {@code
 * recordFailure()}.
 */
public final class CircuitBreaker {
  private final BreakerConfig cfg;
  private final Clock clock;
  private final BiConsumer<BreakerState, BreakerState> onChange;
  private final Object lock = new Object();

  private BreakerState state = BreakerState.CLOSED;
  private int failures;
  private double openedAt;
  private int probesInflight;
  private int probeSuccesses;

  public CircuitBreaker() {
    this(null, null, null);
  }

  public CircuitBreaker(
      BreakerConfig config, Clock clock, BiConsumer<BreakerState, BreakerState> onStateChange) {
    this.cfg = config != null ? config : BreakerConfig.defaults();
    this.clock = clock != null ? clock : new SystemClock();
    this.onChange = onStateChange;
  }

  public BreakerState state() {
    BreakerState[] transition;
    BreakerState current;
    synchronized (lock) {
      transition = refresh();
      current = state;
    }
    emit(transition);
    return current;
  }

  /** Throw {@link CircuitOpenError} if the call must not be attempted. */
  public void beforeCall() {
    CircuitOpenError error = null;
    BreakerState[] transition;
    synchronized (lock) {
      transition = refresh();
      if (state == BreakerState.OPEN) {
        double wait = cfg.recoveryTimeout() - (clock.monotonic() - openedAt);
        error = new CircuitOpenError(Math.max(0.0, wait));
      } else if (state == BreakerState.HALF_OPEN) {
        if (probesInflight >= cfg.halfOpenMaxCalls()) {
          error = new CircuitOpenError(0.0);
        } else {
          probesInflight += 1;
        }
      }
    }
    emit(transition);
    if (error != null) {
      throw error;
    }
  }

  public void recordSuccess() {
    BreakerState[] transition = null;
    synchronized (lock) {
      if (state == BreakerState.CLOSED) {
        failures = 0;
      } else if (state == BreakerState.HALF_OPEN) {
        probesInflight = Math.max(0, probesInflight - 1);
        probeSuccesses += 1;
        if (probeSuccesses >= cfg.successThreshold()) {
          transition = setState(BreakerState.CLOSED);
        }
      }
    }
    emit(transition);
  }

  public void recordFailure() {
    BreakerState[] transition = null;
    synchronized (lock) {
      if (state == BreakerState.CLOSED) {
        failures += 1;
        if (failures >= cfg.failureThreshold()) {
          transition = setState(BreakerState.OPEN);
        }
      } else if (state == BreakerState.HALF_OPEN) {
        transition = setState(BreakerState.OPEN);
      }
      // OPEN: a late failure from a call started before tripping; ignore.
    }
    emit(transition);
  }

  /**
   * The call neither succeeded nor failed (e.g. cancelled or timed out before the backend
   * answered). Release a half-open probe slot without counting it either way.
   */
  public void recordAbort() {
    synchronized (lock) {
      if (state == BreakerState.HALF_OPEN) {
        probesInflight = Math.max(0, probesInflight - 1);
      }
    }
  }

  /** Convenience wrapper for arbitrary callables. */
  public <R> R call(Supplier<R> fn) {
    beforeCall();
    R result;
    try {
      result = fn.get();
    } catch (Throwable t) {
      recordFailure();
      throw t;
    }
    recordSuccess();
    return result;
  }

  public void reset() {
    BreakerState[] transition;
    synchronized (lock) {
      transition = setState(BreakerState.CLOSED);
    }
    emit(transition);
  }

  // -- internals (call with the lock held) --------------------------------
  private BreakerState[] refresh() {
    if (state == BreakerState.OPEN && clock.monotonic() - openedAt >= cfg.recoveryTimeout()) {
      return setState(BreakerState.HALF_OPEN);
    }
    return null;
  }

  private BreakerState[] setState(BreakerState next) {
    BreakerState old = state;
    if (old == next) {
      return null;
    }
    state = next;
    failures = 0;
    probesInflight = 0;
    probeSuccesses = 0;
    if (next == BreakerState.OPEN) {
      openedAt = clock.monotonic();
    }
    return new BreakerState[] {old, next};
  }

  private void emit(BreakerState[] transition) {
    // Fired after the lock is released so callbacks may safely re-enter the breaker.
    if (transition != null && onChange != null) {
      onChange.accept(transition[0], transition[1]);
    }
  }
}
