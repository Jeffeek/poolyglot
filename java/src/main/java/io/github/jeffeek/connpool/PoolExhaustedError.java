package io.github.jeffeek.connpool;

/** The pool is full and the waiter queue is at {@code maxWaiters}; shedding load. */
public final class PoolExhaustedError extends PoolError {
  public PoolExhaustedError(String message) {
    super(message);
  }
}
