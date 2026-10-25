package io.github.jeffeek.connpool;

/** No connection became available before the acquire deadline. */
public final class PoolTimeoutError extends PoolError {
  public PoolTimeoutError(String message) {
    super(message);
  }
}
