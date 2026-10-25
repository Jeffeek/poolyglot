package io.github.jeffeek.connpool;

/** Why a connection left the pool. */
public enum CloseReason {
  EXPIRED("expired"), // max_lifetime reached (rotation)
  MAX_USES("max_uses"), // max_uses reached (rotation)
  IDLE("idle"), // sat idle longer than max_idle_time
  UNHEALTHY("unhealthy"), // failed validation
  ERROR("error"), // caller's block raised and the connection is suspect
  RESET_FAILED("reset_failed"), // factory.reset() raised on release
  DISCARDED("discarded"), // caller asked for it
  POOL_CLOSED("pool_closed");

  private final String value;

  CloseReason(String value) {
    this.value = value;
  }

  /** Stable wire value, kept identical across every poolyglot language. */
  public String value() {
    return value;
  }
}
