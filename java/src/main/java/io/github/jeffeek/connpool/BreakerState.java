package io.github.jeffeek.connpool;

/** Circuit breaker state. */
public enum BreakerState {
  CLOSED("closed"),
  OPEN("open"),
  HALF_OPEN("half_open");

  private final String value;

  BreakerState(String value) {
    this.value = value;
  }

  /** Stable wire value, kept identical across every poolyglot language. */
  public String value() {
    return value;
  }
}
