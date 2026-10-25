package io.github.jeffeek.connpool;

/** Which idle connection to hand out next. */
public enum IdleStrategy {
  LIFO("lifo"), // reuse the hottest connection; cold ones age out and get evicted
  FIFO("fifo"); // reuse the oldest idle connection; spreads load / rotates evenly

  private final String value;

  IdleStrategy(String value) {
    this.value = value;
  }

  /** Stable wire value, kept identical across every poolyglot language. */
  public String value() {
    return value;
  }
}
