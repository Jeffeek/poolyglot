package io.github.jeffeek.connpool;

/**
 * A timeout is one of three things, mirroring the Python {@code float | None | UNSET}:
 * <ul>
 *   <li>{@link #unset()}    - "use the configured default"</li>
 *   <li>{@link #infinite()} - wait forever (the Python {@code None})</li>
 *   <li>{@link #of(double)} - wait this many seconds (0 = don't wait)</li>
 * </ul>
 */
public final class Timeout {
    private enum Kind { UNSET, INFINITE, VALUE }

    private static final Timeout UNSET = new Timeout(Kind.UNSET, 0.0);
    private static final Timeout INFINITE = new Timeout(Kind.INFINITE, 0.0);

    private final Kind kind;
    private final double seconds;

    private Timeout(Kind kind, double seconds) {
        this.kind = kind;
        this.seconds = seconds;
    }

    /** "Use the configured default." */
    public static Timeout unset() {
        return UNSET;
    }

    /** Wait forever (no deadline). */
    public static Timeout infinite() {
        return INFINITE;
    }

    /** Wait at most {@code seconds} (0 = don't wait). */
    public static Timeout of(double seconds) {
        return new Timeout(Kind.VALUE, seconds);
    }

    /** {@code null} maps to {@link #infinite()}, matching the Python {@code None}. */
    public static Timeout ofNullable(Double seconds) {
        return seconds == null ? INFINITE : of(seconds);
    }

    public boolean isUnset() {
        return kind == Kind.UNSET;
    }

    /** Resolve to seconds-to-wait, or {@code null} for "no deadline". */
    Double resolve(Double configuredDefault) {
        switch (kind) {
            case UNSET:
                return configuredDefault;
            case INFINITE:
                return null;
            default:
                return seconds;
        }
    }

    /** Absolute deadline (in {@code now}'s units), or {@code null} for "no deadline". */
    static Double computeDeadline(Timeout timeout, Double configuredDefault, double now) {
        Double resolved = timeout.resolve(configuredDefault);
        return resolved == null ? null : now + resolved;
    }

    /** Seconds left until {@code deadline}, or {@code null} if there is no deadline. */
    static Double remaining(Double deadline, double now) {
        return deadline == null ? null : deadline - now;
    }
}
