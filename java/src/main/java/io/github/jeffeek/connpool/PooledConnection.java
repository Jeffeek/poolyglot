package io.github.jeffeek.connpool;

import java.util.concurrent.atomic.AtomicLong;

/** A raw connection plus the metadata the pool needs to manage it. */
public final class PooledConnection<T> {
    private static final AtomicLong IDS = new AtomicLong(0);

    private final T raw;
    private final long id;
    private final double createdAt;
    private final Double expiresAt;

    // Bookkeeping: readable by anyone (listeners, diagnostics) but only the pool mutates it.
    private Double lastReturnedAt;
    private Double checkedOutAt;
    private int useCount;
    private boolean leakReported;

    public PooledConnection(T raw, double createdAt) {
        this(raw, createdAt, null, null);
    }

    public PooledConnection(T raw, double createdAt, Double expiresAt, Double lastReturnedAt) {
        this.raw = raw;
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
        this.lastReturnedAt = lastReturnedAt != null ? lastReturnedAt : createdAt;
        this.id = IDS.incrementAndGet();
    }

    public T raw() {
        return raw;
    }

    public long id() {
        return id;
    }

    public double createdAt() {
        return createdAt;
    }

    public Double expiresAt() {
        return expiresAt;
    }

    public Double lastReturnedAt() {
        return lastReturnedAt;
    }

    public Double checkedOutAt() {
        return checkedOutAt;
    }

    public int useCount() {
        return useCount;
    }

    public boolean leakReported() {
        return leakReported;
    }

    void setLeakReported(boolean value) {
        this.leakReported = value;
    }

    public double age(double now) {
        return now - createdAt;
    }

    public double idleTime(double now) {
        // lastReturnedAt is never null (constructor defaults it to createdAt, markReturned sets it).
        return now - lastReturnedAt;
    }

    public double holdTime(double now) {
        return checkedOutAt == null ? 0.0 : now - checkedOutAt;
    }

    void markCheckedOut(double now) {
        checkedOutAt = now;
        useCount += 1;
        leakReported = false;
    }

    void markReturned(double now) {
        checkedOutAt = null;
        lastReturnedAt = now;
    }

    @Override
    public String toString() {
        return "<PooledConnection #" + id + " uses=" + useCount + ">";
    }
}
