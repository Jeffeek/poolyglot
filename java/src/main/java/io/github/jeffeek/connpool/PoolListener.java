package io.github.jeffeek.connpool;

/**
 * Observability hooks. Subclass and override what you care about (metrics, tracing, logging).
 * Hooks run synchronously on the calling thread, outside the pool's lock, so keep them fast.
 * Exceptions raised by listeners are logged and swallowed - a broken metrics exporter must
 * never break the pool.
 */
public class PoolListener {
    public void onConnectionCreated(PooledConnection<?> conn, double duration) {
    }

    public void onConnectionClosed(PooledConnection<?> conn, CloseReason reason) {
    }

    public void onCreateFailed(Throwable error) {
    }

    public void onAcquired(PooledConnection<?> conn, double waitTime) {
    }

    public void onReleased(PooledConnection<?> conn, double holdTime) {
    }

    public void onAcquireTimeout(double waited) {
    }

    public void onConnectionLeak(PooledConnection<?> conn, double heldFor) {
    }

    public void onBreakerStateChange(BreakerState old, BreakerState current) {
    }
}
