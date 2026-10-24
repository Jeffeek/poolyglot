package io.github.jeffeek.connpool;

/** Invalid pool / breaker / retry configuration. */
public final class PoolConfigError extends PoolError {
    public PoolConfigError(String message) {
        super(message);
    }
}
