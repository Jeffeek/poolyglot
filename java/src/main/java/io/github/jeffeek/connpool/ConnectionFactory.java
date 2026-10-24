package io.github.jeffeek.connpool;

/**
 * Synchronous factory contract: the ONE seam between the pool and the outside world.
 *
 * <p>To pool <em>anything</em> (DB driver, HTTP client, SSH session, socket, gRPC channel...)
 * subclass this - or just pass lambdas via {@link CallableFactory}.
 */
public abstract class ConnectionFactory<T> {
    /** Open a new connection. Should enforce its own connect timeout. */
    public abstract T create();

    /** Cheap liveness check (e.g. {@code SELECT 1}). Return false / throw = dead. */
    public boolean validate(T conn) {
        return true;
    }

    /** Called on release to scrub session state (e.g. rollback). */
    public void reset(T conn) {
    }

    /** Dispose of a connection. Default: {@code close()} if the connection is {@link AutoCloseable}. */
    public void close(T conn) {
        if (conn instanceof AutoCloseable) {
            try {
                ((AutoCloseable) conn).close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * True if {@code exc} (raised by user code using the connection) proves the connection is
     * broken, so it is discarded without further checks.
     */
    public boolean isFatalError(Throwable exc) {
        return false;
    }
}
