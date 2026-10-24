package examples;

import io.github.jeffeek.connpool.AsyncCallableFactory;
import io.github.jeffeek.connpool.AsyncConnectionPool;
import io.github.jeffeek.connpool.CallableFactory;
import io.github.jeffeek.connpool.CancelToken;
import io.github.jeffeek.connpool.ConnectionPool;
import io.github.jeffeek.connpool.PoolConfig;
import io.github.jeffeek.connpool.Timeout;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dependency-free demo: pool a trivial {@code Session} resource, sync and async. A real factory
 * would open a JDBC connection, HTTP client, SSH session, socket… instead of a counter.
 */
public final class Demo {
    private Demo() {
    }

    /** Stand-in for a real connection; closes itself when the pool retires it. */
    static final class Session implements AutoCloseable {
        private static final AtomicInteger IDS = new AtomicInteger();
        final int id = IDS.incrementAndGet();

        String query(String sql) {
            return "session#" + id + " -> " + sql;
        }

        @Override
        public void close() {
            System.out.println("closing session#" + id);
        }
    }

    public static void main(String[] args) throws Exception {
        syncDemo();
        asyncDemo();
    }

    private static void syncDemo() {
        PoolConfig cfg = PoolConfig.builder().maxSize(4).build();
        ConnectionPool<Session> pool = new ConnectionPool<>(
            CallableFactory.<Session>builder(Session::new).validate(s -> true).build(), cfg);
        try (pool) {
            pool.open();
            String row = pool.use(
                (java.util.function.Function<Session, String>) s -> s.query("SELECT 1"), Timeout.of(5.0));
            System.out.println("[sync] " + row);
        }
    }

    private static void asyncDemo() throws Exception {
        AsyncConnectionPool<Session> pool = new AsyncConnectionPool<>(
            AsyncCallableFactory.fromSync(Session::new).build(), PoolConfig.builder().maxSize(4).build());
        pool.openAsync().join();
        CompletableFuture<Void> work = pool.useAsync(s -> {
            System.out.println("[async] " + s.query("SELECT 2"));
            return CompletableFuture.completedFuture(null);
        }, Timeout.of(5.0), CancelToken.NONE);
        work.join();
        pool.closeAsync().join();
    }
}
