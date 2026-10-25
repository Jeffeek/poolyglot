package io.github.jeffeek.connpool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * One async pool per key, created on demand. Lazy: a pool is built on first use and is NOT
 * {@code openAsync()}-ed, so it does no background maintenance unless you open it yourself.
 */
public final class AsyncPoolRegistry<K, T> {
    private final Function<K, AsyncConnectionFactory<T>> factoryFor;
    private final PoolConfig config;
    private final Map<K, AsyncConnectionPool<T>> pools = new HashMap<>();
    private final Object lock = new Object();
    private boolean closed;

    public AsyncPoolRegistry(Function<K, AsyncConnectionFactory<T>> factoryFor) {
        this(factoryFor, null);
    }

    public AsyncPoolRegistry(Function<K, AsyncConnectionFactory<T>> factoryFor, PoolConfig config) {
        this.factoryFor = factoryFor;
        this.config = config;
    }

    public AsyncConnectionPool<T> pool(K key) {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("registry is closed");
            }
            AsyncConnectionPool<T> pool = pools.get(key);
            if (pool == null) {
                String name = key.toString();
                pool = new AsyncConnectionPool<>(factoryFor.apply(key), config, null, null, null, name != null ? name : "pool");
                pools.put(key, pool);
            }
            return pool;
        }
    }

    public CompletableFuture<T> acquireAsync(K key, Timeout timeout, CancelToken token) {
        return pool(key).acquireAsync(timeout, token);
    }

    public CompletableFuture<Void> useAsync(K key, Function<T, CompletableFuture<Void>> body,
                                            Timeout timeout, CancelToken token) {
        return pool(key).useAsync(body, timeout, token);
    }

    public CompletableFuture<Void> closeAsync() {
        List<AsyncConnectionPool<T>> toClose;
        synchronized (lock) {
            closed = true;
            toClose = new ArrayList<>(pools.values());
            pools.clear();
        }
        CompletableFuture<Void> all = CompletableFuture.completedFuture(null);
        for (AsyncConnectionPool<T> pool : toClose) {
            all = all.thenCompose(ignored -> pool.closeAsync());
        }
        return all;
    }
}
