package io.github.jeffeek.connpool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * One pool per key (e.g. per host, per tenant, per database) created on demand.
 *
 * <p>Lazy: a pool is built on first use and is NOT {@code open()}-ed, so it does no background
 * maintenance unless you open it yourself ({@code registry.pool(key).open()}).
 */
public final class PoolRegistry<K, T> implements AutoCloseable {
    private final Function<K, ConnectionFactory<T>> factoryFor;
    private final PoolConfig config;
    private final Map<K, ConnectionPool<T>> pools = new HashMap<>();
    private final Object lock = new Object();
    private boolean closed;

    public PoolRegistry(Function<K, ConnectionFactory<T>> factoryFor) {
        this(factoryFor, null);
    }

    public PoolRegistry(Function<K, ConnectionFactory<T>> factoryFor, PoolConfig config) {
        this.factoryFor = factoryFor;
        this.config = config;
    }

    public ConnectionPool<T> pool(K key) {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("registry is closed");
            }
            ConnectionPool<T> pool = pools.get(key);
            if (pool == null) {
                String name = key.toString();
                pool = new ConnectionPool<>(factoryFor.apply(key), config, null, null, null, name != null ? name : "pool");
                pools.put(key, pool);
            }
            return pool;
        }
    }

    public T acquire(K key, Timeout timeout) {
        return pool(key).acquire(timeout);
    }

    public void use(K key, Consumer<T> body, Timeout timeout) {
        pool(key).use(body, timeout);
    }

    public <R> R use(K key, Function<T, R> body, Timeout timeout) {
        return pool(key).use(body, timeout);
    }

    public ConnectionPool<T>.Lease connection(K key, Timeout timeout) {
        return pool(key).connection(timeout);
    }

    @Override
    public void close() {
        List<ConnectionPool<T>> toClose;
        synchronized (lock) {
            closed = true;
            toClose = new ArrayList<>(pools.values());
            pools.clear();
        }
        for (ConnectionPool<T> pool : toClose) {
            pool.close();
        }
    }
}
