package io.github.jeffeek.connpool;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe synchronous connection pool.
 *
 * <p>Design in one paragraph: a single lock guards a tiny amount of state (idle deque,
 * checked-out map, a slot counter). <em>All</em> I/O - create, validate, reset, close - happens
 * outside that lock, so a slow backend never blocks unrelated callers. A "slot" is reserved under
 * the lock <em>before</em> creating, which is what makes {@code maxSize} a hard cap. The rules for
 * when connections retire live in {@link Lifecycle}; this file is plumbing.
 */
public class ConnectionPool<T> implements AutoCloseable {
    private static final Logger LOG = System.getLogger("connpool");

    private final String name;
    private final ConnectionFactory<T> factory;
    private final PoolConfig cfg;
    private final Clock clock;
    private final Random rng;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition cond = lock.newCondition();
    private Deque<PooledConnection<T>> idle = new ArrayDeque<>();
    private final Map<T, PooledConnection<T>> checkedOut = new IdentityHashMap<>();
    private int size;       // idle + checked out + being created/closed
    private int waiters;
    private volatile boolean closed;

    private final StatsCollector stats = new StatsCollector();
    private final ListenerGroup events;
    private final CircuitBreaker breaker;
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private Thread thread;

    public ConnectionPool(ConnectionFactory<T> factory) {
        this(factory, null, null, null, null, "pool");
    }

    public ConnectionPool(ConnectionFactory<T> factory, PoolConfig config) {
        this(factory, config, null, null, null, "pool");
    }

    public ConnectionPool(ConnectionFactory<T> factory, PoolConfig config, List<PoolListener> listeners,
                          Clock clock, Random rng, String name) {
        this.name = name;
        this.factory = factory;
        this.cfg = config != null ? config : PoolConfig.defaults();
        this.clock = clock != null ? clock : new SystemClock();
        this.rng = rng != null ? rng : new Random();

        List<PoolListener> all = new ArrayList<>();
        all.add(stats);
        if (listeners != null) {
            all.addAll(listeners);
        }
        this.events = new ListenerGroup(all);
        this.breaker = this.cfg.breaker() != null
            ? new CircuitBreaker(this.cfg.breaker(), this.clock,
                (o, n) -> events.dispatch(l -> l.onBreakerStateChange(o, n)))
            : null;
    }

    public String name() {
        return name;
    }

    // ------------------------------------------------------------------ lifecycle
    /**
     * Warm up {@code minSize} connections and start the background maintenance thread. Optional:
     * without it the pool is purely lazy and does no background work.
     */
    public ConnectionPool<T> open() {
        lock.lock();
        try {
            if (closed) {
                throw new PoolClosedError("pool '" + name + "' is closed");
            }
            if (thread != null) {
                return this;
            }
            if (cfg.maintenanceInterval() != null) {
                thread = new Thread(this::maintenanceLoop, "connpool-" + name);
                thread.setDaemon(true);
            }
        } finally {
            lock.unlock();
        }
        fillToMin();
        if (thread != null) {
            thread.start();
        }
        return this;
    }

    /**
     * Close idle connections now; checked-out ones close on release. Idempotent. {@code timeout}
     * (seconds) drains gracefully: wait up to this long for in-use connections to return.
     * {@code null} does not wait; {@code 0} returns immediately.
     */
    public void close(Double timeout) {
        List<PooledConnection<T>> toClose;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            toClose = new ArrayList<>(idle);
            idle.clear();
            cond.signalAll();
            if (timeout != null) {
                double deadline = clock.monotonic() + timeout;
                while (!checkedOut.isEmpty()) {
                    double left = deadline - clock.monotonic();
                    if (left <= 0) {
                        break;
                    }
                    awaitCond(left);
                }
            }
        } finally {
            lock.unlock();
        }
        stopLatch.countDown();
        Threads.join(thread);
        for (PooledConnection<T> pooled : toClose) {
            destroy(pooled, CloseReason.POOL_CLOSED);
        }
    }

    @Override
    public void close() {
        close(null);
    }

    public boolean closed() {
        return closed;
    }

    // --------------------------------------------------------------------- usage
    public T acquire() {
        return acquire(Timeout.unset());
    }

    public T acquire(Timeout timeout) {
        double started = clock.monotonic();
        Double deadline = Timeout.computeDeadline(timeout, cfg.acquireTimeout(), started);
        PooledConnection<T> pooled;
        try {
            while (true) {
                PooledConnection<T> taken = takeIdleOrReserve(deadline);
                if (taken == null) { // slot reserved -> build a new one
                    Created<T> created = create(deadline);
                    registerNew(created.conn(), created.duration());
                    pooled = created.conn();
                    break;
                }
                CloseReason reason = borrowReason(taken);
                if (reason == null) {
                    pooled = taken;
                    break;
                }
                destroy(taken, reason); // stale/dead: drop it and try again
            }
        } catch (PoolTimeoutError e) {
            events.dispatch(l -> l.onAcquireTimeout(clock.monotonic() - started));
            throw e;
        }
        double now = clock.monotonic();
        pooled.markCheckedOut(now);
        PooledConnection<T> fp = pooled;
        events.dispatch(l -> l.onAcquired(fp, now - started));
        return pooled.raw();
    }

    /** Return a connection. {@code discard} forces it to be closed instead of reused. */
    public void release(T conn) {
        release(conn, false, null);
    }

    public void release(T conn, boolean discard, Throwable error) {
        PooledConnection<T> pooled;
        lock.lock();
        try {
            pooled = checkedOut.remove(conn);
        } finally {
            lock.unlock();
        }
        if (pooled == null) {
            throw new InvalidReleaseError("object is not checked out from this pool (double release?)");
        }
        double now = clock.monotonic();
        double hold = pooled.holdTime(now);
        CloseReason reason = releaseReason(pooled, conn, discard, error, now);
        if (reason == null && !park(pooled, now)) {
            reason = CloseReason.POOL_CLOSED;
        }
        if (reason != null) {
            destroy(pooled, reason);
        }
        PooledConnection<T> fp = pooled;
        events.dispatch(l -> l.onReleased(fp, hold));
    }

    /**
     * Borrow a connection for the duration of {@code body}. If it throws, the connection is
     * discarded when the factory says the error is fatal or when it fails validation; otherwise
     * it is reused.
     */
    public void use(java.util.function.Consumer<T> body) {
        use(body, Timeout.unset());
    }

    public <R> R use(java.util.function.Function<T, R> body) {
        return use(body, Timeout.unset());
    }

    public void use(java.util.function.Consumer<T> body, Timeout timeout) {
        T conn = acquire(timeout);
        try {
            body.accept(conn);
        } catch (RuntimeException | Error ex) {
            release(conn, false, ex);
            throw ex;
        }
        release(conn);
    }

    public <R> R use(java.util.function.Function<T, R> body, Timeout timeout) {
        T conn = acquire(timeout);
        R result;
        try {
            result = body.apply(conn);
        } catch (RuntimeException | Error ex) {
            release(conn, false, ex);
            throw ex;
        }
        release(conn);
        return result;
    }

    /** A borrowed connection released when closed (success path; use {@link #use} for error-aware discard). */
    public final class Lease implements AutoCloseable {
        private final T value;

        Lease(T value) {
            this.value = value;
        }

        public T get() {
            return value;
        }

        @Override
        public void close() {
            release(value);
        }
    }

    public Lease connection(Timeout timeout) {
        return new Lease(acquire(timeout));
    }

    public Lease connection() {
        return connection(Timeout.unset());
    }

    public PoolStats stats() {
        int sz;
        int idleCount;
        int inUse;
        int waiting;
        lock.lock();
        try {
            sz = size;
            idleCount = idle.size();
            inUse = checkedOut.size();
            waiting = waiters;
        } finally {
            lock.unlock();
        }
        return stats.snapshot(name, cfg.maxSize(), sz, idleCount, inUse, waiting,
            breaker != null ? breaker.state().value() : null);
    }

    /** One sweep: evict expired/over-idle connections, then refill to {@code minSize}. */
    public void runMaintenance() {
        List<Lifecycle.Doomed<T>> doomed;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            Lifecycle.SweepResult<T> swept = Lifecycle.sweep(idle, size, cfg, clock.monotonic());
            doomed = swept.doomed();
            idle = new ArrayDeque<>(swept.keep());
        } finally {
            lock.unlock();
        }
        for (Lifecycle.Doomed<T> d : doomed) {
            destroy(d.connection(), d.reason());
        }
        healthCheckIdle();
        fillToMin();
        checkLeaks();
    }

    private void healthCheckIdle() {
        if (!cfg.validateIdle()) {
            return;
        }
        List<PooledConnection<T>> batch;
        lock.lock();
        try {
            batch = new ArrayList<>(idle);
            idle.clear();
            for (PooledConnection<T> pooled : batch) {
                checkedOut.put(pooled.raw(), pooled);
            }
        } finally {
            lock.unlock();
        }
        for (PooledConnection<T> pooled : batch) {
            boolean healthy = isHealthy(pooled.raw());
            boolean keep;
            lock.lock();
            try {
                checkedOut.remove(pooled.raw());
                keep = healthy && !closed;
                if (keep) {
                    idle.addLast(pooled);
                    cond.signal();
                }
            } finally {
                lock.unlock();
            }
            if (!keep) {
                destroy(pooled, healthy ? CloseReason.POOL_CLOSED : CloseReason.UNHEALTHY);
            }
        }
    }

    private void checkLeaks() {
        Double timeout = cfg.leakDetectionTimeout();
        if (timeout == null) {
            return;
        }
        double now = clock.monotonic();
        List<PooledConnection<T>> leaked = new ArrayList<>();
        List<Double> held = new ArrayList<>();
        lock.lock();
        try {
            for (PooledConnection<T> pooled : checkedOut.values()) {
                if (!pooled.leakReported() && pooled.holdTime(now) >= timeout) {
                    pooled.setLeakReported(true);
                    leaked.add(pooled);
                    held.add(pooled.holdTime(now));
                }
            }
        } finally {
            lock.unlock();
        }
        for (int i = 0; i < leaked.size(); i++) {
            PooledConnection<T> pooled = leaked.get(i);
            double h = held.get(i);
            LOG.log(Level.WARNING, String.format(Locale.ROOT,
                "pool '%s': possible connection leak: %s held for %.1fs", name, pooled, h));
            events.dispatch(l -> l.onConnectionLeak(pooled, h));
        }
    }

    // ----------------------------------------------------------------- internals
    private PooledConnection<T> takeIdleOrReserve(Double deadline) {
        lock.lock();
        try {
            while (true) {
                if (closed) {
                    throw new PoolClosedError("pool '" + name + "' is closed");
                }
                PooledConnection<T> pooled = Lifecycle.popIdle(idle, cfg.idleStrategy());
                if (pooled != null) {
                    checkedOut.put(pooled.raw(), pooled);
                    return pooled;
                }
                if (size < cfg.maxSize()) {
                    size += 1;
                    return null;
                }
                Double left = Timeout.remaining(deadline, clock.monotonic());
                if (left != null && left <= 0) {
                    throw new PoolTimeoutError(
                        "pool '" + name + "': no connection available (max_size=" + cfg.maxSize() + ")");
                }
                if (cfg.maxWaiters() != null && waiters >= cfg.maxWaiters()) {
                    throw new PoolExhaustedError(
                        "pool '" + name + "': waiter queue full (max_waiters=" + cfg.maxWaiters() + ")");
                }
                waiters += 1;
                try {
                    awaitCond(left);
                } finally {
                    waiters -= 1;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    // Caller must hold the lock. null seconds = wait forever.
    private void awaitCond(Double seconds) {
        Threads.awaitCondition(cond, seconds, name);
    }

    private CloseReason borrowReason(PooledConnection<T> pooled) {
        double now = clock.monotonic();
        CloseReason reason = Lifecycle.borrowReason(pooled, cfg, now);
        if (reason == null && Lifecycle.needsValidation(pooled, cfg, now) && !isHealthy(pooled.raw())) {
            return CloseReason.UNHEALTHY;
        }
        return reason;
    }

    private CloseReason releaseReason(PooledConnection<T> pooled, T conn, boolean discard, Throwable error, double now) {
        if (discard) {
            return CloseReason.DISCARDED;
        }
        if (error != null && (factory.isFatalError(error) || !isHealthy(conn))) {
            return CloseReason.ERROR;
        }
        CloseReason reason = Lifecycle.rotationReason(pooled, cfg, now);
        if (reason != null) {
            return reason;
        }
        try {
            factory.reset(conn);
        } catch (RuntimeException ex) {
            LOG.log(Level.WARNING, "pool '" + name + "': reset failed; discarding " + pooled, ex);
            return CloseReason.RESET_FAILED;
        }
        return null;
    }

    private boolean isHealthy(T raw) {
        try {
            return factory.validate(raw);
        } catch (RuntimeException e) {
            return false;
        }
    }

    private boolean park(PooledConnection<T> pooled, double now) {
        lock.lock();
        try {
            if (closed) {
                return false;
            }
            pooled.markReturned(now);
            idle.addLast(pooled);
            cond.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** A freshly built connection and how long the factory took. */
    private record Created<C>(PooledConnection<C> conn, double duration) {
    }

    private Created<T> create(Double deadline) {
        double started = clock.monotonic();
        T raw;
        try {
            raw = createRaw(deadline);
        } catch (RuntimeException | Error e) {
            freeSlot();
            throw e;
        }
        double now = clock.monotonic();
        PooledConnection<T> pooled = Lifecycle.newConnection(raw, cfg, now, rng);
        return new Created<>(pooled, now - started);
    }

    private void registerNew(PooledConnection<T> pooled, double createdIn) {
        String action;
        lock.lock();
        try {
            if (closed) {
                action = "closed";
            } else if (checkedOut.containsKey(pooled.raw()) || containsIdle(pooled.raw())) {
                action = "duplicate";
            } else {
                checkedOut.put(pooled.raw(), pooled);
                action = "ok";
            }
        } finally {
            lock.unlock();
        }
        if (action.equals("duplicate")) {
            freeSlot();
            throw new ConnectionCreateError(
                "pool '" + name + "': factory.create() returned an object the pool already manages; "
                    + "each create() must return a distinct connection");
        }
        events.dispatch(l -> l.onConnectionCreated(pooled, createdIn));
        if (action.equals("closed")) {
            destroy(pooled, CloseReason.POOL_CLOSED);
            throw new PoolClosedError("pool '" + name + "' is closed");
        }
    }

    private boolean containsIdle(T raw) {
        for (PooledConnection<T> p : idle) {
            if (p.raw() == raw) {
                return true;
            }
        }
        return false;
    }

    private T createRaw(Double deadline) {
        if (breaker != null) {
            breaker.beforeCall();
        }
        T raw;
        try {
            raw = createWithRetry(deadline);
        } catch (ConnectionCreateError e) {
            if (breaker != null) {
                breaker.recordFailure();
            }
            throw e;
        } catch (RuntimeException | Error e) {
            if (breaker != null) {
                breaker.recordAbort();
            }
            throw e;
        }
        if (breaker != null) {
            breaker.recordSuccess();
        }
        return raw;
    }

    private T createWithRetry(Double deadline) {
        RetryPolicy retry = cfg.retry();
        int attempt = 0;
        while (true) {
            if (closed) {
                throw new PoolClosedError("pool '" + name + "' is closed");
            }
            attempt += 1;
            try {
                return factory.create();
            } catch (RuntimeException exc) {
                events.dispatch(l -> l.onCreateFailed(exc));
                if (attempt >= retry.maxAttempts() || !retry.shouldRetry(exc)) {
                    throw new ConnectionCreateError(
                        "pool '" + name + "': could not create connection after " + attempt
                            + " attempt(s): " + exc.getMessage(), exc);
                }
                double delay = retry.delayFor(attempt, rng);
                Double left = Timeout.remaining(deadline, clock.monotonic());
                if (left != null && delay >= left) {
                    throw new PoolTimeoutError(
                        "pool '" + name + "': acquire deadline reached while retrying creation");
                }
                clock.sleep(delay);
            }
        }
    }

    private void freeSlot() {
        lock.lock();
        try {
            size -= 1;
            cond.signal();
        } finally {
            lock.unlock();
        }
    }

    private void destroy(PooledConnection<T> pooled, CloseReason reason) {
        try {
            factory.close(pooled.raw());
        } catch (RuntimeException ex) {
            LOG.log(Level.WARNING, "pool '" + name + "': error closing " + pooled, ex);
        } finally {
            lock.lock();
            try {
                checkedOut.remove(pooled.raw());
                size -= 1;
                cond.signal();
            } finally {
                lock.unlock();
            }
        }
        events.dispatch(l -> l.onConnectionClosed(pooled, reason));
    }

    private void fillToMin() {
        while (true) {
            lock.lock();
            try {
                if (closed || size >= cfg.minSize()) {
                    return;
                }
                size += 1;
            } finally {
                lock.unlock();
            }
            Created<T> created;
            try {
                created = create(null);
            } catch (RuntimeException ex) {
                LOG.log(Level.WARNING, "pool '" + name + "': warm-up failed: " + ex.getMessage());
                return;
            }
            PooledConnection<T> pooled = created.conn();
            events.dispatch(l -> l.onConnectionCreated(pooled, created.duration()));
            if (!park(pooled, clock.monotonic())) {
                destroy(pooled, CloseReason.POOL_CLOSED);
            }
        }
    }

    void maintenanceLoop() {
        double interval = cfg.maintenanceInterval();
        long intervalMs = (long) Math.max(1, interval * 1000);
        while (true) {
            try {
                if (stopLatch.await(intervalMs, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            maintenanceTick();
        }
    }

    // One maintenance tick with its own error boundary (a failing sweep must not kill the loop).
    void maintenanceTick() {
        try {
            runMaintenance();
        } catch (RuntimeException ex) {
            LOG.log(Level.ERROR, "pool '" + name + "': maintenance pass failed", ex);
        }
    }

    // test seam: stop the maintenance loop started directly by a test
    void signalStop() {
        stopLatch.countDown();
    }

    // test seam: a snapshot of the idle connections' raw values
    List<T> idleSnapshot() {
        lock.lock();
        try {
            List<T> out = new ArrayList<>();
            for (PooledConnection<T> p : idle) {
                out.add(p.raw());
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    boolean warnIfLeaked() {
        if (!closed && size > 0) {
            LOG.log(Level.WARNING, "pool '" + name + "' was not closed; " + size + " connection(s) may leak");
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        PoolStats s = stats();
        return "<ConnectionPool '" + name + "' size=" + s.size() + "/" + s.maxSize()
            + " idle=" + s.idle() + " in_use=" + s.inUse() + ">";
    }
}
