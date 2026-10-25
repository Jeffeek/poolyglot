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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Asynchronous connection pool. Same model and same rules ({@link Lifecycle}) as the sync pool.
 *
 * <p>Java mapping: each public {@code *Async} method runs its (cooperatively blocking) body on a
 * daemon worker pool and returns a {@link CompletableFuture}; the internal condition variable is a
 * {@link ReentrantLock}/{@link Condition} (the JDK twin of {@code asyncio.Condition}); cancellation
 * is explicit via {@link CancelToken}. Differences worth knowing: {@code createTimeout} is enforced
 * here; listener hooks stay synchronous; a cancelled operation never leaks a slot.
 */
public class AsyncConnectionPool<T> {
  private static final Logger LOG = System.getLogger("connpool");

  private final String name;
  private final AsyncConnectionFactory<T> factory;
  private final PoolConfig cfg;
  private final Clock clock;
  private final Random rng;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition cond = lock.newCondition();
  private Deque<PooledConnection<T>> idle = new ArrayDeque<>();
  private final Map<T, PooledConnection<T>> checkedOut = new IdentityHashMap<>();
  private int size;
  private int waiters;
  private volatile boolean closed;

  private final StatsCollector stats = new StatsCollector();
  private final ListenerGroup events;
  private final CircuitBreaker breaker;
  private final CountDownLatch stopLatch = new CountDownLatch(1);
  private Thread thread;
  private boolean opened;

  public AsyncConnectionPool(AsyncConnectionFactory<T> factory) {
    this(factory, null, null, null, null, "pool");
  }

  public AsyncConnectionPool(AsyncConnectionFactory<T> factory, PoolConfig config) {
    this(factory, config, null, null, null, "pool");
  }

  public AsyncConnectionPool(
      AsyncConnectionFactory<T> factory,
      PoolConfig config,
      List<PoolListener> listeners,
      Clock clock,
      Random rng,
      String name) {
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
    this.breaker =
        this.cfg.breaker() != null
            ? new CircuitBreaker(
                this.cfg.breaker(),
                this.clock,
                (o, n) -> events.dispatch(l -> l.onBreakerStateChange(o, n)))
            : null;
  }

  public String name() {
    return name;
  }

  // ------------------------------------------------------------------ lifecycle
  public CompletableFuture<AsyncConnectionPool<T>> openAsync() {
    return CompletableFuture.supplyAsync(
        () -> {
          if (closed) {
            throw new PoolClosedError("pool '" + name + "' is closed");
          }
          if (opened) {
            return this;
          }
          opened = true;
          fillToMin();
          // fillToMin blocks, so close() may have run meanwhile. Start the maintenance thread only
          // while still open, under the lock, so a completed close() can never miss stopping it.
          lock.lock();
          try {
            if (cfg.maintenanceInterval() != null && !closed) {
              thread = new Thread(this::maintenanceLoop, "connpool-" + name);
              thread.setDaemon(true);
              thread.start();
            }
          } finally {
            lock.unlock();
          }
          return this;
        },
        AsyncScheduler.EXECUTOR);
  }

  public CompletableFuture<Void> closeAsync() {
    return closeAsync(null);
  }

  public CompletableFuture<Void> closeAsync(Double timeout) {
    return CompletableFuture.runAsync(
        () -> {
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
        },
        AsyncScheduler.EXECUTOR);
  }

  public boolean closed() {
    return closed;
  }

  // --------------------------------------------------------------------- usage
  public CompletableFuture<T> acquireAsync() {
    return acquireAsync(Timeout.unset(), CancelToken.NONE);
  }

  public CompletableFuture<T> acquireAsync(Timeout timeout, CancelToken token) {
    return CompletableFuture.supplyAsync(() -> acquire(timeout, token), AsyncScheduler.EXECUTOR);
  }

  private T acquire(Timeout timeout, CancelToken token) {
    double started = clock.monotonic();
    Double deadline = Timeout.computeDeadline(timeout, cfg.acquireTimeout(), started);
    PooledConnection<T> pooled;
    try {
      while (true) {
        PooledConnection<T> taken = takeIdleOrReserve(deadline, token);
        if (taken == null) {
          Created<T> created = create(deadline, token);
          registerNew(created.conn(), created.duration());
          pooled = created.conn();
          break;
        }
        CloseReason reason;
        try {
          reason = borrowReason(taken, token);
        } catch (RuntimeException e) {
          destroy(taken, CloseReason.ERROR);
          throw e;
        }
        if (reason == null) {
          pooled = taken;
          break;
        }
        destroy(taken, reason);
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

  public CompletableFuture<Void> releaseAsync(T conn) {
    return releaseAsync(conn, false, null, CancelToken.NONE);
  }

  public CompletableFuture<Void> releaseAsync(
      T conn, boolean discard, Throwable error, CancelToken token) {
    return CompletableFuture.runAsync(
        () -> release(conn, discard, error, token), AsyncScheduler.EXECUTOR);
  }

  private void release(T conn, boolean discard, Throwable error, CancelToken token) {
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
    CloseReason reason;
    try {
      reason = releaseReason(pooled, conn, discard, error, now, token);
      if (reason == null && !park(pooled, now)) {
        reason = CloseReason.POOL_CLOSED;
      }
    } catch (RuntimeException e) {
      destroy(pooled, CloseReason.ERROR);
      throw e;
    }
    if (reason != null) {
      destroy(pooled, reason);
    }
    PooledConnection<T> fp = pooled;
    events.dispatch(l -> l.onReleased(fp, hold));
  }

  public CompletableFuture<Void> useAsync(
      Function<T, CompletableFuture<Void>> body, Timeout timeout, CancelToken token) {
    return acquireAsync(timeout, token)
        .thenCompose(
            conn -> {
              // Guard a body that throws synchronously (instead of returning a failed future) so
              // the
              // connection is always released - otherwise it would leak.
              CompletableFuture<Void> work;
              try {
                work = body.apply(conn);
              } catch (Throwable t) {
                work = failed(t);
              }
              return work.handle((v, ex) -> ex)
                  .thenCompose(
                      ex ->
                          ex != null
                              ? releaseAsync(conn, false, unwrap(ex), token)
                                  .thenCompose(ignored -> failed(ex))
                              : releaseAsync(conn, false, null, token));
            });
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
    return stats.snapshot(
        name,
        cfg.maxSize(),
        sz,
        idleCount,
        inUse,
        waiting,
        breaker != null ? breaker.state().value() : null);
  }

  public CompletableFuture<Void> runMaintenanceAsync() {
    return CompletableFuture.runAsync(this::runMaintenance, AsyncScheduler.EXECUTOR);
  }

  void runMaintenance() {
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
      boolean healthy = isHealthy(pooled.raw(), CancelToken.NONE);
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
      LOG.log(
          Level.WARNING,
          String.format(
              Locale.ROOT,
              "pool '%s': possible connection leak: %s held for %.1fs",
              name,
              pooled,
              h));
      events.dispatch(l -> l.onConnectionLeak(pooled, h));
    }
  }

  // ----------------------------------------------------------------- internals
  private PooledConnection<T> takeIdleOrReserve(Double deadline, CancelToken token) {
    Registration reg = token.canBeCancelled() ? token.register(this::wakeAll) : Registration.NOOP;
    lock.lock();
    try {
      while (true) {
        token.throwIfCancellationRequested();
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
      reg.close();
    }
  }

  private void wakeAll() {
    lock.lock();
    try {
      cond.signalAll();
    } finally {
      lock.unlock();
    }
  }

  // Caller must hold the lock. null seconds = wait forever.
  private void awaitCond(Double seconds) {
    Threads.awaitCondition(cond, seconds, name);
  }

  private CloseReason borrowReason(PooledConnection<T> pooled, CancelToken token) {
    double now = clock.monotonic();
    CloseReason reason = Lifecycle.borrowReason(pooled, cfg, now);
    if (reason == null
        && Lifecycle.needsValidation(pooled, cfg, now)
        && !isHealthy(pooled.raw(), token)) {
      return CloseReason.UNHEALTHY;
    }
    return reason;
  }

  private CloseReason releaseReason(
      PooledConnection<T> pooled,
      T conn,
      boolean discard,
      Throwable error,
      double now,
      CancelToken token) {
    if (discard) {
      return CloseReason.DISCARDED;
    }
    if (error != null && (factory.isFatalError(error) || !isHealthy(conn, token))) {
      return CloseReason.ERROR;
    }
    CloseReason reason = Lifecycle.rotationReason(pooled, cfg, now);
    if (reason != null) {
      return reason;
    }
    try {
      AsyncAwait.await(factory.resetAsync(conn, token), token);
    } catch (CancellationException e) {
      throw e;
    } catch (RuntimeException ex) {
      LOG.log(Level.WARNING, "pool '" + name + "': reset failed; discarding " + pooled, ex);
      return CloseReason.RESET_FAILED;
    }
    return null;
  }

  private boolean isHealthy(T raw, CancelToken token) {
    try {
      return AsyncAwait.await(factory.validateAsync(raw, token), token);
    } catch (CancellationException e) {
      throw e;
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
  private record Created<C>(PooledConnection<C> conn, double duration) {}

  private Created<T> create(Double deadline, CancelToken token) {
    double started = clock.monotonic();
    T raw;
    try {
      raw = createRaw(deadline, token);
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
          "pool '"
              + name
              + "': factory.createAsync() returned an object the pool already manages; "
              + "each createAsync() must return a distinct connection");
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

  private T createRaw(Double deadline, CancelToken token) {
    if (breaker != null) {
      breaker.beforeCall();
    }
    T raw;
    try {
      raw = createWithRetry(deadline, token);
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

  // The create cancellation limit (seconds), plus whether the acquire deadline is the binding
  // constraint - so a timeout can be classified without a clock re-read that could flake.
  private record CreateLimit(Double limit, boolean deadlineBinding) {}

  private CreateLimit createLimit(Double deadline) {
    Double rem = Timeout.remaining(deadline, clock.monotonic());
    Double createTimeout = cfg.createTimeout();
    boolean deadlineBinding = rem != null && (createTimeout == null || rem <= createTimeout);
    Double limit;
    if (createTimeout == null && rem == null) {
      limit = null;
    } else if (createTimeout == null) {
      limit = Math.max(0.0, rem);
    } else if (rem == null) {
      limit = Math.max(0.0, createTimeout);
    } else {
      limit = Math.max(0.0, Math.min(createTimeout, rem));
    }
    return new CreateLimit(limit, deadlineBinding);
  }

  private T createWithRetry(Double deadline, CancelToken token) {
    RetryPolicy retry = cfg.retry();
    int attempt = 0;
    while (true) {
      if (closed) {
        throw new PoolClosedError("pool '" + name + "' is closed");
      }
      attempt += 1;
      CreateLimit cl = createLimit(deadline);
      CancelTokenSource linked = CancelTokenSource.linkedWithTimeout(token, cl.limit());
      try {
        return AsyncAwait.await(factory.createAsync(linked.token()), linked.token());
      } catch (RuntimeException exc) {
        // External cancellation is caller impatience, not a backend fault: propagate it.
        token.throwIfCancellationRequested();
        Double left = Timeout.remaining(deadline, clock.monotonic());
        boolean wasTimeout = exc instanceof CancellationException;
        // A timeout whose binding limit was the acquire deadline is the deadline being
        // reached, not a backend fault - surface it as PoolTimeoutError. Decided by which
        // limit bound this attempt, not by a clock re-read, so it cannot flake.
        if (wasTimeout && cl.deadlineBinding()) {
          throw new PoolTimeoutError(
              "pool '" + name + "': acquire deadline reached while creating connection");
        }
        events.dispatch(l -> l.onCreateFailed(exc));
        if (attempt >= retry.maxAttempts() || !retry.shouldRetry(exc)) {
          throw new ConnectionCreateError(
              "pool '"
                  + name
                  + "': could not create connection after "
                  + attempt
                  + " attempt(s): "
                  + describe(exc),
              exc);
        }
        double delay = retry.delayFor(attempt, rng);
        if (left != null && delay >= left) {
          throw new PoolTimeoutError(
              "pool '" + name + "': acquire deadline reached while retrying creation");
        }
        AsyncAwait.await(clock.asyncSleep(delay, token), token);
      } finally {
        linked.close();
      }
    }
  }

  private static String describe(Throwable exc) {
    String msg = exc.getMessage();
    return (msg == null || msg.isEmpty()) ? exc.getClass().getSimpleName() : msg;
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

  /**
   * Dispose of a connection exactly once. The backend disposal runs to completion uncancellably, so
   * a cancellation can neither interrupt it, skip the slot decrement, nor drop the
   * onConnectionClosed event.
   */
  private void destroy(PooledConnection<T> pooled, CloseReason reason) {
    try {
      AsyncAwait.join(factory.closeAsync(pooled.raw(), CancelToken.NONE));
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

  void fillToMin() {
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
        created = create(null, CancelToken.NONE);
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

  void maintenanceTick() {
    try {
      runMaintenance();
    } catch (RuntimeException ex) {
      LOG.log(Level.ERROR, "pool '" + name + "': maintenance pass failed", ex);
    }
  }

  // test seams
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
      LOG.log(
          Level.WARNING, "pool '" + name + "' was not closed; " + size + " connection(s) may leak");
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    PoolStats s = stats();
    return "<AsyncConnectionPool '"
        + name
        + "' size="
        + s.size()
        + "/"
        + s.maxSize()
        + " idle="
        + s.idle()
        + " in_use="
        + s.inUse()
        + ">";
  }

  private static Throwable unwrap(Throwable ex) {
    // A CompletionException from a CompletableFuture always carries the real cause.
    return ex instanceof java.util.concurrent.CompletionException ? ex.getCause() : ex;
  }

  private static <X> CompletableFuture<X> failed(Throwable ex) {
    CompletableFuture<X> f = new CompletableFuture<>();
    f.completeExceptionally(unwrap(ex));
    return f;
  }
}
