package io.github.jeffeek.connpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jeffeek.connpool.TestSupport.AuthError;
import io.github.jeffeek.connpool.TestSupport.ConnErr;
import io.github.jeffeek.connpool.TestSupport.ConnRefused;
import io.github.jeffeek.connpool.TestSupport.ConnReset;
import io.github.jeffeek.connpool.TestSupport.FakeConn;
import io.github.jeffeek.connpool.TestSupport.FakeFactory;
import io.github.jeffeek.connpool.TestSupport.Recorder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SyncPoolTest {
    private final List<ConnectionPool<FakeConn>> pools = new ArrayList<>();
    private ConnectionFactory<FakeConn> factory;
    private Recorder rec;

    private ConnectionPool<FakeConn> make(ConnectionFactory<FakeConn> f, ManualClock clock, PoolConfig cfg) {
        factory = f != null ? f : new FakeFactory();
        rec = new Recorder();
        ConnectionPool<FakeConn> pool = new ConnectionPool<>(
            factory, cfg != null ? cfg : TestSupport.quietConfig(), List.of(rec), clock, null, "pool");
        pools.add(pool);
        return pool;
    }

    private ConnectionPool<FakeConn> make() {
        return make(null, null, null);
    }

    private ConnectionPool<FakeConn> make(PoolConfig cfg) {
        return make(null, null, cfg);
    }

    private FakeFactory fake() {
        return (FakeFactory) factory;
    }

    @AfterEach
    void cleanup() {
        for (ConnectionPool<FakeConn> pool : pools) {
            try {
                pool.close();
            } catch (RuntimeException ignored) {
                // best effort
            }
        }
    }

    private static Thread run(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.start();
        return t;
    }

    // ----------------------------------------------------------------- basics
    @Test
    void connectionIsReused() {
        ConnectionPool<FakeConn> pool = make();
        FakeConn a = pool.use(c -> c);
        FakeConn b = pool.use(c -> c);
        assertSame(a, b);
        assertEquals(1, fake().created.size());
        assertEquals(2, a.resets);
    }

    @Test
    void maxSizeIsHardCapAndAcquireTimesOut() {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).build());
        FakeConn held = pool.acquire();
        assertThrows(PoolTimeoutError.class, () -> pool.acquire(Timeout.of(0.05)));
        assertEquals(1, pool.stats().timeouts());
        pool.release(held);
    }

    @Test
    void zeroTimeoutIsNonBlocking() {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).build());
        FakeConn held = pool.acquire();
        assertThrows(PoolTimeoutError.class, () -> pool.acquire(Timeout.of(0)));
        pool.release(held);
    }

    @Test
    void waiterGetsConnectionWhenReleased() throws Exception {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).build());
        FakeConn held = pool.acquire();
        AtomicReference<FakeConn> got = new AtomicReference<>();
        Thread t = run(() -> got.set(pool.acquire(Timeout.of(2))));
        TestSupport.spinUntil(() -> pool.stats().waiting() == 1);
        assertEquals(1, pool.stats().waiting());
        pool.release(held);
        t.join(2000);
        assertSame(held, got.get());
    }

    @Test
    void doubleAndForeignReleaseRejected() {
        ConnectionPool<FakeConn> pool = make();
        FakeConn conn = pool.acquire();
        pool.release(conn);
        assertThrows(InvalidReleaseError.class, () -> pool.release(conn));
        assertThrows(InvalidReleaseError.class, () -> pool.release(new FakeConn()));
    }

    @Test
    void idleStrategyLifoVsFifo() {
        for (IdleStrategy strategy : new IdleStrategy[] {IdleStrategy.LIFO, IdleStrategy.FIFO}) {
            ConnectionPool<FakeConn> pool = make(TestSupport.quiet().idleStrategy(strategy).build());
            FakeConn[] conns = {pool.acquire(), pool.acquire(), pool.acquire()};
            for (FakeConn c : conns) {
                pool.release(c);
            }
            assertSame(conns[strategy == IdleStrategy.LIFO ? 2 : 0], pool.acquire());
        }
    }

    static final class Thing implements AutoCloseable {
        boolean closed;

        @Override
        public void close() {
            closed = true;
        }
    }

    @Test
    void fromCallablesAndDefaultClose() {
        ConnectionPool<Thing> pool = new ConnectionPool<>(
            CallableFactory.<Thing>builder(Thing::new).build(), TestSupport.quietConfig());
        Thing t = pool.use(c -> c);
        pool.close();
        assertTrue(t.closed);
    }

    @Test
    void explicitDiscard() {
        ConnectionPool<FakeConn> pool = make();
        FakeConn conn = pool.acquire();
        pool.release(conn, true, null);
        assertTrue(conn.closed);
        assertEquals(0, pool.stats().size());
        assertEquals(List.of(CloseReason.DISCARDED), rec.closedSnapshot());
    }

    @Test
    void validateIdleEvictsDeadConnections() {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().validateIdle(true).maxSize(2).build());
        FakeConn a = pool.acquire();
        FakeConn b = pool.acquire();
        pool.release(a);
        pool.release(b);
        assertEquals(2, pool.stats().idle());
        a.healthy = false;
        pool.runMaintenance();
        assertEquals(1, pool.stats().idle());
        assertTrue(rec.closedSnapshot().contains(CloseReason.UNHEALTHY));
        assertSame(b, pool.use(x -> x));
    }

    @Test
    void defaultConfigWhenOmitted() {
        ConnectionPool<FakeConn> pool = new ConnectionPool<>(new FakeFactory());
        pools.add(pool);
        FakeConn c = pool.acquire();
        pool.release(c);
        assertEquals(1, pool.stats().size());
    }

    @Test
    void healthCheckClosedMidValidationReportsPoolClosed() {
        AtomicReference<ConnectionPool<FakeConn>> ref = new AtomicReference<>();
        boolean[] closedOnce = {false};
        ConnectionFactory<FakeConn> f = CallableFactory.<FakeConn>builder(FakeConn::new)
            .validate(c -> {
                if (!closedOnce[0]) {
                    closedOnce[0] = true;
                    ref.get().close();
                }
                return true;
            }).build();
        ConnectionPool<FakeConn> pool = make(f, null, TestSupport.quiet().validateIdle(true).build());
        ref.set(pool);
        FakeConn a = pool.acquire();
        pool.release(a);
        pool.runMaintenance();
        assertTrue(rec.closedSnapshot().contains(CloseReason.POOL_CLOSED));
    }

    @Test
    void retrySleepsWhenAcquireTimeoutIsInfinite() {
        FakeFactory f = new FakeFactory();
        f.failTimes = 1;
        ConnectionPool<FakeConn> pool = make(f, new ManualClock(), TestSupport.quiet()
            .acquireTimeout(null)
            .retry(RetryPolicy.builder().maxAttempts(3).baseDelay(1).jitter(0).build())
            .build());
        FakeConn c = pool.acquire();
        assertEquals(2, f.attempts);
        assertTrue(c != null);
    }

    @Test
    void duplicateConnectionFromFactoryIsRejected() {
        FakeConn shared = new FakeConn();
        ConnectionPool<FakeConn> pool = make(CallableFactory.<FakeConn>builder(() -> shared).build(), null, null);
        FakeConn first = pool.acquire();
        assertThrows(ConnectionCreateError.class, pool::acquire);
        assertEquals(1, pool.stats().size());
        assertEquals(1, pool.stats().created());
        pool.release(first);
        assertEquals(0, pool.stats().inUse());
    }

    // ----------------------------------------------------------------- rotation
    @Test
    void expiredConnectionIsNotReused() {
        ManualClock clock = new ManualClock();
        ConnectionPool<FakeConn> pool = make(null, clock, TestSupport.quiet().maxLifetime(100.0).build());
        FakeConn a = pool.acquire();
        pool.release(a);
        clock.advance(101);
        FakeConn b = pool.acquire();
        assertNotSame(a, b);
        assertTrue(a.closed);
        assertEquals(List.of(CloseReason.EXPIRED), rec.closedSnapshot());
        pool.release(b);
    }

    @Test
    void connectionExpiringWhileInUseIsClosedOnRelease() {
        ManualClock clock = new ManualClock();
        ConnectionPool<FakeConn> pool = make(null, clock, TestSupport.quiet().maxLifetime(100.0).build());
        FakeConn a = pool.acquire();
        clock.advance(150);
        pool.release(a);
        assertTrue(a.closed);
        assertEquals(0, pool.stats().idle());
    }

    @Test
    void maxUsesRotation() {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxUses(2).build());
        FakeConn first = pool.acquire();
        pool.release(first);
        FakeConn again = pool.acquire();
        pool.release(again);
        assertSame(first, again);
        assertTrue(first.closed);
        FakeConn third = pool.acquire();
        assertNotSame(third, first);
        assertEquals(List.of(CloseReason.MAX_USES), rec.closedSnapshot());
        pool.release(third);
    }

    @Test
    void idleExpiredConnectionDroppedOnBorrow() {
        ManualClock clock = new ManualClock();
        ConnectionPool<FakeConn> pool = make(null, clock, TestSupport.quiet().maxIdleTime(10.0).build());
        FakeConn a = pool.acquire();
        pool.release(a);
        clock.advance(11);
        FakeConn b = pool.acquire();
        assertNotSame(a, b);
        assertEquals(List.of(CloseReason.IDLE), rec.closedSnapshot());
        pool.release(b);
    }

    @Test
    void idleEvictionRespectsMinSize() {
        ManualClock clock = new ManualClock();
        ConnectionPool<FakeConn> pool = make(null, clock, TestSupport.quiet().minSize(1).maxIdleTime(10.0).build());
        FakeConn[] conns = {pool.acquire(), pool.acquire(), pool.acquire()};
        for (FakeConn c : conns) {
            pool.release(c);
        }
        clock.advance(11);
        pool.runMaintenance();
        assertEquals(1, pool.stats().idle());
        long idleClosed = rec.closedSnapshot().stream().filter(r -> r == CloseReason.IDLE).count();
        assertEquals(2, idleClosed);
    }

    @Test
    void maintenanceReplacesExpiredToKeepMinSize() {
        ManualClock clock = new ManualClock();
        ConnectionPool<FakeConn> pool = make(null, clock, TestSupport.quiet().minSize(2).maxLifetime(50.0).build());
        pool.open();
        assertEquals(2, pool.stats().idle());
        clock.advance(51);
        pool.runMaintenance();
        assertEquals(2, pool.stats().idle());
        assertEquals(4, fake().created.size());
        assertEquals(2, fake().created.stream().filter(c -> c.closed).count());
    }

    @Test
    void backgroundMaintenanceThread() {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maintenanceInterval(0.02).maxIdleTime(0.05).build());
        pool.open();
        pool.release(pool.acquire());
        TestSupport.spinUntil(() -> pool.stats().size() == 0, 3);
        assertEquals(0, pool.stats().size());
    }

    // ----------------------------------------------------------------- health
    @Test
    void unhealthyIdleConnectionReplacedOnBorrow() {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().validateAfterIdle(0.0).build());
        FakeConn a = pool.acquire();
        pool.release(a);
        a.healthy = false;
        FakeConn b = pool.acquire();
        assertNotSame(a, b);
        assertTrue(a.closed);
        assertEquals(List.of(CloseReason.UNHEALTHY), rec.closedSnapshot());
        pool.release(b);
    }

    @Test
    void validationSkippedWhenRecentlyUsed() {
        ManualClock clock = new ManualClock();
        ConnectionPool<FakeConn> pool = make(null, clock, TestSupport.quiet().validateAfterIdle(30.0).build());
        FakeConn a = pool.acquire();
        pool.release(a);
        a.healthy = false;
        assertSame(a, pool.acquire());
    }

    @Test
    void fatalErrorDiscardsConnection() {
        FakeFactory f = new FakeFactory();
        f.fatalTypes.add(ConnReset.class);
        ConnectionPool<FakeConn> pool = make(f, null, null);
        AtomicReference<FakeConn> captured = new AtomicReference<>();
        assertThrows(ConnReset.class, () -> pool.use((java.util.function.Consumer<FakeConn>) c -> {
            captured.set(c);
            throw new ConnReset("x");
        }));
        assertTrue(captured.get().closed);
        assertEquals(List.of(CloseReason.ERROR), rec.closedSnapshot());
    }

    @Test
    void benignErrorKeepsHealthyConnection() {
        ConnectionPool<FakeConn> pool = make();
        AtomicReference<FakeConn> captured = new AtomicReference<>();
        assertThrows(IllegalArgumentException.class, () -> pool.use((java.util.function.Consumer<FakeConn>) c -> {
            captured.set(c);
            throw new IllegalArgumentException("app bug");
        }));
        assertFalse(captured.get().closed);
        assertEquals(1, pool.stats().idle());
    }

    @Test
    void errorPlusFailedValidationDiscards() {
        ConnectionPool<FakeConn> pool = make();
        AtomicReference<FakeConn> captured = new AtomicReference<>();
        assertThrows(IllegalArgumentException.class, () -> pool.use((java.util.function.Consumer<FakeConn>) c -> {
            captured.set(c);
            c.healthy = false;
            throw new IllegalArgumentException();
        }));
        assertTrue(captured.get().closed);
    }

    @Test
    void resetFailureDiscards() {
        FakeFactory f = new FakeFactory();
        ConnectionPool<FakeConn> pool = make(f, null, null);
        f.resetRaises = true;
        FakeConn captured = pool.use(c -> c);
        assertTrue(captured.closed);
        assertEquals(List.of(CloseReason.RESET_FAILED), rec.closedSnapshot());
    }

    static final class BadCloseFactory extends FakeFactory {
        @Override
        public void close(FakeConn conn) {
            throw new RuntimeException("close failed");
        }
    }

    @Test
    void closeErrorsDoNotLeakSlots() {
        ConnectionPool<FakeConn> pool = make(new BadCloseFactory(), null, null);
        pool.release(pool.acquire(), true, null);
        assertEquals(0, pool.stats().size());
    }

    @Test
    void validationErrorIsTreatedAsUnhealthy() {
        ConnectionPool<FakeConn> pool = make(
            CallableFactory.<FakeConn>builder(FakeConn::new).validate(c -> {
                throw new IllegalStateException("ping blew up");
            }).build(),
            null, TestSupport.quiet().validateAfterIdle(0.0).build());
        FakeConn a = pool.acquire();
        pool.release(a);
        FakeConn b = pool.acquire();
        assertNotSame(a, b);
        assertEquals(List.of(CloseReason.UNHEALTHY), rec.closedSnapshot());
        pool.release(b);
    }

    // ----------------------------------------------------------------- resilience
    @Test
    void retryWithBackoffSucceeds() {
        FakeFactory f = new FakeFactory();
        f.failTimes = 2;
        ManualClock clock = new ManualClock();
        ConnectionPool<FakeConn> pool = make(f, clock, TestSupport.quiet()
            .retry(RetryPolicy.builder().maxAttempts(3).baseDelay(1).multiplier(2).jitter(0).build())
            .acquireTimeout(60.0).build());
        double start = clock.monotonic();
        pool.acquire();
        assertEquals(3, f.attempts);
        assertEquals(3.0, clock.monotonic() - start);
    }

    @Test
    void exhaustedRetriesRaiseAndDoNotLeakSlot() {
        FakeFactory f = new FakeFactory();
        f.failAlways = true;
        ConnectionPool<FakeConn> pool = make(f, new ManualClock(), TestSupport.quiet()
            .retry(RetryPolicy.builder().maxAttempts(2).jitter(0).build()).build());
        ConnectionCreateError ex = assertThrows(ConnectionCreateError.class, pool::acquire);
        assertTrue(ex.getCause() instanceof ConnRefused);
        assertEquals(2, f.attempts);
        assertEquals(0, pool.stats().size());
        assertEquals(2, pool.stats().createFailures());
    }

    static final class AuthFailFactory extends FakeFactory {
        @Override
        public FakeConn create() {
            attempts += 1;
            throw new AuthError("bad password");
        }
    }

    @Test
    void nonRetryableErrorFailsImmediately() {
        AuthFailFactory f = new AuthFailFactory();
        ConnectionPool<FakeConn> pool = make(f, new ManualClock(), TestSupport.quiet()
            .retry(RetryPolicy.builder().maxAttempts(5).retryOn(e -> e instanceof ConnErr).build()).build());
        assertThrows(ConnectionCreateError.class, pool::acquire);
        assertEquals(1, f.attempts);
    }

    @Test
    void retryNeverSleepsPastAcquireDeadline() {
        FakeFactory f = new FakeFactory();
        f.failAlways = true;
        ConnectionPool<FakeConn> pool = make(f, new ManualClock(), TestSupport.quiet()
            .acquireTimeout(1.0)
            .retry(RetryPolicy.builder().maxAttempts(5).baseDelay(5).jitter(0).build()).build());
        assertThrows(PoolTimeoutError.class, pool::acquire);
        assertEquals(1, f.attempts);
    }

    @Test
    void circuitBreakerFullCycle() {
        FakeFactory f = new FakeFactory();
        f.failAlways = true;
        ManualClock clock = new ManualClock();
        ConnectionPool<FakeConn> pool = make(f, clock, TestSupport.quiet()
            .breaker(BreakerConfig.builder().failureThreshold(2).recoveryTimeout(30).build()).build());
        for (int i = 0; i < 2; i++) {
            assertThrows(ConnectionCreateError.class, pool::acquire);
        }
        int attempts = f.attempts;
        assertThrows(CircuitOpenError.class, pool::acquire);
        assertEquals(attempts, f.attempts);
        assertEquals("open", pool.stats().breakerState());

        clock.advance(31);
        f.failAlways = false;
        FakeConn conn = pool.acquire();
        assertEquals("closed", pool.stats().breakerState());
        assertEquals(1, rec.countEvent("breaker:open->half_open"));
        assertEquals(1, pool.stats().size());
        pool.release(conn);
    }

    @Test
    void failedProbeReopensBreaker() {
        FakeFactory f = new FakeFactory();
        f.failAlways = true;
        ManualClock clock = new ManualClock();
        ConnectionPool<FakeConn> pool = make(f, clock, TestSupport.quiet()
            .breaker(BreakerConfig.builder().failureThreshold(1).recoveryTimeout(5).build()).build());
        assertThrows(ConnectionCreateError.class, pool::acquire);
        clock.advance(6);
        assertThrows(ConnectionCreateError.class, pool::acquire);
        assertThrows(CircuitOpenError.class, pool::acquire);
    }

    @Test
    void breakerCountsOneFailurePerAcquireNotPerAttempt() {
        FakeFactory f = new FakeFactory();
        f.failAlways = true;
        ManualClock clock = new ManualClock();
        ConnectionPool<FakeConn> pool = make(f, clock, TestSupport.quiet()
            .acquireTimeout(60.0)
            .retry(RetryPolicy.builder().maxAttempts(3).baseDelay(1).jitter(0).build())
            .breaker(BreakerConfig.builder().failureThreshold(2).recoveryTimeout(30).build()).build());
        assertThrows(ConnectionCreateError.class, pool::acquire);
        assertEquals(3, f.attempts);
        assertEquals("closed", pool.stats().breakerState());
        assertThrows(ConnectionCreateError.class, pool::acquire);
        assertEquals("open", pool.stats().breakerState());
    }

    @Test
    void createTimeoutDoesNotTripBreaker() {
        FakeFactory f = new FakeFactory();
        f.failAlways = true;
        ManualClock clock = new ManualClock();
        ConnectionPool<FakeConn> pool = make(f, clock, TestSupport.quiet()
            .acquireTimeout(1.0)
            .retry(RetryPolicy.builder().maxAttempts(3).baseDelay(10).jitter(0).build())
            .breaker(BreakerConfig.builder().failureThreshold(1).build()).build());
        assertThrows(PoolTimeoutError.class, pool::acquire);
        assertEquals("closed", pool.stats().breakerState());
    }

    @Test
    void poolClosedMidRetryStopsCreating() {
        AtomicReference<ConnectionPool<FakeConn>> ref = new AtomicReference<>();
        ConnectionFactory<FakeConn> f = CallableFactory.<FakeConn>builder(() -> {
            ref.get().close();
            throw new ConnErr("down");
        }).build();
        ConnectionPool<FakeConn> pool = make(f, null, TestSupport.quiet()
            .retry(RetryPolicy.builder().maxAttempts(3).baseDelay(0).jitter(0).build()).build());
        ref.set(pool);
        assertThrows(PoolClosedError.class, pool::acquire);
    }

    // ----------------------------------------------------- shutdown + observability
    @Test
    void closeSemantics() throws Exception {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).build());
        FakeConn held = pool.acquire();
        List<Throwable> errors = new ArrayList<>();
        Thread t = run(() -> {
            try {
                pool.acquire(Timeout.of(5));
            } catch (PoolClosedError e) {
                synchronized (errors) {
                    errors.add(e);
                }
            }
        });
        TestSupport.spinUntil(() -> pool.stats().waiting() == 1);
        pool.close();
        t.join(2000);
        assertEquals(1, errors.size());
        assertFalse(held.closed);
        pool.release(held);
        assertTrue(held.closed);
        assertThrows(PoolClosedError.class, pool::acquire);
        pool.close(); // idempotent
    }

    @Test
    void closeClosesIdleConnections() {
        ConnectionPool<FakeConn> pool = make();
        FakeConn c = pool.acquire();
        pool.release(c);
        pool.close();
        assertTrue(c.closed);
    }

    @Test
    void contextManagerOpensAndCloses() {
        FakeFactory f = new FakeFactory();
        ConnectionPool<FakeConn> pool = new ConnectionPool<>(f, TestSupport.quiet().minSize(2).build());
        pools.add(pool);
        pool.open();
        assertEquals(2, pool.stats().idle());
        pool.close();
        assertTrue(pool.closed());
        assertTrue(f.created.stream().allMatch(c -> c.closed));
    }

    @Test
    void warmupFailureIsNotFatal() {
        FakeFactory f = new FakeFactory();
        f.failAlways = true;
        ConnectionPool<FakeConn> pool = make(f, null, TestSupport.quiet().minSize(2).build());
        pool.open();
        assertEquals(0, pool.stats().size());
    }

    @Test
    void statsAndEvents() {
        ConnectionPool<FakeConn> pool = make();
        pool.use(ignored -> {
            PoolStats s = pool.stats();
            assertEquals(1, s.size());
            assertEquals(0, s.idle());
            assertEquals(1, s.inUse());
        });
        PoolStats s2 = pool.stats();
        assertEquals(1, s2.created());
        assertEquals(1, s2.acquired());
        assertEquals(1, s2.released());
        assertEquals(1, s2.idle());
        assertEquals(0, s2.inUse());
        assertEquals(List.of("created", "acquired", "released"), rec.events);
    }

    static final class BoomListener extends PoolListener {
        @Override
        public void onAcquired(PooledConnection<?> conn, double waitTime) {
            throw new IllegalStateException("metrics down");
        }
    }

    @Test
    void brokenListenerCannotBreakPool() {
        ConnectionPool<FakeConn> pool = new ConnectionPool<>(
            new FakeFactory(), TestSupport.quietConfig(), List.of(new BoomListener()), null, null, "pool");
        pools.add(pool);
        pool.use(ignored -> { });
    }

    @Test
    void connectionCreatedDuringCloseDoesNotLeak() {
        AtomicReference<ConnectionPool<FakeConn>> ref = new AtomicReference<>();
        List<FakeConn> made = new ArrayList<>();
        ConnectionFactory<FakeConn> f = CallableFactory.<FakeConn>builder(() -> {
            FakeConn c = new FakeConn();
            made.add(c);
            ref.get().close();
            return c;
        }).close(c -> c.closed = true).build();
        ConnectionPool<FakeConn> pool = make(f, null, null);
        ref.set(pool);
        assertThrows(PoolClosedError.class, pool::acquire);
        assertEquals(1, made.size());
        assertTrue(made.get(0).closed);
        assertEquals(0, pool.stats().size());
    }

    @Test
    void gracefulDrainWaitsForInUse() throws Exception {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(2).build());
        FakeConn held = pool.acquire();
        CountDownLatch released = new CountDownLatch(1);
        run(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            pool.release(held);
            released.countDown();
        });
        pool.close(2.0);
        assertTrue(released.await(2, TimeUnit.SECONDS));
        assertTrue(held.closed);
        assertEquals(0, pool.stats().inUse());
    }

    @Test
    void gracefulDrainGivesUpAfterTimeout() {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).build());
        FakeConn held = pool.acquire();
        pool.close(0.05);
        assertEquals(1, pool.stats().inUse());
        assertFalse(held.closed);
        pool.release(held);
        assertTrue(held.closed);
    }

    // ----------------------------------------------------------------- concurrency
    @Test
    void stressRespectsMaxSizeAndLeaksNothing() throws Exception {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(5).acquireTimeout(10.0).maxUses(7).build());
        Object lockObj = new Object();
        int[] now = {0};
        int[] peak = {0};
        List<Throwable> errors = new ArrayList<>();

        Runnable worker = () -> {
            try {
                for (int i = 0; i < 60; i++) {
                    pool.use(conn -> {
                        synchronized (lockObj) {
                            now[0] += 1;
                            peak[0] = Math.max(peak[0], now[0]);
                        }
                        assertFalse(conn.closed);
                        synchronized (lockObj) {
                            now[0] -= 1;
                        }
                    });
                }
            } catch (Throwable exc) {
                synchronized (errors) {
                    errors.add(exc);
                }
            }
        };

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            threads.add(run(worker));
        }
        for (Thread t : threads) {
            t.join(60000);
        }
        assertTrue(errors.isEmpty(), () -> "errors: " + errors);
        assertTrue(peak[0] <= 5);
        PoolStats s = pool.stats();
        assertEquals(0, s.inUse());
        assertEquals(0, s.waiting());
        assertEquals(s.idle(), s.size());
        assertEquals(20 * 60, s.acquired());
        assertEquals(s.idle(), s.created() - s.closed());
        assertTrue(pool.idleSnapshot().stream().noneMatch(c -> c.closed));
    }

    // ----------------------------------------------------------------- leak detection
    @Test
    void leakDetectionWarnsOnce() {
        ManualClock clock = new ManualClock();
        ConnectionPool<FakeConn> pool = make(null, clock, TestSupport.quiet().leakDetectionTimeout(10.0).build());
        FakeConn conn = pool.acquire();
        pool.runMaintenance();
        assertEquals(0, pool.stats().leaks());
        clock.advance(11);
        pool.runMaintenance();
        pool.runMaintenance();
        assertEquals(1, pool.stats().leaks());
        pool.release(conn);
    }

    // ----------------------------------------------------------------- backpressure
    @Test
    void zeroWaitersFailsFastWhenFull() {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).maxWaiters(0).acquireTimeout(5.0).build());
        FakeConn held = pool.acquire();
        assertThrows(PoolExhaustedError.class, pool::acquire);
        pool.release(held);
    }

    @Test
    void waiterQueueIsCapped() throws Exception {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).maxWaiters(1).acquireTimeout(5.0).build());
        FakeConn held = pool.acquire();
        Thread t = run(() -> {
            try {
                pool.release(pool.acquire(Timeout.of(5)));
            } catch (RuntimeException ignored) {
                // ignore
            }
        });
        TestSupport.spinUntil(() -> pool.stats().waiting() >= 1);
        assertThrows(PoolExhaustedError.class, () -> pool.acquire(Timeout.of(5)));
        pool.release(held);
        t.join(2000);
    }

    // ----------------------------------------------------------------- lifecycle edges
    @Test
    void openOnClosedPoolRaises() {
        ConnectionPool<FakeConn> pool = make();
        pool.close();
        assertThrows(PoolClosedError.class, pool::open);
    }

    @Test
    void openIsIdempotent() {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maintenanceInterval(0.5).build());
        assertSame(pool.open(), pool.open());
    }

    @Test
    void runMaintenanceOnClosedPoolIsNoop() {
        ConnectionPool<FakeConn> pool = make();
        pool.close();
        pool.runMaintenance();
    }

    @Test
    void warmupConnectionDestroyedIfClosedDuringCreate() {
        List<FakeConn> made = new ArrayList<>();
        AtomicReference<ConnectionPool<FakeConn>> ref = new AtomicReference<>();
        ConnectionFactory<FakeConn> f = CallableFactory.<FakeConn>builder(() -> {
            FakeConn c = new FakeConn();
            made.add(c);
            ref.get().close();
            return c;
        }).close(c -> c.closed = true).build();
        ConnectionPool<FakeConn> pool = new ConnectionPool<>(f, TestSupport.quiet().minSize(1).build());
        pools.add(pool);
        ref.set(pool);
        pool.open();
        assertTrue(made.size() > 0 && made.get(0).closed);
    }

    @Test
    void reprMentionsSizes() {
        ConnectionPool<FakeConn> pool = make();
        assertTrue(pool.toString().contains("ConnectionPool"));
    }

    @Test
    void leakWarningWhenNotClosed() {
        ConnectionPool<FakeConn> pool = new ConnectionPool<>(new FakeFactory(), TestSupport.quietConfig());
        pools.add(pool);
        assertFalse(pool.warnIfLeaked()); // fresh: not closed, size 0
        pool.acquire();
        assertTrue(pool.warnIfLeaked());
        pool.close();
        assertFalse(pool.warnIfLeaked());
    }

    static final class BoomMaintenancePool extends ConnectionPool<FakeConn> {
        final AtomicInteger calls = new AtomicInteger();

        BoomMaintenancePool(PoolConfig cfg) {
            super(new FakeFactory(), cfg);
        }

        @Override
        public void runMaintenance() {
            calls.incrementAndGet();
            throw new IllegalStateException("sweep broke");
        }
    }

    static final class BlockingMaintenancePool extends ConnectionPool<FakeConn> {
        final CountDownLatch entered = new CountDownLatch(1);

        BlockingMaintenancePool(PoolConfig cfg) {
            super(new FakeFactory(), cfg);
        }

        @Override
        public void runMaintenance() {
            entered.countDown();
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            super.runMaintenance();
        }
    }

    @Test
    void closeJoinsLiveMaintenanceThread() throws Exception {
        BlockingMaintenancePool pool = new BlockingMaintenancePool(TestSupport.quiet().maintenanceInterval(0.01).build());
        pools.add(pool);
        pool.open();
        assertTrue(pool.entered.await(2, TimeUnit.SECONDS));
        pool.close();
        assertTrue(pool.closed());
    }

    @Test
    void disposeViaCloseableClosesPool() {
        ConnectionPool<FakeConn> pool = new ConnectionPool<>(new FakeFactory(), TestSupport.quietConfig());
        pools.add(pool);
        try (ConnectionPool<FakeConn>.Lease lease = pool.connection()) {
            assertTrue(lease.get() != null);
        }
        assertEquals(0, pool.stats().inUse());
        pool.close();
        assertTrue(pool.closed());
    }

    @Test
    void useResultPropagatesError() {
        ConnectionPool<FakeConn> pool = make();
        assertThrows(IllegalArgumentException.class, () -> pool.use((java.util.function.Function<FakeConn, Integer>) c -> {
            throw new IllegalArgumentException();
        }, Timeout.unset()));
        assertEquals(1, pool.stats().idle());
    }

    @Test
    void infiniteWaitWaiterIsServed() throws Exception {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).acquireTimeout(null).build());
        FakeConn held = pool.acquire();
        AtomicReference<FakeConn> got = new AtomicReference<>();
        Thread t = run(() -> got.set(pool.acquire()));
        TestSupport.spinUntil(() -> pool.stats().waiting() == 1);
        pool.release(held);
        t.join(2000);
        assertSame(held, got.get());
    }

    @Test
    void healthyIdleConnectionValidatedAndReused() {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().validateAfterIdle(0.0).build());
        FakeConn a = pool.acquire();
        pool.release(a);
        FakeConn b = pool.acquire();
        assertSame(a, b);
        pool.release(b);
    }

    @Test
    void maintenanceLoopSurvivesRunErrors() throws Exception {
        BoomMaintenancePool pool = new BoomMaintenancePool(TestSupport.quiet().maintenanceInterval(0.01).build());
        pools.add(pool);
        Thread t = new Thread(pool::maintenanceLoop);
        t.setDaemon(true);
        t.start();
        TestSupport.spinUntil(() -> pool.calls.get() > 0);
        pool.signalStop();
        t.join(1000);
        assertTrue(pool.calls.get() > 0);
    }

    @Test
    void maintenanceLoopInterruptStops() throws Exception {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maintenanceInterval(5.0).build());
        Thread t = new Thread(pool::maintenanceLoop);
        t.setDaemon(true);
        t.start();
        TestSupport.spinUntil(t::isAlive);
        t.interrupt();
        t.join(2000);
        assertFalse(t.isAlive());
    }

    @Test
    void explicitRngIsUsed() {
        ConnectionPool<FakeConn> pool = new ConnectionPool<>(
            new FakeFactory(), TestSupport.quietConfig(), null, null, new java.util.Random(0), "p");
        pools.add(pool);
        FakeConn c = pool.acquire();
        pool.release(c);
        assertEquals(1, pool.stats().size());
    }

    @Test
    void waiterInterruptedRaisesPoolError() throws Exception {
        ConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).acquireTimeout(null).build());
        FakeConn held = pool.acquire();
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                pool.acquire();
            } catch (Throwable ex) {
                err.set(ex);
            }
        });
        t.setDaemon(true);
        t.start();
        TestSupport.spinUntil(() -> pool.stats().waiting() == 1);
        t.interrupt();
        t.join(2000);
        assertTrue(err.get() instanceof PoolError, () -> "got " + err.get());
        pool.release(held);
    }

    @Test
    void duplicateIdleConnectionRejectedViaRace() throws Exception {
        FakeConn shared = new FakeConn();
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
        AtomicInteger creates = new AtomicInteger();
        ConnectionFactory<FakeConn> f = new ConnectionFactory<>() {
            @Override
            public FakeConn create() {
                if (creates.incrementAndGet() == 1) {
                    return shared;
                }
                entered.countDown();
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return shared;
            }

            @Override
            public void close(FakeConn c) {
                c.closed = true;
            }
        };
        ConnectionPool<FakeConn> pool = make(f, null, TestSupport.quiet().maxSize(2).build());
        FakeConn a = pool.acquire(); // create #1 -> shared checked out
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread t = run(() -> {
            try {
                pool.acquire(); // reserves a 2nd slot, blocks in create #2
            } catch (Throwable ex) {
                err.set(ex);
            }
        });
        entered.await();
        pool.release(a);  // shared becomes idle (no longer checked out)
        gate.countDown(); // create #2 returns shared -> registerNew sees it in idle
        t.join(2000);
        assertTrue(err.get() instanceof ConnectionCreateError, () -> "got " + err.get());
    }

    static final class SelfClosingPool extends ConnectionPool<FakeConn> {
        volatile boolean done;

        SelfClosingPool(PoolConfig cfg) {
            super(new FakeFactory(), cfg);
        }

        @Override
        public void runMaintenance() {
            if (!done) {
                done = true;
                close();
            }
        }
    }

    @Test
    void closeFromMaintenanceThreadDoesNotSelfJoin() {
        SelfClosingPool pool = new SelfClosingPool(TestSupport.quiet().maintenanceInterval(0.01).build());
        pools.add(pool);
        pool.open();
        TestSupport.spinUntil(pool::closed);
        assertTrue(pool.closed());
    }

    @Test
    void closeWithInterruptedCallerRestoresFlag() throws Exception {
        BlockingMaintenancePool pool = new BlockingMaintenancePool(TestSupport.quiet().maintenanceInterval(0.01).build());
        pools.add(pool);
        pool.open();
        assertTrue(pool.entered.await(2, TimeUnit.SECONDS));
        Thread.currentThread().interrupt();
        pool.close(); // Threads.join sees the interrupt flag and returns, restoring it
        assertTrue(Thread.interrupted()); // flag restored; clear it for later tests
        assertTrue(pool.closed());
    }
}
