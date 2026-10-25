package io.github.jeffeek.connpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jeffeek.connpool.TestSupport.AsyncFakeFactory;
import io.github.jeffeek.connpool.TestSupport.ConnErr;
import io.github.jeffeek.connpool.TestSupport.ConnReset;
import io.github.jeffeek.connpool.TestSupport.FakeConn;
import io.github.jeffeek.connpool.TestSupport.FakeFactory;
import io.github.jeffeek.connpool.TestSupport.Recorder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AsyncPoolTest {
    private final List<AsyncConnectionPool<FakeConn>> pools = new ArrayList<>();
    private FakeFactory inner;
    private Recorder rec;

    private AsyncConnectionPool<FakeConn> make(AsyncConnectionFactory<FakeConn> factory, ManualClock clock, PoolConfig cfg) {
        AsyncFakeFactory af = factory instanceof AsyncFakeFactory ? (AsyncFakeFactory) factory : null;
        inner = af != null ? af.inner : new FakeFactory();
        AsyncConnectionFactory<FakeConn> f = factory != null ? factory : new AsyncFakeFactory(inner, 0.0);
        rec = new Recorder();
        AsyncConnectionPool<FakeConn> pool = new AsyncConnectionPool<>(
            f, cfg != null ? cfg : TestSupport.quietConfig(), List.of(rec), clock, null, "pool");
        pools.add(pool);
        return pool;
    }

    private AsyncConnectionPool<FakeConn> make() {
        return make(null, null, null);
    }

    private AsyncConnectionPool<FakeConn> make(PoolConfig cfg) {
        return make(null, null, cfg);
    }

    @AfterEach
    void cleanup() {
        for (AsyncConnectionPool<FakeConn> pool : pools) {
            try {
                await(pool.closeAsync());
            } catch (RuntimeException ignored) {
                // best effort
            }
        }
    }

    private static <X> X await(CompletableFuture<X> f) {
        try {
            return f.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable c = e.getCause();
            if (c instanceof RuntimeException) {
                throw (RuntimeException) c;
            }
            if (c instanceof Error) {
                throw (Error) c;
            }
            throw new RuntimeException(c);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private static final CompletableFuture<Void> DONE = CompletableFuture.completedFuture(null);

    // ------------------------------------------------------------------ basics
    @Test
    void reuseAndStats() {
        AsyncConnectionPool<FakeConn> pool = make();
        FakeConn a = await(pool.acquireAsync());
        await(pool.releaseAsync(a));
        FakeConn b = await(pool.acquireAsync());
        await(pool.releaseAsync(b));
        assertSame(a, b);
        PoolStats s = pool.stats();
        assertEquals(1, s.created());
        assertEquals(2, s.acquired());
        assertEquals(1, s.idle());
        assertEquals(0, s.inUse());
    }

    @Test
    void timeoutWhenExhausted() {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).build());
        FakeConn held = await(pool.acquireAsync());
        assertThrows(PoolTimeoutError.class, () -> await(pool.acquireAsync(Timeout.of(0.05), CancelToken.NONE)));
        assertEquals(1, pool.stats().timeouts());
        await(pool.releaseAsync(held));
    }

    @Test
    void waiterWokenByRelease() {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).build());
        FakeConn held = await(pool.acquireAsync());
        CompletableFuture<FakeConn> waiter = pool.acquireAsync(Timeout.of(2), CancelToken.NONE);
        TestSupport.spinUntil(() -> pool.stats().waiting() == 1);
        await(pool.releaseAsync(held));
        assertSame(held, await(waiter));
    }

    @Test
    void doubleReleaseRejected() {
        AsyncConnectionPool<FakeConn> pool = make();
        FakeConn conn = await(pool.acquireAsync());
        await(pool.releaseAsync(conn));
        assertThrows(InvalidReleaseError.class, () -> await(pool.releaseAsync(conn)));
    }

    @Test
    void rotationByLifetime() {
        ManualClock clock = new ManualClock();
        AsyncConnectionPool<FakeConn> pool = make(null, clock, TestSupport.quiet().maxLifetime(10.0).build());
        FakeConn a = await(pool.acquireAsync());
        await(pool.releaseAsync(a));
        clock.advance(11);
        FakeConn b = await(pool.acquireAsync());
        assertNotSame(a, b);
        assertEquals(List.of(CloseReason.EXPIRED), rec.closedSnapshot());
        await(pool.releaseAsync(b));
    }

    @Test
    void validationAndErrorDiscard() {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().validateAfterIdle(0.0).build());
        FakeConn a = await(pool.acquireAsync());
        await(pool.releaseAsync(a));
        a.healthy = false;
        FakeConn b = await(pool.acquireAsync());
        assertNotSame(a, b);
        await(pool.releaseAsync(b));

        inner.fatalTypes.add(ConnReset.class);
        AtomicReference<FakeConn> captured = new AtomicReference<>();
        assertThrows(ConnReset.class, () -> await(pool.useAsync(c -> {
            captured.set(c);
            return CompletableFuture.failedFuture(new ConnReset("x"));
        }, Timeout.unset(), CancelToken.NONE)));
        assertTrue(captured.get().closed);
    }

    @Test
    void retryAndBreaker() {
        FakeFactory f = new FakeFactory();
        f.failAlways = true;
        ManualClock clock = new ManualClock();
        AsyncConnectionPool<FakeConn> pool = make(new AsyncFakeFactory(f, 0.0), clock, TestSupport.quiet()
            .acquireTimeout(60.0)
            .retry(RetryPolicy.builder().maxAttempts(2).baseDelay(1).jitter(0).build())
            .breaker(BreakerConfig.builder().failureThreshold(2).recoveryTimeout(30).build()).build());
        assertThrows(ConnectionCreateError.class, () -> await(pool.acquireAsync()));
        assertEquals(2, f.attempts);
        assertEquals("closed", pool.stats().breakerState());
        assertThrows(ConnectionCreateError.class, () -> await(pool.acquireAsync()));
        assertEquals(4, f.attempts);
        assertThrows(CircuitOpenError.class, () -> await(pool.acquireAsync()));
        assertEquals(4, f.attempts);
        assertEquals(0, pool.stats().size());
        clock.advance(31);
        f.failAlways = false;
        await(pool.releaseAsync(await(pool.acquireAsync())));
        assertEquals("closed", pool.stats().breakerState());
    }

    @Test
    void createTimeoutIsEnforced() {
        AsyncConnectionPool<FakeConn> pool = make(new AsyncFakeFactory(new FakeFactory(), 5.0), null,
            TestSupport.quiet().createTimeout(0.05).build());
        assertThrows(ConnectionCreateError.class, () -> await(pool.acquireAsync()));
        TestSupport.spinUntil(() -> pool.stats().size() == 0);
        assertEquals(0, pool.stats().size());
    }

    @Test
    void cancelWhileWaitingDoesNotLeak() {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).acquireTimeout(null).build());
        FakeConn held = await(pool.acquireAsync());
        CancelTokenSource cts = new CancelTokenSource();
        CompletableFuture<FakeConn> waiter = pool.acquireAsync(Timeout.unset(), cts.token());
        TestSupport.spinUntil(() -> pool.stats().waiting() == 1);
        cts.cancel();
        assertThrows(CancellationException.class, () -> await(waiter));
        TestSupport.spinUntil(() -> pool.stats().waiting() == 0);
        assertEquals(0, pool.stats().waiting());
        await(pool.releaseAsync(held));
        assertSame(held, await(pool.acquireAsync(Timeout.of(1), CancelToken.NONE)));
    }

    @Test
    void cancelDuringCreateFreesSlot() {
        AsyncConnectionPool<FakeConn> pool = make(new AsyncFakeFactory(new FakeFactory(), 5.0), null,
            TestSupport.quiet().maxSize(1).acquireTimeout(null).createTimeout(null).build());
        CancelTokenSource cts = new CancelTokenSource();
        CompletableFuture<FakeConn> task = pool.acquireAsync(Timeout.unset(), cts.token());
        TestSupport.spinUntil(() -> pool.stats().size() == 1);
        cts.cancel();
        assertThrows(CancellationException.class, () -> await(task));
        TestSupport.spinUntil(() -> pool.stats().size() == 0);
        assertEquals(0, pool.stats().size());
    }

    @Test
    void closeWakesWaiters() {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).acquireTimeout(null).build());
        FakeConn held = await(pool.acquireAsync());
        CompletableFuture<FakeConn> waiter = pool.acquireAsync();
        TestSupport.spinUntil(() -> pool.stats().waiting() == 1);
        await(pool.closeAsync());
        assertThrows(PoolClosedError.class, () -> await(waiter));
        await(pool.releaseAsync(held));
        assertTrue(held.closed);
    }

    @Test
    void maintenanceTaskAndContextManager() {
        AsyncFakeFactory factory = new AsyncFakeFactory();
        AsyncConnectionPool<FakeConn> pool = new AsyncConnectionPool<>(
            factory, TestSupport.quiet().minSize(2).maintenanceInterval(0.02).build());
        pools.add(pool);
        await(pool.openAsync());
        assertEquals(2, pool.stats().idle());
        await(pool.closeAsync());
        assertTrue(pool.closed());
        assertTrue(factory.inner.created.stream().allMatch(c -> c.closed));
    }

    // GatedPool: close wins the race against open's fill-to-min, so no maintenance thread starts.
    static final class GatedPool extends AsyncConnectionPool<FakeConn> {
        GatedPool(PoolConfig cfg) {
            super(new AsyncFakeFactory(), cfg);
        }

        @Override
        void fillToMin() {
            await(closeAsync()); // closed becomes true before fillToMin returns
            super.fillToMin();
        }
    }

    @Test
    void openRacingCloseDoesNotStartMaintenance() {
        GatedPool pool = new GatedPool(TestSupport.quiet().maintenanceInterval(0.01).build());
        pools.add(pool);
        await(pool.openAsync());
        assertTrue(pool.closed());
    }

    @Test
    void concurrencyStress() throws Exception {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(4).acquireTimeout(10.0).maxUses(9).build());
        Object lockObj = new Object();
        int[] now = {0};
        int[] peak = {0};
        List<CompletableFuture<Void>> workers = new ArrayList<>();
        for (int w = 0; w < 25; w++) {
            CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
            for (int i = 0; i < 40; i++) {
                chain = chain.thenCompose(ignored -> pool.useAsync(conn -> {
                    synchronized (lockObj) {
                        now[0] += 1;
                        peak[0] = Math.max(peak[0], now[0]);
                    }
                    assertFalse(conn.closed);
                    synchronized (lockObj) {
                        now[0] -= 1;
                    }
                    return DONE;
                }, Timeout.unset(), CancelToken.NONE));
            }
            workers.add(chain);
        }
        CompletableFuture.allOf(workers.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
        PoolStats s = pool.stats();
        assertTrue(peak[0] <= 4);
        assertEquals(0, s.inUse());
        assertEquals(0, s.waiting());
        assertEquals(25 * 40, s.acquired());
        assertEquals(s.idle(), s.created() - s.closed());
    }

    @Test
    void duplicateConnectionFromFactoryIsRejected() {
        FakeConn shared = new FakeConn();
        AsyncConnectionPool<FakeConn> pool = make(
            AsyncCallableFactory.<FakeConn>builder(t -> CompletableFuture.completedFuture(shared)).build(),
            null, null);
        FakeConn first = await(pool.acquireAsync());
        assertThrows(ConnectionCreateError.class, () -> await(pool.acquireAsync()));
        assertEquals(1, pool.stats().size());
        assertEquals(1, pool.stats().created());
        await(pool.releaseAsync(first));
    }

    @Test
    void validateIdleEvictsDeadConnections() {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().validateIdle(true).maxSize(2).build());
        FakeConn a = await(pool.acquireAsync());
        FakeConn b = await(pool.acquireAsync());
        await(pool.releaseAsync(a));
        await(pool.releaseAsync(b));
        assertEquals(2, pool.stats().idle());
        a.healthy = false;
        await(pool.runMaintenanceAsync());
        assertEquals(1, pool.stats().idle());
        assertTrue(rec.closedSnapshot().contains(CloseReason.UNHEALTHY));
        FakeConn c = await(pool.acquireAsync());
        assertSame(b, c);
        await(pool.releaseAsync(c));
    }

    @Test
    void defaultConfigWhenOmitted() {
        AsyncConnectionPool<FakeConn> pool = new AsyncConnectionPool<>(new AsyncFakeFactory());
        pools.add(pool);
        FakeConn c = await(pool.acquireAsync());
        await(pool.releaseAsync(c));
        assertEquals(1, pool.stats().size());
    }

    @Test
    void healthCheckClosedMidValidationReportsPoolClosed() {
        AtomicReference<AsyncConnectionPool<FakeConn>> ref = new AtomicReference<>();
        boolean[] closedOnce = {false};
        AsyncConnectionFactory<FakeConn> factory = AsyncCallableFactory.<FakeConn>builder(
            t -> CompletableFuture.completedFuture(new FakeConn()))
            .validate((c, t) -> {
                if (!closedOnce[0]) {
                    closedOnce[0] = true;
                    await(ref.get().closeAsync());
                }
                return CompletableFuture.completedFuture(true);
            }).build();
        AsyncConnectionPool<FakeConn> pool = make(factory, null, TestSupport.quiet().validateIdle(true).build());
        ref.set(pool);
        FakeConn a = await(pool.acquireAsync());
        await(pool.releaseAsync(a));
        await(pool.runMaintenanceAsync());
        assertTrue(rec.closedSnapshot().contains(CloseReason.POOL_CLOSED));
    }

    @Test
    void acquireWithNoTimeoutsUsesUnboundedCreate() {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().acquireTimeout(null).createTimeout(null).build());
        FakeConn c = await(pool.acquireAsync());
        await(pool.releaseAsync(c));
        assertEquals(1, pool.stats().created());
    }

    @Test
    void createTimeoutOnlyUsesCreateLimit() {
        // acquireTimeout null (rem null), createTimeout set -> createLimit takes the createTimeout branch.
        FakeFactory f = new FakeFactory();
        f.failTimes = 1;
        AsyncConnectionPool<FakeConn> pool = make(new AsyncFakeFactory(f, 0.0), new ManualClock(),
            TestSupport.quiet().acquireTimeout(null).createTimeout(5.0)
                .retry(RetryPolicy.builder().maxAttempts(3).baseDelay(1).jitter(0).build()).build());
        FakeConn c = await(pool.acquireAsync());
        assertEquals(2, f.attempts);
        assertTrue(c != null);
        await(pool.releaseAsync(c));
    }

    @Test
    void retrySleepsWhenAcquireTimeoutIsInfinite() {
        FakeFactory f = new FakeFactory();
        f.failTimes = 1;
        AsyncConnectionPool<FakeConn> pool = make(new AsyncFakeFactory(f, 0.0), new ManualClock(),
            TestSupport.quiet().acquireTimeout(null).createTimeout(null)
                .retry(RetryPolicy.builder().maxAttempts(3).baseDelay(1).jitter(0).build()).build());
        FakeConn c = await(pool.acquireAsync());
        assertEquals(2, f.attempts);
        assertTrue(c != null);
        await(pool.releaseAsync(c));
    }

    @Test
    void createFailureWithEmptyMessageIsDescribedByType() {
        AsyncConnectionFactory<FakeConn> factory = AsyncCallableFactory.<FakeConn>builder(
            t -> CompletableFuture.failedFuture(new ConnErr(""))).build();
        AsyncConnectionPool<FakeConn> pool = make(factory, null, TestSupport.quiet().retry(RetryPolicy.NO_RETRY).build());
        ConnectionCreateError ex = assertThrows(ConnectionCreateError.class, () -> await(pool.acquireAsync()));
        assertTrue(ex.getMessage().contains("ConnErr"));
    }

    @Test
    void leakDetectionWarnsOnce() {
        ManualClock clock = new ManualClock();
        AsyncConnectionPool<FakeConn> pool = make(null, clock, TestSupport.quiet().leakDetectionTimeout(10.0).build());
        FakeConn conn = await(pool.acquireAsync());
        await(pool.runMaintenanceAsync()); // not held long enough yet (holdTime < timeout)
        assertEquals(0, pool.stats().leaks());
        clock.advance(11);
        await(pool.runMaintenanceAsync());
        await(pool.runMaintenanceAsync());
        assertEquals(1, pool.stats().leaks());
        await(pool.releaseAsync(conn));
    }

    @Test
    void gracefulDrainWaitsForInUse() {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(2).build());
        FakeConn held = await(pool.acquireAsync());
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            await(pool.releaseAsync(held));
        });
        await(pool.closeAsync(2.0));
        assertTrue(held.closed);
        assertEquals(0, pool.stats().inUse());
    }

    @Test
    void drainTimeoutGivesUp() {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).build());
        FakeConn held = await(pool.acquireAsync());
        await(pool.closeAsync(0.05));
        assertEquals(1, pool.stats().inUse());
        assertFalse(held.closed);
        await(pool.releaseAsync(held));
        assertTrue(held.closed);
    }

    @Test
    void drainZeroTimeoutReturnsImmediately() {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).build());
        FakeConn held = await(pool.acquireAsync());
        await(pool.closeAsync(0.0));
        assertEquals(1, pool.stats().inUse());
        await(pool.releaseAsync(held));
    }

    @Test
    void openEdges() {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().maintenanceInterval(0.5).build());
        assertSame(await(pool.openAsync()), await(pool.openAsync()));
        await(pool.closeAsync());
        assertThrows(PoolClosedError.class, () -> await(pool.openAsync()));
    }

    @Test
    void runMaintenanceOnClosedPoolIsNoop() {
        AsyncConnectionPool<FakeConn> pool = make();
        await(pool.closeAsync());
        await(pool.runMaintenanceAsync());
    }

    @Test
    void maintenanceEvictsIdle() {
        ManualClock clock = new ManualClock();
        AsyncConnectionPool<FakeConn> pool = make(null, clock, TestSupport.quiet().maxIdleTime(10.0).build());
        FakeConn a = await(pool.acquireAsync());
        await(pool.releaseAsync(a));
        clock.advance(11);
        await(pool.runMaintenanceAsync());
        assertEquals(0, pool.stats().idle());
        assertEquals(List.of(CloseReason.IDLE), rec.closedSnapshot());
    }

    @Test
    void maxWaitersFailsFast() {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().maxSize(1).maxWaiters(0).acquireTimeout(5.0).build());
        FakeConn held = await(pool.acquireAsync());
        assertThrows(PoolExhaustedError.class, () -> await(pool.acquireAsync()));
        await(pool.releaseAsync(held));
    }

    @Test
    void releaseDiscardAndCloseErrorDoNotLeak() {
        AsyncConnectionPool<FakeConn> pool = make(AsyncCallableFactory.<FakeConn>builder(
            t -> CompletableFuture.completedFuture(new FakeConn()))
            .close((c, t) -> CompletableFuture.failedFuture(new RuntimeException("close boom"))).build(), null, null);
        await(pool.releaseAsync(await(pool.acquireAsync()), true, null, CancelToken.NONE));
        assertEquals(0, pool.stats().size());
        assertEquals(List.of(CloseReason.DISCARDED), rec.closedSnapshot());
    }

    @Test
    void releaseResetErrorIsResetFailed() {
        AsyncConnectionPool<FakeConn> pool = make(AsyncCallableFactory.<FakeConn>builder(
            t -> CompletableFuture.completedFuture(new FakeConn()))
            .reset((c, t) -> CompletableFuture.failedFuture(new IllegalStateException("reset boom"))).build(), null, null);
        await(pool.releaseAsync(await(pool.acquireAsync())));
        assertEquals(List.of(CloseReason.RESET_FAILED), rec.closedSnapshot());
    }

    @Test
    void releaseResetCancellationDestroys() {
        AsyncConnectionPool<FakeConn> pool = make(AsyncCallableFactory.<FakeConn>builder(
            t -> CompletableFuture.completedFuture(new FakeConn()))
            .reset((c, t) -> CompletableFuture.failedFuture(new CancellationException())).build(), null, null);
        FakeConn a = await(pool.acquireAsync());
        assertThrows(CancellationException.class, () -> await(pool.releaseAsync(a)));
        assertEquals(List.of(CloseReason.ERROR), rec.closedSnapshot());
    }

    @Test
    void checkoutValidationErrorIsUnhealthy() {
        AsyncConnectionPool<FakeConn> pool = make(AsyncCallableFactory.<FakeConn>builder(
            t -> CompletableFuture.completedFuture(new FakeConn()))
            .validate((c, t) -> CompletableFuture.failedFuture(new IllegalStateException("ping boom"))).build(),
            null, TestSupport.quiet().validateAfterIdle(0.0).build());
        FakeConn a = await(pool.acquireAsync());
        await(pool.releaseAsync(a));
        FakeConn b = await(pool.acquireAsync());
        assertNotSame(a, b);
        assertEquals(List.of(CloseReason.UNHEALTHY), rec.closedSnapshot());
        await(pool.releaseAsync(b));
    }

    @Test
    void borrowValidationCancellationIsNotSwallowed() {
        AsyncConnectionPool<FakeConn> pool = make(AsyncCallableFactory.<FakeConn>builder(
            t -> CompletableFuture.completedFuture(new FakeConn()))
            .validate((c, t) -> CompletableFuture.failedFuture(new CancellationException())).build(),
            null, TestSupport.quiet().validateAfterIdle(0.0).build());
        FakeConn a = await(pool.acquireAsync());
        await(pool.releaseAsync(a));
        assertThrows(CancellationException.class, () -> await(pool.acquireAsync()));
        assertEquals(List.of(CloseReason.ERROR), rec.closedSnapshot());
    }

    @Test
    void createTimeoutDuringRetryDoesNotTripBreaker() {
        FakeFactory f = new FakeFactory();
        f.failAlways = true;
        ManualClock clock = new ManualClock();
        AsyncConnectionPool<FakeConn> pool = make(new AsyncFakeFactory(f, 0.0), clock, TestSupport.quiet()
            .acquireTimeout(1.0)
            .retry(RetryPolicy.builder().maxAttempts(3).baseDelay(10).jitter(0).build())
            .breaker(BreakerConfig.builder().failureThreshold(1).build()).build());
        assertThrows(PoolTimeoutError.class, () -> await(pool.acquireAsync()));
        assertEquals("closed", pool.stats().breakerState());
    }

    @Test
    void createTimeoutCountsAsBreakerFailure() {
        AsyncConnectionPool<FakeConn> pool = make(new AsyncFakeFactory(new FakeFactory(), 5.0), null, TestSupport.quiet()
            .createTimeout(0.05).acquireTimeout(5.0)
            .breaker(BreakerConfig.builder().failureThreshold(1).build()).build());
        assertThrows(ConnectionCreateError.class, () -> await(pool.acquireAsync()));
        assertEquals("open", pool.stats().breakerState());
    }

    @Test
    void acquireDeadlineTimeoutDoesNotTripBreaker() {
        AsyncConnectionPool<FakeConn> pool = make(new AsyncFakeFactory(new FakeFactory(), 5.0), null, TestSupport.quiet()
            .createTimeout(null).acquireTimeout(0.05)
            .breaker(BreakerConfig.builder().failureThreshold(1).build()).build());
        assertThrows(PoolTimeoutError.class, () -> await(pool.acquireAsync()));
        assertEquals("closed", pool.stats().breakerState());
    }

    @Test
    void poolClosedMidRetry() {
        AtomicReference<AsyncConnectionPool<FakeConn>> ref = new AtomicReference<>();
        AsyncConnectionFactory<FakeConn> factory = AsyncCallableFactory.<FakeConn>builder(t -> {
            await(ref.get().closeAsync());
            return CompletableFuture.failedFuture(new ConnErr("down"));
        }).build();
        AsyncConnectionPool<FakeConn> pool = make(factory, null, TestSupport.quiet()
            .retry(RetryPolicy.builder().maxAttempts(3).baseDelay(0).jitter(0).build()).build());
        ref.set(pool);
        assertThrows(PoolClosedError.class, () -> await(pool.acquireAsync()));
    }

    @Test
    void connectionCreatedDuringCloseIsDestroyed() {
        AtomicReference<AsyncConnectionPool<FakeConn>> ref = new AtomicReference<>();
        List<FakeConn> made = new ArrayList<>();
        AsyncConnectionFactory<FakeConn> factory = AsyncCallableFactory.<FakeConn>builder(t -> {
            FakeConn c = new FakeConn();
            made.add(c);
            await(ref.get().closeAsync());
            return CompletableFuture.completedFuture(c);
        }).closeSync(c -> c.closed = true).build();
        AsyncConnectionPool<FakeConn> pool = make(factory, null, null);
        ref.set(pool);
        assertThrows(PoolClosedError.class, () -> await(pool.acquireAsync()));
        assertTrue(made.size() > 0 && made.get(0).closed);
    }

    @Test
    void warmupFailureIsNotFatal() {
        FakeFactory f = new FakeFactory();
        f.failAlways = true;
        AsyncConnectionPool<FakeConn> pool = make(new AsyncFakeFactory(f, 0.0), null, TestSupport.quiet().minSize(2).build());
        await(pool.openAsync());
        assertEquals(0, pool.stats().size());
    }

    @Test
    void warmupConnectionDestroyedIfClosedDuringCreate() {
        AtomicReference<AsyncConnectionPool<FakeConn>> ref = new AtomicReference<>();
        List<FakeConn> made = new ArrayList<>();
        AsyncConnectionFactory<FakeConn> factory = AsyncCallableFactory.<FakeConn>builder(t -> {
            FakeConn c = new FakeConn();
            made.add(c);
            await(ref.get().closeAsync());
            return CompletableFuture.completedFuture(c);
        }).closeSync(c -> c.closed = true).build();
        AsyncConnectionPool<FakeConn> pool = new AsyncConnectionPool<>(factory, TestSupport.quiet().minSize(1).build());
        pools.add(pool);
        ref.set(pool);
        await(pool.openAsync());
        assertTrue(made.size() > 0 && made.get(0).closed);
    }

    static final class BoomMaintenancePool extends AsyncConnectionPool<FakeConn> {
        final AtomicInteger calls = new AtomicInteger();

        BoomMaintenancePool(PoolConfig cfg) {
            super(new AsyncFakeFactory(), cfg);
        }

        @Override
        public void runMaintenance() {
            calls.incrementAndGet();
            throw new IllegalStateException("sweep broke");
        }
    }

    @Test
    void maintenanceLoopRunsAndSurvivesErrors() {
        BoomMaintenancePool pool = new BoomMaintenancePool(TestSupport.quiet().maintenanceInterval(0.01).build());
        pools.add(pool);
        await(pool.openAsync());
        TestSupport.spinUntil(() -> pool.calls.get() > 0);
        assertTrue(pool.calls.get() >= 1);
        await(pool.closeAsync());
    }

    static final class CountingMaintenancePool extends AsyncConnectionPool<FakeConn> {
        final AtomicInteger calls = new AtomicInteger();

        CountingMaintenancePool(PoolConfig cfg) {
            super(new AsyncFakeFactory(), cfg);
        }

        @Override
        public void runMaintenance() {
            super.runMaintenance();
            calls.incrementAndGet();
        }
    }

    @Test
    void maintenanceLoopRunsSuccessfully() {
        CountingMaintenancePool pool = new CountingMaintenancePool(TestSupport.quiet().maintenanceInterval(0.01).build());
        pools.add(pool);
        await(pool.openAsync());
        TestSupport.spinUntil(() -> pool.calls.get() > 0);
        assertTrue(pool.calls.get() > 0);
        await(pool.closeAsync());
    }

    @Test
    void maintenanceLoopInterruptStops() throws Exception {
        AsyncConnectionPool<FakeConn> pool = make(TestSupport.quiet().maintenanceInterval(5.0).build());
        Thread t = new Thread(pool::maintenanceLoop);
        t.setDaemon(true);
        t.start();
        TestSupport.spinUntil(t::isAlive);
        t.interrupt();
        t.join(2000);
        assertFalse(t.isAlive());
    }

    @Test
    void useResultPropagatesError() {
        AsyncConnectionPool<FakeConn> pool = make();
        // body fails inside a stage -> the exception reaches useAsync wrapped in CompletionException
        assertThrows(IllegalArgumentException.class, () -> await(pool.useAsync(
            c -> CompletableFuture.completedFuture((Void) null).thenRun(() -> {
                throw new IllegalArgumentException();
            }), Timeout.unset(), CancelToken.NONE)));
        assertEquals(1, pool.stats().idle());
    }

    @Test
    void expiredConnectionDroppedWithLiveToken() {
        ManualClock clock = new ManualClock();
        AsyncConnectionPool<FakeConn> pool = make(null, clock, TestSupport.quiet().maxLifetime(10.0).build());
        CancelTokenSource cts = new CancelTokenSource();
        FakeConn a = await(pool.acquireAsync(Timeout.unset(), cts.token()));
        await(pool.releaseAsync(a, false, null, cts.token()));
        clock.advance(11);
        FakeConn b = await(pool.acquireAsync(Timeout.unset(), cts.token()));
        assertNotSame(a, b);
        await(pool.releaseAsync(b));
    }

    @Test
    void reprMentionsSizes() {
        assertTrue(make().toString().contains("AsyncConnectionPool"));
    }

    @Test
    void leakWarningWhenNotClosed() {
        AsyncConnectionPool<FakeConn> pool = new AsyncConnectionPool<>(new AsyncFakeFactory(), TestSupport.quietConfig());
        pools.add(pool);
        assertFalse(pool.warnIfLeaked()); // fresh: not closed, size 0
        await(pool.acquireAsync());
        assertTrue(pool.warnIfLeaked());
        await(pool.closeAsync());
        assertFalse(pool.warnIfLeaked());
    }

    @Test
    void idleSnapshotExposesRaw() {
        AsyncConnectionPool<FakeConn> pool = make();
        FakeConn a = await(pool.acquireAsync());
        await(pool.releaseAsync(a));
        assertEquals(List.of(a), pool.idleSnapshot());
    }

    @Test
    void explicitRngIsUsed() {
        AsyncConnectionPool<FakeConn> pool = new AsyncConnectionPool<>(
            new AsyncFakeFactory(), TestSupport.quietConfig(), null, null, new java.util.Random(0), "p");
        pools.add(pool);
        FakeConn c = await(pool.acquireAsync());
        await(pool.releaseAsync(c));
        assertEquals(1, pool.stats().size());
    }

    @Test
    void waiterQueueHasRoom() {
        AsyncConnectionPool<FakeConn> pool = make(
            TestSupport.quiet().maxSize(1).maxWaiters(2).acquireTimeout(null).build());
        FakeConn held = await(pool.acquireAsync());
        CompletableFuture<FakeConn> waiter = pool.acquireAsync();
        TestSupport.spinUntil(() -> pool.stats().waiting() == 1);
        await(pool.releaseAsync(held));
        assertSame(held, await(waiter));
    }

    @Test
    void useAsyncBodyThrowingSynchronouslyReleases() {
        AsyncConnectionPool<FakeConn> pool = make();
        assertThrows(IllegalStateException.class, () -> await(pool.useAsync(
            c -> {
                throw new IllegalStateException("sync throw");
            }, Timeout.unset(), CancelToken.NONE)));
        assertEquals(1, pool.stats().idle());
        assertEquals(0, pool.stats().inUse());
    }

    @Test
    void benignErrorKeepsConnection() {
        AsyncConnectionPool<FakeConn> pool = make();
        assertThrows(IllegalArgumentException.class, () -> await(pool.useAsync(
            c -> CompletableFuture.failedFuture(new IllegalArgumentException("app bug")),
            Timeout.unset(), CancelToken.NONE)));
        assertEquals(1, pool.stats().idle());
    }

    @Test
    void benignErrorWithFailedValidationDiscards() {
        AsyncConnectionPool<FakeConn> pool = make();
        assertThrows(IllegalArgumentException.class, () -> await(pool.useAsync(c -> {
            c.healthy = false;
            return CompletableFuture.failedFuture(new IllegalArgumentException());
        }, Timeout.unset(), CancelToken.NONE)));
        assertEquals(0, pool.stats().idle());
    }

    @Test
    void nonRetryableErrorFailsImmediately() {
        FakeFactory f = new FakeFactory();
        f.failAlways = true;
        AsyncConnectionPool<FakeConn> pool = make(new AsyncFakeFactory(f, 0.0), new ManualClock(),
            TestSupport.quiet().retry(
                RetryPolicy.builder().maxAttempts(5).retryOn(e -> e instanceof ConnReset).build()).build());
        assertThrows(ConnectionCreateError.class, () -> await(pool.acquireAsync()));
        assertEquals(1, f.attempts);
    }

    @Test
    void duplicateIdleConnectionRejectedViaRace() throws Exception {
        FakeConn shared = new FakeConn();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger creates = new AtomicInteger();
        AsyncConnectionFactory<FakeConn> f = new AsyncConnectionFactory<>() {
            @Override
            public CompletableFuture<FakeConn> createAsync(CancelToken t) {
                if (creates.incrementAndGet() == 1) {
                    return CompletableFuture.completedFuture(shared);
                }
                entered.countDown();
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        gate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return shared;
                });
            }

            @Override
            public CompletableFuture<Void> closeAsync(FakeConn c, CancelToken t) {
                c.closed = true;
                return DONE;
            }
        };
        AsyncConnectionPool<FakeConn> pool = make(f, null, TestSupport.quiet().maxSize(2).build());
        FakeConn a = await(pool.acquireAsync());
        CompletableFuture<FakeConn> task = pool.acquireAsync();
        entered.await();
        await(pool.releaseAsync(a));
        gate.countDown();
        assertThrows(ConnectionCreateError.class, () -> await(task));
    }
}
