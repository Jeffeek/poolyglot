package io.github.jeffeek.connpool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Built-in metrics listener. One lock guards everything so a snapshot of counters + windows +
 * by-reason is consistent.
 */
final class StatsCollector extends PoolListener {
    private final Object lock = new Object();

    private int created;
    private int createFailures;
    private int acquired;
    private int released;
    private int timeouts;
    private int closed;
    private int leaks;
    private final Map<CloseReason, Integer> closedByReason = new EnumMap<>(CloseReason.class);
    private final Deque<Double> acquireWaits = new ArrayDeque<>();
    private final Deque<Double> createLatencies = new ArrayDeque<>();

    // exposed for the bounded-window test
    int waitsCount() {
        synchronized (lock) {
            return acquireWaits.size();
        }
    }

    private static void push(Deque<Double> window, double value) {
        window.addLast(value);
        if (window.size() > Statistics.ACQUIRE_WAIT_WINDOW) {
            window.removeFirst();
        }
    }

    @Override
    public void onConnectionCreated(PooledConnection<?> conn, double duration) {
        synchronized (lock) {
            created += 1;
            push(createLatencies, duration);
        }
    }

    @Override
    public void onCreateFailed(Throwable error) {
        synchronized (lock) {
            createFailures += 1;
        }
    }

    @Override
    public void onConnectionClosed(PooledConnection<?> conn, CloseReason reason) {
        synchronized (lock) {
            closed += 1;
            closedByReason.merge(reason, 1, Integer::sum);
        }
    }

    @Override
    public void onAcquired(PooledConnection<?> conn, double waitTime) {
        synchronized (lock) {
            acquired += 1;
            push(acquireWaits, waitTime);
        }
    }

    @Override
    public void onReleased(PooledConnection<?> conn, double holdTime) {
        synchronized (lock) {
            released += 1;
        }
    }

    @Override
    public void onAcquireTimeout(double waited) {
        synchronized (lock) {
            timeouts += 1;
        }
    }

    @Override
    public void onConnectionLeak(PooledConnection<?> conn, double heldFor) {
        synchronized (lock) {
            leaks += 1;
        }
    }

    PoolStats snapshot(String name, int maxSize, int size, int idle, int inUse, int waiting, String breakerState) {
        synchronized (lock) {
            List<Double> waits = new ArrayList<>(acquireWaits);
            Collections.sort(waits);
            List<Double> creates = new ArrayList<>(createLatencies);
            Collections.sort(creates);
            Map<String, Integer> byReason = new LinkedHashMap<>();
            for (Map.Entry<CloseReason, Integer> e : closedByReason.entrySet()) {
                byReason.put(e.getKey().value(), e.getValue());
            }
            byReason = Collections.unmodifiableMap(byReason); // the snapshot is immutable
            return new PoolStats(
                name, maxSize, size, idle, inUse, waiting,
                created, createFailures, acquired, released, timeouts, closed, leaks, byReason,
                avg(waits), Statistics.percentile(waits, Statistics.P50),
                Statistics.percentile(waits, Statistics.P95), Statistics.percentile(waits, Statistics.P99),
                avg(creates), Statistics.percentile(creates, Statistics.P50),
                Statistics.percentile(creates, Statistics.P95), Statistics.percentile(creates, Statistics.P99),
                breakerState);
        }
    }

    private static double avg(List<Double> values) {
        if (values.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (double v : values) {
            sum += v;
        }
        return sum / values.size();
    }
}
