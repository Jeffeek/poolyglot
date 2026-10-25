package io.github.jeffeek.connpool;

import java.util.List;

/** Percentile math and the sample-window size shared by the stats collector. */
final class Statistics {
    Statistics() {
    }

    /** How many recent acquire-wait / create-latency samples to keep (bounded memory). */
    static final int ACQUIRE_WAIT_WINDOW = 1024;

    // The quantiles reported in every PoolStats snapshot.
    static final double P50 = 0.50;
    static final double P95 = 0.95;
    static final double P99 = 0.99;

    /** Linear-interpolated percentile of an already-sorted list. {@code quantile} in [0, 1]. */
    static double percentile(List<Double> sortedValues, double quantile) {
        if (sortedValues.isEmpty()) {
            return 0.0;
        }
        double rank = (sortedValues.size() - 1) * quantile;
        int lower = (int) rank;
        int upper = Math.min(lower + 1, sortedValues.size() - 1);
        return sortedValues.get(lower) + (sortedValues.get(upper) - sortedValues.get(lower)) * (rank - lower);
    }
}
