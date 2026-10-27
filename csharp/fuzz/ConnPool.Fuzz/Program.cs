// SharpFuzz entry point for the divergent async primitives: AsyncCondition (Pools/AsyncPrimitives.cs)
// and AwaitShielded, exercised through the public AsyncConnectionPool<T> surface
// (AcquireAsync/ReleaseAsync with CancellationToken). Fuzz bytes decode into a bounded op
// sequence that includes cancelling the CTS mid-AcquireAsync, so the waiter LinkedList and the
// cancellation-vs-notify race in AsyncCondition.WaitAsync get exercised with adversarial timing.
//
// Build/run (libFuzzer host only - see README.md): this project is intentionally outside
// ConnPool.sln, so it never affects `dotnet test ConnPool.sln` / run_tests.sh's 100% gate.
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ConnPool;
using SharpFuzz;

Fuzzer.LibFuzzer.Run(bytes => Harness.RunOnce(bytes));

internal static class Harness
{
    // One fuzz input -> one bounded sequence of pool operations against a tiny pool (MaxSize=2),
    // so waiters queue up and contend for real. Every op is awaited synchronously via
    // GetAwaiter().GetResult() the way libFuzzer drives a synchronous Fuzzer.LibFuzzer.Run body.
    public static void RunOnce(ReadOnlySpan<byte> bytes)
    {
        RunAsync(bytes.ToArray()).GetAwaiter().GetResult();
    }

    private static async Task RunAsync(byte[] data)
    {
        var cfg = new PoolConfig
        {
            MaxSize = 2,
            MinSize = 0,
            MaintenanceInterval = null,
            AcquireTimeout = 0.2,
            Retry = RetryPolicy.NoRetry,
            Breaker = null,
        };
        await using var pool = AsyncConnectionPool<object>.FromCallables(
            create: _ => Task.FromResult(new object()),
            close: (_, _) => Task.CompletedTask,
            config: cfg,
            name: "fuzz");
        await pool.OpenAsync().ConfigureAwait(false);

        var held = new List<object>();
        int pos = 0;
        byte Next() => pos < data.Length ? data[pos++] : (byte)0;

        // Bound the op count so one input can't run forever under the libFuzzer watchdog.
        int ops = Math.Min(32, data.Length);
        for (int i = 0; i < ops; i++)
        {
            byte op = Next();
            switch (op % 4)
            {
                case 0: // plain acquire
                {
                    try
                    {
                        var conn = await pool.AcquireAsync().ConfigureAwait(false);
                        held.Add(conn);
                    }
                    catch (PoolTimeoutError) { /* exhausted - expected under contention */ }
                    break;
                }
                case 1: // release a held connection, if any
                {
                    if (held.Count > 0)
                    {
                        var conn = held[^1];
                        held.RemoveAt(held.Count - 1);
                        await pool.ReleaseAsync(conn).ConfigureAwait(false);
                    }
                    break;
                }
                case 2: // acquire, then cancel the token mid-flight (byte picks the delay)
                {
                    using var cts = new CancellationTokenSource();
                    int delayMs = Next() % 5;
                    cts.CancelAfter(TimeSpan.FromMilliseconds(delayMs));
                    try
                    {
                        var conn = await pool.AcquireAsync(cancellationToken: cts.Token).ConfigureAwait(false);
                        held.Add(conn);
                    }
                    catch (OperationCanceledException) { /* expected: caller cancelled */ }
                    catch (PoolTimeoutError) { /* expected: deadline reached */ }
                    break;
                }
                default: // release under an already-cancelled token (AwaitShielded path)
                {
                    if (held.Count > 0)
                    {
                        var conn = held[^1];
                        held.RemoveAt(held.Count - 1);
                        using var cts = new CancellationTokenSource();
                        cts.Cancel();
                        try
                        {
                            await pool.ReleaseAsync(conn, cancellationToken: cts.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) { /* expected: shielded disposal still finished */ }
                    }
                    break;
                }
            }
        }

        // Drain whatever is still held, then assert the pool's invariant: every connection the
        // pool thinks it owns is accounted for (idle + in-use == size), i.e. no reserved slot
        // leaked from a cancelled AcquireAsync.
        foreach (var conn in held)
        {
            try { await pool.ReleaseAsync(conn).ConfigureAwait(false); }
            catch (InvalidReleaseError) { /* already released by a cancelled-but-completed path */ }
        }

        var stats = pool.Stats();
        if (stats.Idle + stats.InUse != stats.Size)
            throw new InvalidOperationException(
                $"invariant violated: idle({stats.Idle}) + in_use({stats.InUse}) != size({stats.Size})");
    }
}
