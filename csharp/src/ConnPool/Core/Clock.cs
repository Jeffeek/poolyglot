// Time source abstraction so every time-dependent rule is deterministic in tests.
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace ConnPool
{
    public interface IClock
    {
        double Monotonic();
        void Sleep(double seconds);
        Task AsyncSleepAsync(double seconds, CancellationToken cancellationToken = default);
    }

    /// <summary>Real time. The default.</summary>
    public sealed class SystemClock : IClock
    {
        public double Monotonic() => Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency;

        public void Sleep(double seconds)
        {
            if (seconds > 0)
                Thread.Sleep(TimeSpan.FromSeconds(seconds));
        }

        public Task AsyncSleepAsync(double seconds, CancellationToken cancellationToken = default) =>
            Task.Delay(seconds > 0 ? TimeSpan.FromSeconds(seconds) : TimeSpan.Zero, cancellationToken);
    }

    /// <summary>Fake clock for tests: time moves only when you say so; sleeping just advances it.</summary>
    public sealed class ManualClock : IClock
    {
        private double _now;

        public ManualClock(double start = 1000.0) => _now = start;

        public double Monotonic() => _now;

        public void Advance(double seconds) => _now += seconds;

        public void Sleep(double seconds) => _now += seconds;

        public async Task AsyncSleepAsync(double seconds, CancellationToken cancellationToken = default)
        {
            _now += seconds;
            await Task.Yield();
        }
    }
}
