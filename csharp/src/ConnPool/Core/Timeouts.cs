// Timeout handling shared by the pools.
//
// A Timeout is one of three things, mirroring the Python `float | None | UNSET`:
//   * Unset     -> "use the configured default" (the default value of the struct)
//   * Infinite  -> wait forever (the Python `None`)
//   * Of(secs)  -> wait this many seconds (0 = don't wait)
namespace ConnPool
{
    public readonly struct Timeout
    {
        private enum Kind : byte { Unset = 0, Infinite = 1, Value = 2 }

        private readonly Kind _kind;
        private readonly double _seconds;

        private Timeout(Kind kind, double seconds)
        {
            _kind = kind;
            _seconds = seconds;
        }

        /// <summary>"Use the configured default." This is also <c>default(Timeout)</c>.</summary>
        public static Timeout Unset => default;

        /// <summary>Wait forever (no deadline).</summary>
        public static Timeout Infinite => new Timeout(Kind.Infinite, 0.0);

        /// <summary>Wait at most <paramref name="seconds"/> (0 = don't wait).</summary>
        public static Timeout Of(double seconds) => new Timeout(Kind.Value, seconds);

        public bool IsUnset => _kind == Kind.Unset;

        public static implicit operator Timeout(double seconds) => Of(seconds);

        public static implicit operator Timeout(double? seconds) =>
            seconds is null ? Infinite : Of(seconds.Value);

        /// <summary>Resolve to seconds-to-wait, or null for "no deadline".</summary>
        internal double? Resolve(double? configuredDefault) => _kind switch
        {
            Kind.Unset => configuredDefault,
            Kind.Infinite => null,
            _ => _seconds,
        };
    }

    internal static class TimeoutMath
    {
        public static double? ComputeDeadline(Timeout timeout, double? configuredDefault, double now)
        {
            double? resolved = timeout.Resolve(configuredDefault);
            return resolved is null ? (double?)null : now + resolved.Value;
        }

        public static double? Remaining(double? deadline, double now) =>
            deadline is null ? (double?)null : deadline.Value - now;
    }
}
