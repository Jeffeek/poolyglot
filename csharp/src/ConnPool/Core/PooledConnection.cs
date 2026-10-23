// Bookkeeping wrapper around a raw connection.
using System.Threading;

namespace ConnPool
{
    /// <summary>Why a connection left the pool.</summary>
    public enum CloseReason
    {
        Expired,      // max_lifetime reached (rotation)
        MaxUses,      // max_uses reached (rotation)
        Idle,         // sat idle longer than max_idle_time
        Unhealthy,    // failed validation
        Error,        // caller's block raised and the connection is suspect
        ResetFailed,  // factory.Reset() raised on release
        Discarded,    // caller asked for it
        PoolClosed,
    }

    public static class CloseReasonExtensions
    {
        /// <summary>Stable wire value, kept identical across every poolyglot language.</summary>
        public static string Value(this CloseReason reason) => reason switch
        {
            CloseReason.Expired => "expired",
            CloseReason.MaxUses => "max_uses",
            CloseReason.Idle => "idle",
            CloseReason.Unhealthy => "unhealthy",
            CloseReason.Error => "error",
            CloseReason.ResetFailed => "reset_failed",
            CloseReason.Discarded => "discarded",
            _ => "pool_closed",
        };
    }

    /// <summary>Metadata the pool needs to manage a connection, independent of its type.</summary>
    public abstract class PooledConnection
    {
        private static long _ids;

        // Bookkeeping is readable by anyone (listeners, diagnostics) but only the pool mutates it.
        public long Id { get; }
        public double CreatedAt { get; }
        public double? ExpiresAt { get; internal set; }
        public double? LastReturnedAt { get; internal set; }
        public double? CheckedOutAt { get; internal set; }
        public int UseCount { get; internal set; }
        public bool LeakReported { get; internal set; }

        /// <summary>The raw connection as <see cref="object"/> (for type-agnostic listeners).</summary>
        public abstract object RawObject { get; }

        protected PooledConnection(double createdAt, double? expiresAt, double? lastReturnedAt)
        {
            CreatedAt = createdAt;
            ExpiresAt = expiresAt;
            LastReturnedAt = lastReturnedAt ?? createdAt;
            Id = Interlocked.Increment(ref _ids);
        }

        public double Age(double now) => now - CreatedAt;

        public double IdleTime(double now) => now - (LastReturnedAt ?? CreatedAt);

        public double HoldTime(double now) => CheckedOutAt is null ? 0.0 : now - CheckedOutAt.Value;

        public void MarkCheckedOut(double now)
        {
            CheckedOutAt = now;
            UseCount += 1;
            LeakReported = false;
        }

        public void MarkReturned(double now)
        {
            CheckedOutAt = null;
            LastReturnedAt = now;
        }

        public override string ToString() => $"<PooledConnection #{Id} uses={UseCount}>";
    }

    /// <summary>A raw connection plus the metadata the pool needs to manage it.</summary>
    public sealed class PooledConnection<T> : PooledConnection where T : class
    {
        public T Raw { get; }

        public override object RawObject => Raw;

        public PooledConnection(T raw, double createdAt, double? expiresAt = null, double? lastReturnedAt = null)
            : base(createdAt, expiresAt, lastReturnedAt) => Raw = raw;
    }
}
