// The pure "functional core": every rule about *when a connection must go away*.
//
// No locks, no I/O, no clocks of its own - just functions of (connection, config, now).
// Both the sync and async pools call these, so the rules live in exactly one place.
using System;
using System.Collections.Generic;

namespace ConnPool
{
    internal static class Lifecycle
    {
        public static PooledConnection<T> NewConnection<T>(T raw, PoolConfig cfg, double now, Random rng)
            where T : class
        {
            double? expiresAt = null;
            if (cfg.MaxLifetime is not null)
                expiresAt = now + cfg.MaxLifetime.Value * (1.0 - cfg.LifetimeJitter * rng.NextDouble());
            return new(raw, createdAt: now, expiresAt: expiresAt);
        }

        public static PooledConnection<T>? PopIdle<T>(LinkedList<PooledConnection<T>> idle, IdleStrategy strategy)
            where T : class
        {
            if (idle.Count == 0)
                return null;
            var node = strategy == IdleStrategy.Lifo ? idle.Last! : idle.First!;
            idle.Remove(node);
            return node.Value;
        }

        /// <summary>Connections that must be retired no matter what (lifetime / use-count rotation).</summary>
        public static CloseReason? RotationReason(PooledConnection c, PoolConfig cfg, double now)
        {
            if (c.ExpiresAt is not null && now >= c.ExpiresAt.Value)
                return CloseReason.Expired;
            if (cfg.MaxUses is not null && c.UseCount >= cfg.MaxUses.Value)
                return CloseReason.MaxUses;
            return null;
        }

        public static bool IdleTooLong(PooledConnection c, PoolConfig cfg, double now) =>
            cfg.MaxIdleTime is not null && c.IdleTime(now) >= cfg.MaxIdleTime.Value;

        public static bool NeedsValidation(PooledConnection c, PoolConfig cfg, double now) =>
            cfg.ValidateAfterIdle is not null && c.IdleTime(now) >= cfg.ValidateAfterIdle.Value;

        /// <summary>Static checks applied to an idle connection about to be handed out
        /// (validation, which needs I/O, is done by the pool afterwards).</summary>
        public static CloseReason? BorrowReason(PooledConnection c, PoolConfig cfg, double now)
        {
            var reason = RotationReason(c, cfg, now);
            if (reason is null && IdleTooLong(c, cfg, now))
                reason = CloseReason.Idle;
            return reason;
        }

        /// <summary>Split idle connections into (keep, doomed). Rotation victims always go;
        /// idle-timeout victims go only while the pool stays at or above MinSize.</summary>
        public static (List<PooledConnection<T>> Keep, List<(PooledConnection<T>, CloseReason)> Doomed) Sweep<T>(
            IEnumerable<PooledConnection<T>> idle, int totalSize, PoolConfig cfg, double now)
            where T : class
        {
            var keep = new List<PooledConnection<T>>();
            var doomed = new List<(PooledConnection<T>, CloseReason)>();
            var remaining = totalSize;
            foreach (var c in idle)
            {
                var reason = RotationReason(c, cfg, now);
                if (reason is null && IdleTooLong(c, cfg, now) && remaining > cfg.MinSize)
                    reason = CloseReason.Idle;
                if (reason is null)
                {
                    keep.Add(c);
                }
                else
                {
                    doomed.Add((c, reason.Value));
                    remaining -= 1;
                }
            }
            return (keep, doomed);
        }
    }
}
