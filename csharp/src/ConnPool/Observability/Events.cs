// Observability hooks. Subclass PoolListener and override what you care about
// (metrics, tracing, logging). Hooks run synchronously on the calling thread/task, outside
// the pool's lock, so keep them fast. Exceptions raised by listeners are logged and
// swallowed - a broken metrics exporter must never break the pool.
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace ConnPool
{
    public class PoolListener
    {
        public virtual void OnConnectionCreated(PooledConnection? conn, double duration)
        {
        }

        public virtual void OnConnectionClosed(PooledConnection? conn, CloseReason reason)
        {
        }

        public virtual void OnCreateFailed(Exception error)
        {
        }

        public virtual void OnAcquired(PooledConnection? conn, double waitTime)
        {
        }

        public virtual void OnReleased(PooledConnection? conn, double holdTime)
        {
        }

        public virtual void OnAcquireTimeout(double waited)
        {
        }

        public virtual void OnConnectionLeak(PooledConnection? conn, double heldFor)
        {
        }

        public virtual void OnBreakerStateChange(BreakerState old, BreakerState @new)
        {
        }
    }

    internal sealed class ListenerGroup
    {
        private readonly List<PoolListener> _listeners;

        public ListenerGroup(IEnumerable<PoolListener> listeners) => _listeners = new(listeners);

        public void Dispatch(Action<PoolListener> hook)
        {
            foreach (var listener in _listeners)
            {
                try
                {
                    hook(listener);
                }
                catch (Exception ex)
                {
                    // listeners must never break the pool
                    Trace.TraceError($"ConnPool: pool listener {listener} failed: {ex}");
                }
            }
        }
    }
}
