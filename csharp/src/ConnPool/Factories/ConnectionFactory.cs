// Synchronous factory contract: the ONE seam between the pool and the outside world.
//
// To pool *anything* (DB driver, HTTP client, SSH session, socket, gRPC channel...)
// subclass ConnectionFactory - or just pass delegates via CallableFactory.
using System;

namespace ConnPool
{
    public abstract class ConnectionFactory<T> where T : class
    {
        /// <summary>Open a new connection. Should enforce its own connect timeout.</summary>
        public abstract T Create();

        /// <summary>Cheap liveness check (e.g. <c>SELECT 1</c>). Return false/throw = dead.</summary>
        public virtual bool Validate(T conn) => true;

        /// <summary>Called on release to scrub session state (e.g. rollback).</summary>
        public virtual void Reset(T conn)
        {
        }

        /// <summary>Dispose of a connection. Default: Dispose() if the connection is IDisposable.</summary>
        public virtual void Close(T conn)
        {
            if (conn is IDisposable disposable)
                disposable.Dispose();
        }

        /// <summary>True if <paramref name="exc"/> (raised by user code using the connection)
        /// proves the connection is broken, so it is discarded without further checks.</summary>
        public virtual bool IsFatalError(Exception exc) => false;
    }

    /// <summary>Build a factory from plain delegates.</summary>
    public sealed class CallableFactory<T> : ConnectionFactory<T> where T : class
    {
        private readonly Func<T> _create;
        private readonly Action<T>? _close;
        private readonly Func<T, bool>? _validate;
        private readonly Action<T>? _reset;
        private readonly Func<Exception, bool>? _isFatal;

        public CallableFactory(
            Func<T> create,
            Action<T>? close = null,
            Func<T, bool>? validate = null,
            Action<T>? reset = null,
            Func<Exception, bool>? isFatalError = null)
        {
            _create = create;
            _close = close;
            _validate = validate;
            _reset = reset;
            _isFatal = isFatalError;
        }

        public override T Create() => _create();

        public override bool Validate(T conn) => _validate is null || _validate(conn);

        public override void Reset(T conn) => _reset?.Invoke(conn);

        public override void Close(T conn)
        {
            if (_close != null)
                _close(conn);
            else
                base.Close(conn);
        }

        public override bool IsFatalError(Exception exc) => _isFatal != null && _isFatal(exc);
    }
}
