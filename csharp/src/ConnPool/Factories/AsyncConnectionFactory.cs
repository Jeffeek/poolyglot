// Asynchronous factory contract: async twin of ConnectionFactory.
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ConnPool
{
    public abstract class AsyncConnectionFactory<T> where T : class
    {
        public abstract Task<T> CreateAsync(CancellationToken cancellationToken);

        public virtual Task<bool> ValidateAsync(T conn, CancellationToken cancellationToken) =>
            Task.FromResult(true);

        public virtual Task ResetAsync(T conn, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public virtual async Task CloseAsync(T conn, CancellationToken cancellationToken)
        {
            if (conn is IAsyncDisposable asyncDisposable)
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            else if (conn is IDisposable disposable)
                disposable.Dispose();
        }

        public virtual bool IsFatalError(Exception exc) => false;
    }

    /// <summary>Build an async factory from plain delegates.</summary>
    public sealed class AsyncCallableFactory<T> : AsyncConnectionFactory<T> where T : class
    {
        private readonly Func<CancellationToken, Task<T>> _create;
        private readonly Func<T, CancellationToken, Task>? _close;
        private readonly Func<T, CancellationToken, Task<bool>>? _validate;
        private readonly Func<T, CancellationToken, Task>? _reset;
        private readonly Func<Exception, bool>? _isFatal;

        public AsyncCallableFactory(
            Func<CancellationToken, Task<T>> create,
            Func<T, CancellationToken, Task>? close = null,
            Func<T, CancellationToken, Task<bool>>? validate = null,
            Func<T, CancellationToken, Task>? reset = null,
            Func<Exception, bool>? isFatalError = null)
        {
            _create = create;
            _close = close;
            _validate = validate;
            _reset = reset;
            _isFatal = isFatalError;
        }

        /// <summary>Convenience for a synchronous creator.</summary>
        public static AsyncCallableFactory<T> FromSync(
            Func<T> create,
            Action<T>? close = null,
            Func<T, bool>? validate = null,
            Action<T>? reset = null,
            Func<Exception, bool>? isFatalError = null) =>
            new AsyncCallableFactory<T>(
                _ => Task.FromResult(create()),
                close is null ? (Func<T, CancellationToken, Task>?)null : (c, _) => { close(c); return Task.CompletedTask; },
                validate is null ? (Func<T, CancellationToken, Task<bool>>?)null : (c, _) => Task.FromResult(validate(c)),
                reset is null ? (Func<T, CancellationToken, Task>?)null : (c, _) => { reset(c); return Task.CompletedTask; },
                isFatalError);

        public override Task<T> CreateAsync(CancellationToken cancellationToken) => _create(cancellationToken);

        public override Task<bool> ValidateAsync(T conn, CancellationToken cancellationToken) =>
            _validate is null ? Task.FromResult(true) : _validate(conn, cancellationToken);

        public override Task ResetAsync(T conn, CancellationToken cancellationToken) =>
            _reset is null ? Task.CompletedTask : _reset(conn, cancellationToken);

        public override Task CloseAsync(T conn, CancellationToken cancellationToken) =>
            _close is null ? base.CloseAsync(conn, cancellationToken) : _close(conn, cancellationToken);

        public override bool IsFatalError(Exception exc) => _isFatal != null && _isFatal(exc);
    }
}
