// One pool per key (e.g. per host, per tenant, per database) created on demand.
//
// Both registries are *lazy*: a pool is built on first use and is NOT Open()-ed, so it does no
// background maintenance unless you open it yourself (registry.Pool(key).Open()).
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConnPool
{
    public sealed class PoolRegistry<TKey, T> : IDisposable where TKey : notnull where T : class
    {
        private readonly Func<TKey, ConnectionFactory<T>> _factoryFor;
        private readonly PoolConfig? _config;
        private readonly Dictionary<TKey, ConnectionPool<T>> _pools = new();
        private readonly object _lock = new();
        private bool _closed;

        public PoolRegistry(Func<TKey, ConnectionFactory<T>> factoryFor, PoolConfig? config = null)
        {
            _factoryFor = factoryFor;
            _config = config;
        }

        public ConnectionPool<T> Pool(TKey key)
        {
            lock (_lock)
            {
                if (_closed)
                    throw new InvalidOperationException("registry is closed");
                if (!_pools.TryGetValue(key, out var pool))
                {
                    pool = new ConnectionPool<T>(_factoryFor(key), _config, name: key.ToString() ?? "pool");
                    _pools[key] = pool;
                }
                return pool;
            }
        }

        public T Acquire(TKey key, Timeout timeout = default) => Pool(key).Acquire(timeout);

        public void Use(TKey key, Action<T> body, Timeout timeout = default) => Pool(key).Use(body, timeout);

        public TResult Use<TResult>(TKey key, Func<T, TResult> body, Timeout timeout = default) =>
            Pool(key).Use(body, timeout);

        public ConnectionPool<T>.Lease Connection(TKey key, Timeout timeout = default) =>
            Pool(key).Connection(timeout);

        public void Close()
        {
            List<ConnectionPool<T>> pools;
            lock (_lock)
            {
                _closed = true;
                pools = _pools.Values.ToList();
                _pools.Clear();
            }
            foreach (var pool in pools)
                pool.Close();
        }

        void IDisposable.Dispose() => Close();
    }

    public sealed class AsyncPoolRegistry<TKey, T> : IAsyncDisposable where TKey : notnull where T : class
    {
        private readonly Func<TKey, AsyncConnectionFactory<T>> _factoryFor;
        private readonly PoolConfig? _config;
        private readonly Dictionary<TKey, AsyncConnectionPool<T>> _pools = new();
        private readonly object _lock = new();
        private bool _closed;

        public AsyncPoolRegistry(Func<TKey, AsyncConnectionFactory<T>> factoryFor, PoolConfig? config = null)
        {
            _factoryFor = factoryFor;
            _config = config;
        }

        public Task<AsyncConnectionPool<T>> PoolAsync(TKey key)
        {
            lock (_lock)
            {
                if (_closed)
                    throw new InvalidOperationException("registry is closed");
                if (!_pools.TryGetValue(key, out var pool))
                {
                    pool = new AsyncConnectionPool<T>(_factoryFor(key), _config, name: key.ToString() ?? "pool");
                    _pools[key] = pool;
                }
                return Task.FromResult(pool);
            }
        }

        public async Task<T> AcquireAsync(TKey key, Timeout timeout = default)
        {
            var pool = await PoolAsync(key).ConfigureAwait(false);
            return await pool.AcquireAsync(timeout).ConfigureAwait(false);
        }

        public async Task UseAsync(TKey key, Func<T, Task> body, Timeout timeout = default)
        {
            var pool = await PoolAsync(key).ConfigureAwait(false);
            await pool.UseAsync(body, timeout).ConfigureAwait(false);
        }

        public async Task<TResult> UseAsync<TResult>(TKey key, Func<T, Task<TResult>> body, Timeout timeout = default)
        {
            var pool = await PoolAsync(key).ConfigureAwait(false);
            return await pool.UseAsync(body, timeout).ConfigureAwait(false);
        }

        public async Task CloseAsync()
        {
            List<AsyncConnectionPool<T>> pools;
            lock (_lock)
            {
                _closed = true;
                pools = _pools.Values.ToList();
                _pools.Clear();
            }
            foreach (var pool in pools)
                await pool.CloseAsync().ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync() => await CloseAsync().ConfigureAwait(false);
    }
}
