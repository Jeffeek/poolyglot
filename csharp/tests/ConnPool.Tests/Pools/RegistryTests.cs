using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConnPool;
using ConnPool.Tests.Support;
using Xunit;

namespace ConnPool.Tests.Pools
{
    internal sealed class KeyedConn
    {
        public string Key { get; }
        public KeyedConn(string key) => Key = key;
    }

    /// <summary>A key whose ToString() is null, exercising the "pool" fallback name.</summary>
    internal sealed class NullNameKey
    {
        public override string? ToString() => null;
    }

    public class RegistryTests
    {
        [Fact]
        public void OnePoolPerKey()
        {
            var made = new List<string>();
            ConnectionFactory<KeyedConn> FactoryFor(string key)
            {
                made.Add(key);
                return new CallableFactory<KeyedConn>(() => new KeyedConn(key));
            }

            var reg = new PoolRegistry<string, KeyedConn>(FactoryFor, Helpers.QuietConfig());
            KeyedConn a1 = reg.Use("a", c => c);
            KeyedConn a2 = reg.Use("a", c => c);
            KeyedConn b = reg.Use("b", c => c);
            Assert.Same(a1, a2);
            Assert.Equal(new[] { "a", "b" }, made.OrderBy(x => x));
            Assert.Equal("b", b.Key);

            reg.Close();
            Assert.Throws<InvalidOperationException>(() => reg.Pool("a"));
        }

        [Fact]
        public void AcquireUseVoidConnectionAndDispose()
        {
            using var reg = new PoolRegistry<string, KeyedConn>(
                k => new CallableFactory<KeyedConn>(() => new KeyedConn(k)), Helpers.QuietConfig());

            KeyedConn direct = reg.Acquire("a");
            reg.Pool("a").Release(direct);

            bool ran = false;
            reg.Use("a", _ => { ran = true; }); // block body -> the void Use overload
            Assert.True(ran);

            using (var lease = reg.Connection("b"))
                Assert.Equal("b", lease.Value.Key);
            // IDisposable.Dispose runs at the end of the outer `using var reg`
        }

        [Fact]
        public void KeyWithNullToStringFallsBackToPoolName()
        {
            using var reg = new PoolRegistry<NullNameKey, KeyedConn>(
                _ => new CallableFactory<KeyedConn>(() => new KeyedConn("x")), Helpers.QuietConfig());
            ConnectionPool<KeyedConn> pool = reg.Pool(new NullNameKey());
            Assert.Equal("pool", pool.Name);
        }
    }

    public class AsyncRegistryTests
    {
        [Fact]
        public async Task OnePoolPerKey()
        {
            var made = new List<string>();
            AsyncConnectionFactory<KeyedConn> FactoryFor(string key)
            {
                made.Add(key);
                return new AsyncCallableFactory<KeyedConn>(_ => Task.FromResult(new KeyedConn(key)));
            }

            var reg = new AsyncPoolRegistry<string, KeyedConn>(FactoryFor, Helpers.QuietConfig());
            KeyedConn a1 = await reg.UseAsync("a", c => Task.FromResult(c));
            KeyedConn a2 = await reg.UseAsync("a", c => Task.FromResult(c));
            KeyedConn b = await reg.UseAsync("b", c => Task.FromResult(c));
            Assert.Same(a1, a2);
            Assert.Equal(new[] { "a", "b" }, made.OrderBy(x => x));
            Assert.Equal("b", b.Key);

            await reg.CloseAsync();
            await Assert.ThrowsAsync<InvalidOperationException>(() => reg.PoolAsync("a"));
        }

        [Fact]
        public async Task AcquireUseVoidAndDispose()
        {
            var reg = new AsyncPoolRegistry<string, KeyedConn>(
                k => new AsyncCallableFactory<KeyedConn>(_ => Task.FromResult(new KeyedConn(k))), Helpers.QuietConfig());

            KeyedConn direct = await reg.AcquireAsync("a");
            AsyncConnectionPool<KeyedConn> pool = await reg.PoolAsync("a");
            await pool.ReleaseAsync(direct);

            bool ran = false;
            await reg.UseAsync("a", _ => { ran = true; return Task.CompletedTask; });
            Assert.True(ran);

            await reg.DisposeAsync();
            await Assert.ThrowsAsync<InvalidOperationException>(() => reg.PoolAsync("a"));
        }

        [Fact]
        public async Task KeyWithNullToStringFallsBackToPoolName()
        {
            await using var reg = new AsyncPoolRegistry<NullNameKey, KeyedConn>(
                _ => new AsyncCallableFactory<KeyedConn>(_ => Task.FromResult(new KeyedConn("x"))), Helpers.QuietConfig());
            AsyncConnectionPool<KeyedConn> pool = await reg.PoolAsync(new NullNameKey());
            Assert.Equal("pool", pool.Name);
        }
    }
}
