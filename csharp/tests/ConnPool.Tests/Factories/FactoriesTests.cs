using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ConnPool;
using Xunit;

namespace ConnPool.Tests.Factories
{
    internal sealed class Conn : IDisposable
    {
        public bool Closed;
        public void Dispose() => Closed = true;
    }

    public class SyncFactoryTests
    {
        private sealed class Minimal : ConnectionFactory<Conn>
        {
            public override Conn Create() => new Conn();
        }

        private sealed class MinimalObject : ConnectionFactory<object>
        {
            public override object Create() => new object();
        }

        [Fact]
        public void BaseClassDefaults()
        {
            var f = new Minimal();
            Conn c = f.Create();
            Assert.True(f.Validate(c));                       // default: always healthy
            Assert.False(f.IsFatalError(new InvalidOperationException())); // default: nothing is fatal
            f.Reset(c);                                        // default: no-op
            f.Close(c);                                         // default: Dispose() the connection
            Assert.True(c.Closed);
        }

        [Fact]
        public void BaseCloseWithoutDisposable()
        {
            new MinimalObject().Close(new object());           // not IDisposable: silently fine
        }

        [Fact]
        public void CallableFactoryAllCallbacks()
        {
            var resets = new List<Conn>();
            var f = new CallableFactory<Conn>(
                () => new Conn(),
                validate: c => true,
                reset: c => resets.Add(c),
                isFatalError: e => e is ArgumentException);
            Conn c = f.Create();
            Assert.True(f.Validate(c));
            f.Reset(c);
            Assert.Equal(new[] { c }, resets);
            Assert.True(f.IsFatalError(new ArgumentException()));
            Assert.False(f.IsFatalError(new InvalidOperationException()));
        }

        [Fact]
        public void CallableFactoryDefaultsWhenCallbacksOmitted()
        {
            var f = new CallableFactory<Conn>(() => new Conn());
            Conn c = f.Create();
            Assert.True(f.Validate(c));                         // null validate -> true
            f.Reset(c);                                         // null reset -> no-op
            Assert.False(f.IsFatalError(new InvalidOperationException()));
            f.Close(c);                                         // falls back to Dispose()
            Assert.True(c.Closed);
        }
    }

    public class AsyncFactoryTests
    {
        private sealed class Minimal : AsyncConnectionFactory<Conn>
        {
            public override Task<Conn> CreateAsync(CancellationToken cancellationToken) =>
                Task.FromResult(new Conn());
        }

        private sealed class MinimalObject : AsyncConnectionFactory<object>
        {
            public override Task<object> CreateAsync(CancellationToken cancellationToken) =>
                Task.FromResult(new object());
        }

        [Fact]
        public async Task BaseClassDefaults()
        {
            var f = new Minimal();
            Conn c = await f.CreateAsync(default);
            Assert.True(await f.ValidateAsync(c, default));
            await f.ResetAsync(c, default);
            Assert.False(f.IsFatalError(new InvalidOperationException()));
            await f.CloseAsync(c, default);                    // Dispose() the connection
            Assert.True(c.Closed);
        }

        [Fact]
        public async Task BaseCloseWithoutDisposable()
        {
            await new MinimalObject().CloseAsync(new object(), default);
        }

        [Fact]
        public async Task CallableFactoryAllCallbacksSyncAndAsync()
        {
            var resets = new List<Conn>();
            var f = new AsyncCallableFactory<Conn>(
                _ => Task.FromResult(new Conn()),
                validate: (c, _) => Task.FromResult(true),
                reset: (c, _) => { resets.Add(c); return Task.CompletedTask; },
                isFatalError: e => e is ArgumentException);
            Conn c = await f.CreateAsync(default);
            Assert.True(await f.ValidateAsync(c, default));
            await f.ResetAsync(c, default);
            Assert.Equal(new[] { c }, resets);
            Assert.True(f.IsFatalError(new ArgumentException()));
        }

        [Fact]
        public async Task CallableFactoryDefaultsWhenCallbacksOmitted()
        {
            var f = new AsyncCallableFactory<Conn>(_ => Task.FromResult(new Conn()));
            Conn c = await f.CreateAsync(default);
            Assert.True(await f.ValidateAsync(c, default));     // null validate -> true
            await f.ResetAsync(c, default);                     // null reset -> no-op
            Assert.False(f.IsFatalError(new InvalidOperationException()));
            await f.CloseAsync(c, default);                     // falls back to Dispose()
            Assert.True(c.Closed);
        }

        [Fact]
        public async Task FromSyncAdaptsSyncCallbacks()
        {
            var resets = new List<Conn>();
            var closed = new List<Conn>();
            var f = AsyncCallableFactory<Conn>.FromSync(
                () => new Conn(),
                close: c => closed.Add(c),
                validate: c => true,
                reset: c => resets.Add(c),
                isFatalError: e => e is ArgumentException);
            Conn c = await f.CreateAsync(default);
            Assert.True(await f.ValidateAsync(c, default));
            await f.ResetAsync(c, default);
            await f.CloseAsync(c, default);
            Assert.Equal(new[] { c }, resets);
            Assert.Equal(new[] { c }, closed);
            Assert.True(f.IsFatalError(new ArgumentException()));
        }

        [Fact]
        public async Task FromSyncOmittingCallbacksUsesDefaults()
        {
            var f = AsyncCallableFactory<Conn>.FromSync(() => new Conn()); // null close/validate/reset
            Conn c = await f.CreateAsync(default);
            Assert.True(await f.ValidateAsync(c, default));     // null validate -> true
            await f.ResetAsync(c, default);                     // null reset -> no-op
            await f.CloseAsync(c, default);                     // null close -> Dispose()
            Assert.True(c.Closed);
        }
    }
}
