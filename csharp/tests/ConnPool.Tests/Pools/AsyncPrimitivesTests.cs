using System.Threading;
using System.Threading.Tasks;
using ConnPool;
using Xunit;

namespace ConnPool.Tests.Pools
{
    public class AsyncPrimitivesTests
    {
        [Fact]
        public async Task NotifySkipsAlreadyCancelledWaiter()
        {
            // A waiter that times out cancels its TCS, but the node is only pulled from the list by
            // ReacquireAsync once it re-grabs the mutex. If Notify runs while we hold the mutex, it meets
            // a still-listed waiter whose TrySetResult(true) returns false and must skip it.
            var cond = new AsyncCondition();

            async Task<bool> RunWaiter()
            {
                // WaitAsync enqueues the node then releases the mutex itself, and re-acquires it on return,
                // so the surrounding using disposes the lock exactly once.
                using (await cond.LockAsync())
                    return await cond.WaitAsync(timeoutSeconds: 0.1, CancellationToken.None);
            }

            Task<bool> waiter = RunWaiter();
            await Task.Delay(30); // let the waiter enqueue its node and release the mutex

            // Grab the mutex before the waiter's timeout fires, so its ReacquireAsync blocks and the
            // (soon-to-be-cancelled) node stays in the list.
            using (await cond.LockAsync())
            {
                await Task.Delay(250); // let the 0.1s timeout cancel the waiter's TCS while it stays listed
                cond.Notify();         // hits the cancelled, still-listed node -> TrySetResult returns false
            }

            Assert.False(await waiter); // the waiter reports a timeout, not a notify
        }
    }
}
