using System.Threading.Tasks;
using ConnPool;
using Xunit;

namespace ConnPool.Tests.Core
{
    public class SystemClockTests
    {
        [Fact]
        public void MonotonicAndSleep()
        {
            var clock = new SystemClock();
            double before = clock.Monotonic();
            clock.Sleep(0.001);
            Assert.True(clock.Monotonic() >= before);
        }

        [Fact]
        public async Task AsyncSleep()
        {
            await new SystemClock().AsyncSleepAsync(0.001);
        }
    }

    public class ManualClockTests
    {
        [Fact]
        public void TimeOnlyMovesWhenTold()
        {
            var clock = new ManualClock(100.0);
            Assert.Equal(100.0, clock.Monotonic());
            clock.Advance(5);
            Assert.Equal(105.0, clock.Monotonic());
            clock.Sleep(2); // sleeping just advances time
            Assert.Equal(107.0, clock.Monotonic());
        }

        [Fact]
        public async Task AsyncSleepAdvances()
        {
            var clock = new ManualClock(0.0);
            await clock.AsyncSleepAsync(3);
            Assert.Equal(3.0, clock.Monotonic());
        }
    }
}
