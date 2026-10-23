using ConnPool;
using Xunit;

namespace ConnPool.Tests.Core
{
    public class PooledConnectionTests
    {
        [Fact]
        public void AgeIdleAndHoldTimes()
        {
            var c = new PooledConnection<object>(new object(), createdAt: 100.0);
            Assert.Equal(10.0, c.Age(110.0));
            Assert.Equal(5.0, c.IdleTime(105.0));   // LastReturnedAt defaults to CreatedAt
            Assert.Equal(0.0, c.HoldTime(110.0));    // not checked out yet

            c.MarkCheckedOut(120.0);
            Assert.Equal(1, c.UseCount);
            Assert.Equal(3.0, c.HoldTime(123.0));

            c.MarkReturned(130.0);
            Assert.Equal(1.0, c.IdleTime(131.0));
            Assert.Null(c.CheckedOutAt);
        }

        [Fact]
        public void IdleTimeFallsBackToCreatedAtWhenNeverReturned()
        {
            var c = new PooledConnection<object>(new object(), createdAt: 100.0);
            c.LastReturnedAt = null;                 // defensive: public setter may clear it
            Assert.Equal(5.0, c.IdleTime(105.0));    // falls back to CreatedAt
        }

        [Fact]
        public void ToStringMentionsType()
        {
            var c = new PooledConnection<object>(new object(), createdAt: 0.0);
            Assert.Contains("PooledConnection", c.ToString());
        }
    }
}
