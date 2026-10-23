using System.Collections.Generic;
using System.Linq;
using ConnPool;
using Xunit;

namespace ConnPool.Tests.Observability
{
    public class PercentileTests
    {
        [Fact]
        public void EmptyIsZero()
        {
            Assert.Equal(0.0, Statistics.Percentile(new List<double>(), 0.5));
        }

        [Fact]
        public void KnownQuantiles()
        {
            var vals = Enumerable.Range(1, 100).Select(i => (double)i).ToList(); // 1..100, sorted
            Assert.Equal(1.0, Statistics.Percentile(vals, 0.0), 6);
            Assert.Equal(100.0, Statistics.Percentile(vals, 1.0), 6);
            Assert.Equal(50.5, Statistics.Percentile(vals, 0.5), 6);     // linear interpolation
            Assert.Equal(99.01, Statistics.Percentile(vals, 0.99), 6);
        }
    }

    public class CollectorTests
    {
        [Fact]
        public void AcquireWaitPercentilesInSnapshot()
        {
            var c = new StatsCollector();
            foreach (double w in new[] { 0.1, 0.2, 0.3, 0.4, 100.0 }) // one slow outlier
                c.OnAcquired(null, w);
            PoolStats s = c.Snapshot("x", 1, 0, 0, 0, 0, null);
            Assert.Equal(20.2, s.AvgAcquireWait, 3);
            Assert.True(s.P50AcquireWait < 1.0);   // median unaffected by the outlier
            Assert.True(s.P99AcquireWait > 50.0);  // tail catches it
        }

        [Fact]
        public void CreateLatencyPercentilesInSnapshot()
        {
            var c = new StatsCollector();
            foreach (double d in new[] { 0.01, 0.02, 0.03, 0.04, 5.0 }) // one slow create
                c.OnConnectionCreated(null, d);
            PoolStats s = c.Snapshot("x", 1, 0, 0, 0, 0, null);
            Assert.Equal(5.1 / 5, s.AvgCreateLatency, 6);
            Assert.True(s.P50CreateLatency < 1.0);
            Assert.True(s.P99CreateLatency > 1.0);
        }

        [Fact]
        public void WaitWindowIsBounded()
        {
            var c = new StatsCollector();
            for (int i = 0; i < Statistics.AcquireWaitWindow + 500; i++)
                c.OnAcquired(null, 1.0);
            Assert.Equal(Statistics.AcquireWaitWindow, c.WaitsCount);
        }

        [Fact]
        public void SnapshotToDictionary()
        {
            var c = new StatsCollector();
            PoolStats s = c.Snapshot("x", 2, 1, 1, 0, 0, null);
            IReadOnlyDictionary<string, object?> d = s.ToDictionary();
            Assert.Equal("x", d["name"]);
            Assert.Equal(2, d["max_size"]);
            Assert.True(d.ContainsKey("p99_acquire_wait"));
        }
    }
}
