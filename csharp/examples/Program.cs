// Runnable demos:  dotnet run --project examples
//
//   1. a thread-pool hammering a SQLite database through a sync ConnectionPool
//   2. an asyncio-style AsyncConnectionPool fanning out concurrent calls
using System.Data.Common;
using ConnPool;
using ConnPool.Examples;
using Microsoft.Data.Sqlite;

SqliteDemo();
await AsyncDemo();

static void SqliteDemo()
{
    Console.WriteLine("== SQLite demo (sync pool) ==");
    string path = Path.Combine(Path.GetTempPath(), $"connpool-demo-{Guid.NewGuid():N}.db");

    // SQLite is a convenient standard-library-ish stand-in: swap the connect lambda for any
    // other ADO.NET provider (Npgsql, SqlClient, ...) and nothing else changes.
    var factory = new AdoNetFactory(
        () => new SqliteConnection($"Data Source={path}"),
        onOpen: conn =>
        {
            using DbCommand pragma = conn.CreateCommand();
            pragma.CommandText = "PRAGMA busy_timeout=5000"; // wait out writer locks instead of failing
            pragma.ExecuteNonQuery();
        });

    using var pool = new ConnectionPool<DbConnection>(
        factory, new PoolConfig { MinSize = 2, MaxSize = 4, MaxUses = 500 }, name: "sqlite");
    pool.Open();

    pool.Use(db => Exec(db, "CREATE TABLE hits (n INTEGER)"));

    Parallel.For(0, 200, i => pool.Use(db =>
    {
        using DbCommand cmd = db.CreateCommand();
        cmd.CommandText = "INSERT INTO hits VALUES ($n)";
        DbParameter p = cmd.CreateParameter();
        p.ParameterName = "$n";
        p.Value = i;
        cmd.Parameters.Add(p);
        cmd.ExecuteNonQuery();
    }));

    long rows = pool.Use(db =>
    {
        using DbCommand cmd = db.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM hits";
        return Convert.ToInt64(cmd.ExecuteScalar());
    });

    Console.WriteLine($"rows: {rows}");
    Console.WriteLine(pool.Stats());

    static void Exec(DbConnection db, string sql)
    {
        using DbCommand cmd = db.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}

static async Task AsyncDemo()
{
    Console.WriteLine();
    Console.WriteLine("== async demo (AsyncConnectionPool) ==");

    // Swap FakeClient for asyncpg/Npgsql/StackExchange.Redis/HttpClient-backed clients.
    var pool = AsyncConnectionPool<FakeClient>.FromCallables(
        _ => Task.FromResult(new FakeClient()),
        validate: (c, _) => c.PingAsync(),
        close: (c, _) => c.CloseAsync(),
        config: new PoolConfig
        {
            MinSize = 1,
            MaxSize = 5,
            AcquireTimeout = 2,
            CreateTimeout = 3,
            Retry = new RetryPolicy { MaxAttempts = 3, BaseDelay = 0.2 },
            Breaker = new BreakerConfig { FailureThreshold = 5, RecoveryTimeout = 20 },
        });

    await using (pool)
    {
        await pool.OpenAsync();
        int[] results = await Task.WhenAll(
            Enumerable.Range(0, 100).Select(i => pool.UseAsync(client => client.CallAsync(i))));
        Console.WriteLine($"sum: {results.Sum()}");
        Console.WriteLine(pool.Stats());
    }
}

sealed class FakeClient
{
    public async Task<bool> PingAsync()
    {
        await Task.Yield();
        return true;
    }

    public async Task<int> CallAsync(int n)
    {
        await Task.Delay(10);
        return n * 2;
    }

    public Task CloseAsync() => Task.CompletedTask;
}
