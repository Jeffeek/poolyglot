// Keep-alive TCP sockets, one pool per host (illustration; needs network to actually connect).
//
// Why per-host pooling?
// - Avoids head-of-line blocking: one slow host won't starve requests to fast hosts
// - Each host gets its own connection limit (MaxSize per pool)
// - PoolRegistry creates pools lazily on first use
//
// Pattern: PoolRegistry keyed by "host:port"
// - Key = "host:port" (e.g., "api.github.com:443", "example.com:80")
// - Value = pool of TCP connections to that host:port
// - Config is shared across all hosts (MaxSize=8 per host)
//
// A PoolRegistry gives you one lazily-created pool per key - here per "host:port" - which is the
// natural shape for per-host connection reuse (HTTP keep-alive, SMTP, a custom RPC, ...).
using System.Net.Sockets;
using ConnPool;

namespace ConnPool.Examples;

public sealed class TcpFactory : ConnectionFactory<TcpClient>
{
    private readonly string _host;
    private readonly int _port;

    public TcpFactory(string host, int port)
    {
        _host = host;
        _port = port;
    }

    public override TcpClient Create()
    {
        var client = new TcpClient();
        client.Connect(_host, _port);  // fail in Create(), not in the caller's request
        return client;
    }

    public override bool Validate(TcpClient conn) => conn.Connected; // socket still open?

    public override void Close(TcpClient conn) => conn.Dispose();

    public override bool IsFatalError(Exception exc) => exc is SocketException or IOException;
}

public static class TcpKeepAlivePerHost
{
    public static void Run()
    {
        // Create registry: one pool per host:port, shared config
        using var hosts = new PoolRegistry<string, TcpClient>(
            key =>
            {
                string[] parts = key.Split(':');
                return new TcpFactory(parts[0], int.Parse(parts[1]));
            },
            new PoolConfig { MaxSize = 8, MaxLifetime = 300, ValidateAfterIdle = 5 });

        // First request to example.com:80 creates a pool lazily
        using var lease = hosts.Connection("example.com:80");
        NetworkStream stream = lease.Value.GetStream();
        // ... write a request, read a response ...
        _ = stream;

        // Pool stats (per-host)
        var pool = hosts.Pool("example.com:80");
        var stats = pool.Stats();
        Console.WriteLine($"\nPool stats for example.com:80:");
        Console.WriteLine($"  Size: {stats.Size}/{stats.MaxSize}");
        Console.WriteLine($"  Created: {stats.Created}, Closed: {stats.Closed}");
        Console.WriteLine($"  Acquire wait p95: {stats.P95AcquireWait:F3}s");
    }
}
