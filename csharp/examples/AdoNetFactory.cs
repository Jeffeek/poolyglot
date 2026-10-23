// One factory for ANY ADO.NET provider.
//
// Every provider exposes the common System.Data.Common base types (DbConnection, DbCommand),
// so a single adapter covers SqlClient, Npgsql, MySqlConnector, Microsoft.Data.Sqlite, ...
// The pool itself knows nothing about databases; this lives in examples/ because it is *client* code.
using System.Data.Common;
using ConnPool;

namespace ConnPool.Examples;

public sealed class AdoNetFactory : ConnectionFactory<DbConnection>
{
    private readonly Func<DbConnection> _connect;
    private readonly string _pingSql;
    private readonly Action<DbConnection>? _onOpen;

    public AdoNetFactory(Func<DbConnection> connect, string pingSql = "SELECT 1",
        Action<DbConnection>? onOpen = null)
    {
        _connect = connect;
        _pingSql = pingSql;
        _onOpen = onOpen;
    }

    public override DbConnection Create()
    {
        DbConnection conn = _connect();
        conn.Open();                 // fail here, not in the caller's query
        _onOpen?.Invoke(conn);
        return conn;
    }

    public override bool Validate(DbConnection conn)
    {
        using DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = _pingSql;
        cmd.ExecuteScalar();
        return true;
    }

    // Default Reset is a no-op; override to roll back an ambient transaction if you use one.

    public override void Close(DbConnection conn) => conn.Dispose();

    public override bool IsFatalError(Exception exc) => exc is DbException;
}
