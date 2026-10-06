using Microsoft.Data.Sqlite;

namespace DotnetJobKit.Sqlite;

internal sealed class SqliteImmediateTransaction : IAsyncDisposable
{
    private readonly SqliteConnection _connection;
    public SqliteTransaction? Transaction { get; private set; }
    private bool _committed;

    private SqliteImmediateTransaction(SqliteConnection connection, SqliteTransaction? transaction)
    {
        _connection = connection;
        Transaction = transaction;
    }

    public static async Task<SqliteImmediateTransaction> BeginAsync(
        SqliteConnection connection,
        bool immediate,
        CancellationToken cancellationToken)
    {
        if (connection.State != System.Data.ConnectionState.Open)
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        if (immediate)
        {
            await using var begin = connection.CreateCommand();
            begin.CommandText = "BEGIN IMMEDIATE;";
            await begin.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            return new SqliteImmediateTransaction(connection, null);
        }

        var tx = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        return new SqliteImmediateTransaction(connection, tx);
    }

    public async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_committed)
            return;

        if (Transaction is not null)
            await Transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        else
        {
            await using var commit = _connection.CreateCommand();
            commit.CommandText = "COMMIT;";
            await commit.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        _committed = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (_committed)
            return;

        if (Transaction is not null)
            await Transaction.RollbackAsync().ConfigureAwait(false);
        else
        {
            await using var rollback = _connection.CreateCommand();
            rollback.CommandText = "ROLLBACK;";
            await rollback.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }
}
