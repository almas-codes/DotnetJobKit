using MySqlConnector;

namespace DotnetJobKit.MySql;

public sealed partial class MySqlJobStore
{
    private readonly MySqlDataSource _dataSource;

    private async ValueTask<MySqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (var iso = connection.CreateCommand())
        {
            iso.CommandText = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;";
            await iso.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        return connection;
    }
}
