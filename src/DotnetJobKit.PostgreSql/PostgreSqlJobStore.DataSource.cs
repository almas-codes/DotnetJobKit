using Npgsql;

namespace DotnetJobKit.PostgreSql;

public sealed partial class PostgreSqlJobStore
{
    private readonly NpgsqlDataSource _dataSource;

    private async ValueTask<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
        => await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
}
