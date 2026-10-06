using DotnetJobKit.Configuration;

namespace DotnetJobKit.PostgreSql;

public sealed class PostgreSqlJobStoreOptions
{
    public required string ConnectionString { get; set; }
    public OwnershipFencingMode OwnershipFencing { get; set; } = OwnershipFencingMode.AttemptCount;
}
