using DotnetJobKit.Configuration;

namespace DotnetJobKit.MySql;

public sealed class MySqlJobStoreOptions
{
    public required string ConnectionString { get; set; }
    public OwnershipFencingMode OwnershipFencing { get; set; } = OwnershipFencingMode.AttemptCount;
}
