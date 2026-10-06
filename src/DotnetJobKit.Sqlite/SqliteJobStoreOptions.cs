using DotnetJobKit.Configuration;

namespace DotnetJobKit.Sqlite;

public sealed class SqliteJobStoreOptions
{
    public required string ConnectionString { get; set; }
    public int BusyTimeoutMs { get; set; } = 5000;
    public bool EnableWal { get; set; } = true;
    public bool UseBeginImmediate { get; set; } = true;
    public int Synchronous { get; set; } = 1;
    public long CacheSizeKiB { get; set; } = -64000;
    public long MmapSizeBytes { get; set; } = 268_435_456;
    public OwnershipFencingMode OwnershipFencing { get; set; } = OwnershipFencingMode.AttemptCount;
}
