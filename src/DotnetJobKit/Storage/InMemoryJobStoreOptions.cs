using DotnetJobKit.Configuration;

namespace DotnetJobKit.Storage;

public sealed class InMemoryJobStoreOptions
{
    public OwnershipFencingMode OwnershipFencing { get; set; } = OwnershipFencingMode.AttemptCount;
}
