namespace DotnetJobKit.Handlers;

public sealed class JobContext
{
    public required Guid JobId { get; init; }
    public required string Queue { get; init; }
    public required string ContractName { get; init; }
    public required int AttemptCount { get; init; }
    public required int MaxAttempts { get; init; }
}
