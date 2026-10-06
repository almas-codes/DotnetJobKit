namespace DotnetJobKit.Abstractions;

public sealed class ClaimedJob
{
    public required Guid JobId { get; init; }
    public required string Queue { get; init; }
    public required string ContractName { get; init; }
    public required int ContractVersion { get; init; }
    public required string Payload { get; init; }
    public required int AttemptCount { get; init; }
    public required int MaxAttempts { get; init; }
    public required bool CancellationRequested { get; init; }
    public required DateTimeOffset LeaseExpiresAt { get; init; }
    public Guid? LeaseToken { get; init; }
}
