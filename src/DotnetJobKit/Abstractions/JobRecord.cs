namespace DotnetJobKit.Abstractions;

public sealed class JobRecord
{
    public required Guid JobId { get; init; }
    public required string Queue { get; init; }
    public required string ContractName { get; init; }
    public required int ContractVersion { get; init; }
    public required string Payload { get; init; }
    public required JobState State { get; init; }
    public DateTimeOffset? EligibleAt { get; init; }
    public required int AttemptCount { get; init; }
    public required int MaxAttempts { get; init; }
    public required bool CancellationRequested { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? CompletedAt { get; init; }
    public string? LastError { get; init; }
    public string? IdempotencyKey { get; init; }
    public DateTimeOffset? IdempotencyExpiresAt { get; init; }
}
