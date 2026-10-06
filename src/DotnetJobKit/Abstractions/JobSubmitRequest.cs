namespace DotnetJobKit.Abstractions;

public sealed class JobSubmitRequest
{
    public required string Queue { get; init; }
    public required string ContractName { get; init; }
    public required int ContractVersion { get; init; }
    public required string Payload { get; init; }
    public DateTimeOffset? EligibleAt { get; init; }
    public int? MaxAttempts { get; init; }
    public string? IdempotencyKey { get; init; }
    public TimeSpan? IdempotencyTtl { get; init; }
}
