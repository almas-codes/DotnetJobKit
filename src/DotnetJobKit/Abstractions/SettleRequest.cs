namespace DotnetJobKit.Abstractions;

public sealed class SettleRequest
{
    public required Guid JobId { get; init; }
    public required int AttemptCount { get; init; }
    public Guid? LeaseToken { get; init; }
    public required SettleOutcome Outcome { get; init; }
    public DateTimeOffset? NextEligibleAt { get; init; }
    public string? LastError { get; init; }
    public required DateTimeOffset Now { get; init; }
    public bool DeleteOnSuccess { get; init; }
}
