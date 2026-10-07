namespace DotnetJobKit.Abstractions;

public readonly record struct LeaseMaintenanceRequest(
    Guid JobId,
    int AttemptCount,
    Guid? LeaseToken,
    DateTimeOffset NewLeaseExpiry);

public sealed record LeaseMaintenanceResult(
    Guid JobId,
    int AttemptCount,
    bool StillOwner,
    bool CancellationRequested,
    DateTimeOffset? LeaseExpiresAt);
