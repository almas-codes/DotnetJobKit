namespace DotnetJobKit.Abstractions;

public readonly record struct JobOwnership(Guid JobId, int AttemptCount, Guid? LeaseToken);
