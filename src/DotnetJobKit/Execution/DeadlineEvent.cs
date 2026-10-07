namespace DotnetJobKit.Execution;

internal readonly record struct DeadlineEvent(
    Guid JobId,
    int AttemptCount,
    DeadlineKind Kind,
    long Generation,
    DateTimeOffset DueAt);
