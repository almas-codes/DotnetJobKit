using DotnetJobKit.Configuration;

namespace DotnetJobKit.Abstractions;

public enum ExecutionOutcomeKind
{
    Succeeded,
    Retry,
    Dead,
    Cancelled,
    Recurring,
}

public sealed record ContinuationSpec(
    string Queue,
    string ContractName,
    int ContractVersion,
    string Payload);

public sealed record ExecutionOutcome(
    ExecutionOutcomeKind Kind,
    DateTimeOffset? NextDueAt = null,
    string? Error = null,
    ContinuationSpec? Continuation = null,
    string? RecurrenceCron = null,
    bool DeleteOnSuccess = false);

public sealed record CommitOutcomeRequest(
    Guid JobId,
    int AttemptCount,
    Guid? LeaseToken,
    ExecutionOutcome Outcome,
    int MaxAttemptsForRetryPolicy,
    RetryPolicyOptions RetryPolicy);

public enum CommitOutcomeStatus
{
    Committed,
    AlreadyCommitted,
    StaleOwner,
}

public sealed record CommitOutcomeResult(
    CommitOutcomeStatus Status,
    Guid? SuccessorJobId = null);
