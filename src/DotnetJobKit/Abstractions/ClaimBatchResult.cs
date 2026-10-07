namespace DotnetJobKit.Abstractions;

public sealed record ClaimBatchResult(
    IReadOnlyList<ClaimedJob> Jobs,
    DateTimeOffset? NextDueAt);
