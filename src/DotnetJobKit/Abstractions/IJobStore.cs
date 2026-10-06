namespace DotnetJobKit.Abstractions;

public interface IJobStore
{
    Task<Guid> SubmitAsync(JobSubmitRequest request, DateTimeOffset now, CancellationToken cancellationToken);

    Task<IReadOnlyList<ClaimedJob>> ClaimAsync(
        IReadOnlyList<string> queues,
        int maxCount,
        DateTimeOffset now,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken);

    Task<bool> RenewAsync(
        Guid jobId,
        int attemptCount,
        Guid? leaseToken,
        DateTimeOffset leaseExpiresAt,
        CancellationToken cancellationToken);

    Task<bool> IsCancellationRequestedAsync(
        Guid jobId,
        int attemptCount,
        Guid? leaseToken,
        CancellationToken cancellationToken);

    Task<bool> SettleAsync(SettleRequest request, CancellationToken cancellationToken);

    Task<bool> CancelReadyAsync(Guid jobId, DateTimeOffset now, CancellationToken cancellationToken);

    Task<bool> RequestLeasedCancellationAsync(Guid jobId, CancellationToken cancellationToken);

    Task<JobRecord?> GetAsync(Guid jobId, CancellationToken cancellationToken);

    Task<DateTimeOffset?> GetNextEligibleAtAsync(
        IReadOnlyList<string> queues,
        DateTimeOffset now,
        CancellationToken cancellationToken);

    Task<int> DeleteTerminalBatchAsync(DateTimeOffset now, int batchSize, CancellationToken cancellationToken);
}
