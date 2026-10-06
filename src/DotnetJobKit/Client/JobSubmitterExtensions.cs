using DotnetJobKit.Abstractions;
using DotnetJobKit.Handlers;

namespace DotnetJobKit.Client;

public static class JobSubmitterExtensions
{
    public static Task<Guid> EnqueueAsync<TJob>(
        this IJobSubmitter submitter,
        TJob job,
        JobEnqueueOptions options,
        CancellationToken cancellationToken = default)
        where TJob : notnull
    {
        if (submitter is not JobSubmitter concrete)
            throw new InvalidOperationException("Extended enqueue requires the default JobSubmitter registration.");

        return concrete.EnqueueAsync(job, options, cancellationToken);
    }

    public static Task<Guid> EnqueueWithUniqueKeyAsync<TJob>(
        this IJobSubmitter submitter,
        TJob job,
        string uniqueKey,
        TimeSpan? idempotencyTtl = null,
        CancellationToken cancellationToken = default)
        where TJob : notnull
        => submitter.EnqueueAsync(job, new JobEnqueueOptions { UniqueKey = uniqueKey, IdempotencyTtl = idempotencyTtl }, cancellationToken);

    public static Task<Guid> EnqueueRecurringAsync<TJob>(
        this IJobSubmitter submitter,
        TJob job,
        string cronExpression,
        CancellationToken cancellationToken = default)
        where TJob : notnull
        => submitter.EnqueueAsync(job, new JobEnqueueOptions { RecurrenceCron = cronExpression }, cancellationToken);

    public static Task<Guid> ContinueWithAsync<TParent, TNext>(
        this IJobSubmitter submitter,
        TParent parentJob,
        TNext continuationJob,
        CancellationToken cancellationToken = default)
        where TParent : notnull
        where TNext : notnull
    {
        if (submitter is not JobSubmitter concrete)
            throw new InvalidOperationException("ContinueWith requires the default JobSubmitter registration.");

        return concrete.ContinueWithAsync(parentJob, continuationJob, cancellationToken);
    }
}
