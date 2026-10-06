using DotnetJobKit.Abstractions;

namespace DotnetJobKit.Client;

public static class TransactionalEnqueueExtensions
{
    public static Task<Guid> EnqueueInTransactionAsync<TJob>(
        this IJobSubmitter submitter,
        TJob job,
        System.Data.Common.DbConnection connection,
        System.Data.Common.DbTransaction? transaction,
        CancellationToken cancellationToken = default)
        where TJob : notnull
    {
        if (submitter is not JobSubmitter concrete)
        {
            throw new InvalidOperationException(
                "Transactional enqueue requires the default JobSubmitter service registration.");
        }

        return concrete.EnqueueInTransactionAsync(job, connection, transaction, cancellationToken);
    }
}
