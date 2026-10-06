using DotnetJobKit.Abstractions;
using DotnetJobKit.Client;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace DotnetJobKit.EntityFrameworkCore;

public static class EntityFrameworkJobEnqueueExtensions
{
    public static Task<Guid> EnqueueInTransactionAsync<TJob>(
        this IJobSubmitter submitter,
        TJob job,
        DbContext dbContext,
        CancellationToken cancellationToken = default)
        where TJob : notnull
    {
        var connection = dbContext.Database.GetDbConnection();
        var transaction = dbContext.Database.CurrentTransaction?.GetDbTransaction();
        return submitter.EnqueueInTransactionAsync(job, connection, transaction, cancellationToken);
    }
}
