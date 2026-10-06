using System.Data.Common;

namespace DotnetJobKit.Abstractions;

public interface IEnlistedJobStore
{
    Task<Guid> SubmitAsync(
        JobSubmitRequest request,
        DateTimeOffset now,
        DbConnection connection,
        DbTransaction? transaction,
        CancellationToken cancellationToken);
}
