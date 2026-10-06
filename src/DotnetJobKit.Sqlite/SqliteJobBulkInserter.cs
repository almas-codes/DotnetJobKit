using DotnetJobKit.Abstractions;
using Microsoft.Data.Sqlite;

namespace DotnetJobKit.Sqlite;

public static class SqliteJobBulkInserter
{
    public static async Task InsertReadyJobsAsync(
        SqliteConnection connection,
        int count,
        string queue,
        string contractName,
        int contractVersion,
        string payload,
        DateTimeOffset eligibleAt,
        int maxAttempts,
        int batchSize = 10_000,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
    {
        if (count <= 0)
            return;

        SqliteJobStore.EnsureSchema(connection);
        SqliteJobStore.ApplyPragmas(connection, new SqliteJobStoreOptions
        {
            ConnectionString = connection.ConnectionString,
            EnableWal = true,
            Synchronous = 0,
        });

        var inserted = 0L;
        var now = DateTimeOffset.UtcNow.ToString("O");
        var eligible = eligibleAt.ToString("O");

        await using var insert = connection.CreateCommand();
        insert.CommandText = """
            INSERT INTO Jobs (
                JobId, Queue, ContractName, ContractVersion, Payload, State, EligibleAt,
                AttemptCount, MaxAttempts, CancellationRequested, CreatedAt, CompletedAt, LastError,
                IdempotencyKey, IdempotencyExpiresAt, LeaseToken)
            VALUES ($jobId, $queue, $contractName, $contractVersion, $payload, $state, $eligibleAt,
                0, $maxAttempts, 0, $createdAt, NULL, NULL, NULL, NULL, NULL);
            """;

        var pJobId = insert.Parameters.Add("$jobId", SqliteType.Text);
        var pQueue = insert.Parameters.Add("$queue", SqliteType.Text);
        var pContract = insert.Parameters.Add("$contractName", SqliteType.Text);
        var pVersion = insert.Parameters.Add("$contractVersion", SqliteType.Integer);
        var pPayload = insert.Parameters.Add("$payload", SqliteType.Text);
        var pState = insert.Parameters.Add("$state", SqliteType.Integer);
        var pEligible = insert.Parameters.Add("$eligibleAt", SqliteType.Text);
        var pMax = insert.Parameters.Add("$maxAttempts", SqliteType.Integer);
        var pCreated = insert.Parameters.Add("$createdAt", SqliteType.Text);

        pQueue.Value = queue;
        pContract.Value = contractName;
        pVersion.Value = contractVersion;
        pPayload.Value = payload;
        pState.Value = (int)JobState.Ready;
        pEligible.Value = eligible;
        pMax.Value = maxAttempts;
        pCreated.Value = now;

        while (inserted < count)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var batch = (int)Math.Min(batchSize, count - inserted);

            await using var tx = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            insert.Transaction = (SqliteTransaction)tx;

            for (var i = 0; i < batch; i++)
            {
                pJobId.Value = Guid.NewGuid().ToString();
                await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            inserted += batch;
            progress?.Report(inserted);
        }
    }
}
