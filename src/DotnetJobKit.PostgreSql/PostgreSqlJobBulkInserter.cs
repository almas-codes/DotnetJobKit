using DotnetJobKit.Abstractions;
using Npgsql;

namespace DotnetJobKit.PostgreSql;

public static class PostgreSqlJobBulkInserter
{
    public static async Task InsertReadyJobsAsync(
        NpgsqlConnection connection,
        int count,
        string queue,
        string contractName,
        int contractVersion,
        string payload,
        DateTimeOffset eligibleAt,
        int maxAttempts,
        int batchSize = 5_000,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
    {
        if (count <= 0)
            return;

        PostgreSqlJobStore.EnsureSchema(connection);
        var now = DateTimeOffset.UtcNow;
        var inserted = 0L;

        while (inserted < count)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var batch = Math.Min(batchSize, count - (int)inserted);

            await using var writer = await connection.BeginBinaryImportAsync(
                """
                COPY djk_jobs (
                    job_id, queue, contract_name, contract_version, payload, state, eligible_at,
                    attempt_count, max_attempts, cancellation_requested, created_at, completed_at, last_error,
                    idempotency_key, idempotency_expires_at, lease_token)
                FROM STDIN (FORMAT BINARY)
                """,
                cancellationToken).ConfigureAwait(false);

            for (var i = 0; i < batch; i++)
            {
                await writer.StartRowAsync(cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(Guid.NewGuid(), NpgsqlTypes.NpgsqlDbType.Uuid, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(queue, NpgsqlTypes.NpgsqlDbType.Text, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(contractName, NpgsqlTypes.NpgsqlDbType.Text, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(contractVersion, NpgsqlTypes.NpgsqlDbType.Integer, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(payload, NpgsqlTypes.NpgsqlDbType.Text, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync((short)JobState.Ready, NpgsqlTypes.NpgsqlDbType.Smallint, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(eligibleAt, NpgsqlTypes.NpgsqlDbType.TimestampTz, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(0, NpgsqlTypes.NpgsqlDbType.Integer, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(maxAttempts, NpgsqlTypes.NpgsqlDbType.Integer, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(false, NpgsqlTypes.NpgsqlDbType.Boolean, cancellationToken).ConfigureAwait(false);
                await writer.WriteAsync(now, NpgsqlTypes.NpgsqlDbType.TimestampTz, cancellationToken).ConfigureAwait(false);
                await writer.WriteNullAsync(cancellationToken).ConfigureAwait(false);
                await writer.WriteNullAsync(cancellationToken).ConfigureAwait(false);
                await writer.WriteNullAsync(cancellationToken).ConfigureAwait(false);
                await writer.WriteNullAsync(cancellationToken).ConfigureAwait(false);
                await writer.WriteNullAsync(cancellationToken).ConfigureAwait(false);
            }

            await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
            inserted += batch;
            progress?.Report(inserted);
            if (inserted % 500_000 == 0 || inserted == count)
                Console.WriteLine($"[postgres bulk] {inserted:N0} / {count:N0} rows");
        }
    }

    public static async Task TruncateAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
    {
        PostgreSqlJobStore.EnsureSchema(connection);
        await using var cmd = new NpgsqlCommand($"TRUNCATE TABLE {PostgreSqlSchema.TableName};", connection);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
