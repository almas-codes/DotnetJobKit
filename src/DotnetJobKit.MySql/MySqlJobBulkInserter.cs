using DotnetJobKit.Abstractions;
using MySqlConnector;

namespace DotnetJobKit.MySql;

public static class MySqlJobBulkInserter
{
    public static async Task InsertReadyJobsAsync(
        MySqlConnection connection,
        int count,
        string queue,
        string contractName,
        int contractVersion,
        string payload,
        DateTimeOffset eligibleAt,
        int maxAttempts,
        int batchSize = 2_000,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
    {
        if (count <= 0)
            return;

        MySqlJobStore.EnsureSchema(connection);
        var now = DateTime.UtcNow;
        var eligible = eligibleAt.UtcDateTime;
        var inserted = 0L;

        await using var insert = new MySqlCommand(
            """
            INSERT INTO djk_jobs (
                job_id, queue, contract_name, contract_version, payload, state, eligible_at,
                attempt_count, max_attempts, cancellation_requested, created_at)
            VALUES (@job_id, @queue, @contract_name, @contract_version, @payload, @state, @eligible_at,
                0, @max_attempts, 0, @created_at);
            """,
            connection);

        insert.Parameters.Add("@job_id", MySqlDbType.VarChar);
        insert.Parameters.Add("@queue", MySqlDbType.VarChar);
        insert.Parameters.Add("@contract_name", MySqlDbType.VarChar);
        insert.Parameters.Add("@contract_version", MySqlDbType.Int32);
        insert.Parameters.Add("@payload", MySqlDbType.Text);
        insert.Parameters.Add("@state", MySqlDbType.Byte);
        insert.Parameters.Add("@eligible_at", MySqlDbType.DateTime);
        insert.Parameters.Add("@max_attempts", MySqlDbType.Int32);
        insert.Parameters.Add("@created_at", MySqlDbType.DateTime);

        insert.Parameters["@queue"].Value = queue;
        insert.Parameters["@contract_name"].Value = contractName;
        insert.Parameters["@contract_version"].Value = contractVersion;
        insert.Parameters["@payload"].Value = payload;
        insert.Parameters["@state"].Value = (byte)JobState.Ready;
        insert.Parameters["@eligible_at"].Value = eligible;
        insert.Parameters["@max_attempts"].Value = maxAttempts;
        insert.Parameters["@created_at"].Value = now;

        while (inserted < count)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var batch = Math.Min(batchSize, count - (int)inserted);
            await using var tx = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            insert.Transaction = tx;

            for (var i = 0; i < batch; i++)
            {
                insert.Parameters["@job_id"].Value = Guid.NewGuid().ToString();
                await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            inserted += batch;
            progress?.Report(inserted);
        }
    }

    public static async Task TruncateAsync(MySqlConnection connection, CancellationToken cancellationToken = default)
    {
        MySqlJobStore.EnsureSchema(connection);
        await using var cmd = new MySqlCommand($"TRUNCATE TABLE {MySqlSchema.TableName};", connection);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
