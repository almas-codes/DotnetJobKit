using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.Diagnostics;
using Microsoft.Extensions.Options;
using MySqlConnector;

namespace DotnetJobKit.MySql;

public sealed partial class MySqlJobStore : IJobStore, IDisposable
{
    private readonly MySqlJobStoreOptions _options;

    public MySqlJobStore(IOptions<MySqlJobStoreOptions> options)
    {
        _options = options.Value;
        var builder = new MySqlConnectionStringBuilder(_options.ConnectionString) { Pooling = true };
        _dataSource = new MySqlDataSource(builder.ConnectionString);
        using var bootstrap = _dataSource.OpenConnection();
        EnsureSchema(bootstrap);
    }

    public static void EnsureSchema(MySqlConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = MySqlSchema.CreateTable;
        cmd.ExecuteNonQuery();
        EnsureOptionalColumn(connection, "recurrence_cron", "VARCHAR(128) NULL");
        EnsureOptionalColumn(connection, "continuation_queue", "VARCHAR(128) NULL");
        EnsureOptionalColumn(connection, "continuation_contract_name", "VARCHAR(256) NULL");
        EnsureOptionalColumn(connection, "continuation_contract_version", "INT NULL");
        EnsureOptionalColumn(connection, "continuation_payload", "LONGTEXT NULL");

        cmd.CommandText = """
            CREATE UNIQUE INDEX ux_djk_active_idempotency ON djk_jobs (idempotency_key, state);
            """;
        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (MySqlException)
        {
            // Index may already exist.
        }
    }

    private static void EnsureOptionalColumn(MySqlConnection connection, string name, string definition)
    {
        using var check = connection.CreateCommand();
        check.CommandText = """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = 'djk_jobs' AND column_name = @name;
            """;
        check.Parameters.AddWithValue("@name", name);
        if (Convert.ToInt64(check.ExecuteScalar()) > 0)
            return;

        using var alter = connection.CreateCommand();
        alter.CommandText = $"ALTER TABLE djk_jobs ADD COLUMN {name} {definition};";
        alter.ExecuteNonQuery();
    }

    public async Task<Guid> SubmitAsync(JobSubmitRequest request, DateTimeOffset now, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(request.IdempotencyKey))
            return await InsertJobAsync(connection, null, request, now, Guid.NewGuid(), cancellationToken).ConfigureAwait(false);

        await using var tx = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await ClearExpiredIdempotencyKeyAsync(connection, tx, request.IdempotencyKey, now, cancellationToken).ConfigureAwait(false);

        var jobId = Guid.NewGuid();
        try
        {
            return await InsertJobAsync(connection, tx, request, now, jobId, cancellationToken).ConfigureAwait(false);
        }
        catch (MySqlException ex) when (ex.Number == 1062)
        {
            var existing = await ReadActiveIdempotentJobIdInTransactionAsync(
                connection,
                tx,
                request.IdempotencyKey,
                now,
                cancellationToken).ConfigureAwait(false);
            if (existing is null)
                throw;
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return existing.Value;
        }
    }

    private static async Task ClearExpiredIdempotencyKeyAsync(
        MySqlConnection connection,
        MySqlTransaction tx,
        string key,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await using var cmd = new MySqlCommand(
            """
            UPDATE djk_jobs SET idempotency_key = NULL, idempotency_expires_at = NULL
            WHERE idempotency_key = @key AND state IN (0, 1)
              AND idempotency_expires_at IS NOT NULL AND idempotency_expires_at <= @now;
            """,
            connection,
            tx);
        cmd.Parameters.AddWithValue("@key", key);
        cmd.Parameters.AddWithValue("@now", now.UtcDateTime);
        JobStoreDiagnostics.RecordCommand();
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<Guid?> ReadActiveIdempotentJobIdInTransactionAsync(
        MySqlConnection connection,
        MySqlTransaction tx,
        string key,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await using var cmd = new MySqlCommand(
            """
            SELECT job_id FROM djk_jobs
            WHERE idempotency_key = @key AND state IN (0, 1)
              AND (idempotency_expires_at IS NULL OR idempotency_expires_at > @now)
            LIMIT 1;
            """,
            connection,
            tx);
        cmd.Parameters.AddWithValue("@key", key);
        cmd.Parameters.AddWithValue("@now", now.UtcDateTime);
        JobStoreDiagnostics.RecordCommand();
        var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value is null or DBNull ? null : value is Guid g ? g : Guid.Parse(value.ToString()!);
    }

    private static async Task<Guid> InsertJobAsync(
        MySqlConnection connection,
        MySqlTransaction? tx,
        JobSubmitRequest request,
        DateTimeOffset now,
        Guid jobId,
        CancellationToken cancellationToken)
    {
        var expiresAt = request.IdempotencyTtl is { } ttl ? now + ttl : (DateTimeOffset?)null;
        await using var cmd = new MySqlCommand(
            """
            INSERT INTO djk_jobs (
                job_id, queue, contract_name, contract_version, payload, state, eligible_at,
                attempt_count, max_attempts, cancellation_requested, created_at,
                idempotency_key, idempotency_expires_at,
                recurrence_cron, continuation_queue, continuation_contract_name, continuation_contract_version, continuation_payload)
            VALUES (@job_id, @queue, @contract_name, @contract_version, @payload, @state, @eligible_at,
                0, @max_attempts, 0, @created_at,
                @idempotency_key, @idempotency_expires_at,
                @recurrence_cron, @continuation_queue, @continuation_contract_name, @continuation_contract_version, @continuation_payload);
            """,
            connection,
            tx);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        cmd.Parameters.AddWithValue("@queue", request.Queue);
        cmd.Parameters.AddWithValue("@contract_name", request.ContractName);
        cmd.Parameters.AddWithValue("@contract_version", request.ContractVersion);
        cmd.Parameters.AddWithValue("@payload", request.Payload);
        cmd.Parameters.AddWithValue("@state", (byte)JobState.Ready);
        cmd.Parameters.AddWithValue("@eligible_at", (request.EligibleAt ?? now).UtcDateTime);
        cmd.Parameters.AddWithValue("@max_attempts", request.MaxAttempts ?? 3);
        cmd.Parameters.AddWithValue("@created_at", now.UtcDateTime);
        cmd.Parameters.AddWithValue("@idempotency_key", (object?)request.IdempotencyKey ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@idempotency_expires_at", expiresAt?.UtcDateTime ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@recurrence_cron", (object?)request.RecurrenceCron ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@continuation_queue", (object?)request.ContinuationQueue ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@continuation_contract_name", (object?)request.ContinuationContractName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@continuation_contract_version", (object?)request.ContinuationContractVersion ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@continuation_payload", (object?)request.ContinuationPayload ?? DBNull.Value);
        JobStoreDiagnostics.RecordCommand();
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (tx is not null)
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        return jobId;
    }

    private static async Task<Guid?> TryGetActiveIdempotentJobIdAsync(
        MySqlConnection connection,
        string key,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await using var cmd = new MySqlCommand(
            """
            SELECT job_id, idempotency_expires_at, state FROM djk_jobs
            WHERE idempotency_key = @key LIMIT 1;
            """,
            connection);
        cmd.Parameters.AddWithValue("@key", key);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        var state = (JobState)reader.GetByte(reader.GetOrdinal("state"));
        if (state is not (JobState.Ready or JobState.Leased))
            return null;

        var expires = reader.IsDBNull(reader.GetOrdinal("idempotency_expires_at"))
            ? (DateTimeOffset?)null
            : new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(reader.GetOrdinal("idempotency_expires_at")), DateTimeKind.Utc));
        if (expires is not null && expires <= now)
            return null;

        var jobIdValue = reader.GetValue(reader.GetOrdinal("job_id"));
        return jobIdValue is Guid g ? g : Guid.Parse(jobIdValue.ToString()!);
    }

    public async Task<IReadOnlyList<ClaimedJob>> ClaimAsync(
        IReadOnlyList<string> queues,
        int maxCount,
        DateTimeOffset now,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken)
    {
        if (maxCount <= 0 || queues.Count == 0)
            return Array.Empty<ClaimedJob>();

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var claimed = new List<ClaimedJob>(maxCount);
        var queueList = string.Join(",", queues.Select((_, i) => $"@q{i}"));
        await using var tx = await connection.BeginTransactionAsync(
            System.Data.IsolationLevel.ReadCommitted,
            cancellationToken).ConfigureAwait(false);

        for (var n = 0; n < maxCount; n++)
        {
            await using var select = new MySqlCommand(
                $"""
                SELECT job_id, queue, contract_name, contract_version, payload, state, attempt_count, max_attempts, cancellation_requested
                FROM djk_jobs
                WHERE queue IN ({queueList})
                  AND state IN (0, 1)
                  AND eligible_at IS NOT NULL
                  AND eligible_at <= @now
                ORDER BY eligible_at, job_id
                LIMIT 1
                FOR UPDATE SKIP LOCKED;
                """,
                connection,
                tx);
            for (var i = 0; i < queues.Count; i++)
                select.Parameters.AddWithValue($"@q{i}", queues[i]);
            select.Parameters.AddWithValue("@now", now.UtcDateTime);

            await using var reader = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                break;

            var jobIdValue = reader.GetValue(0);
            var jobId = jobIdValue is Guid guidValue ? guidValue : Guid.Parse(jobIdValue.ToString()!);
            var queue = reader.GetString(1);
            var contractName = reader.GetString(2);
            var contractVersion = reader.GetInt32(3);
            var payload = reader.GetString(4);
            var state = (JobState)reader.GetByte(5);
            var attemptCount = reader.GetInt32(6);
            var maxAttempts = reader.GetInt32(7);
            var cancellationRequested = reader.GetBoolean(8);
            await reader.CloseAsync().ConfigureAwait(false);

            if (state == JobState.Leased && attemptCount >= maxAttempts)
                continue;

            var newAttempt = attemptCount + 1;
            var leaseExpiresAt = now + leaseDuration;
            var leaseToken = _options.OwnershipFencing == OwnershipFencingMode.LeaseToken ? Guid.NewGuid() : (Guid?)null;

            await using var update = new MySqlCommand(
                """
                UPDATE djk_jobs
                SET state = 1, attempt_count = @new_attempt, eligible_at = @lease_expires, lease_token = @lease_token
                WHERE job_id = @job_id AND state IN (0, 1) AND attempt_count = @attempt_count AND eligible_at <= @now;
                """,
                connection,
                tx);
            update.Parameters.AddWithValue("@job_id", jobId.ToString());
            update.Parameters.AddWithValue("@new_attempt", newAttempt);
            update.Parameters.AddWithValue("@lease_expires", leaseExpiresAt.UtcDateTime);
            update.Parameters.AddWithValue("@lease_token", leaseToken?.ToString() ?? (object)DBNull.Value);
            update.Parameters.AddWithValue("@attempt_count", attemptCount);
            update.Parameters.AddWithValue("@now", now.UtcDateTime);
            if (await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 0)
                continue;

            claimed.Add(new ClaimedJob
            {
                JobId = jobId,
                Queue = queue,
                ContractName = contractName,
                ContractVersion = contractVersion,
                Payload = payload,
                AttemptCount = newAttempt,
                MaxAttempts = maxAttempts,
                CancellationRequested = cancellationRequested,
                LeaseExpiresAt = leaseExpiresAt,
                LeaseToken = leaseToken,
            });
        }

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        return claimed;
    }

    public async Task<bool> RenewAsync(Guid jobId, int attemptCount, Guid? leaseToken, DateTimeOffset leaseExpiresAt, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new MySqlCommand(
            """
            UPDATE djk_jobs SET eligible_at = @lease_expires
            WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
              AND (@lease_token IS NULL OR lease_token = @lease_token);
            """,
            connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        cmd.Parameters.AddWithValue("@attempt_count", attemptCount);
        cmd.Parameters.AddWithValue("@lease_expires", leaseExpiresAt.UtcDateTime);
        cmd.Parameters.AddWithValue("@lease_token", leaseToken?.ToString() ?? (object)DBNull.Value);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<bool> IsCancellationRequestedAsync(Guid jobId, int attemptCount, Guid? leaseToken, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new MySqlCommand(
            """
            SELECT cancellation_requested FROM djk_jobs
            WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
              AND (@lease_token IS NULL OR lease_token = @lease_token);
            """,
            connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        cmd.Parameters.AddWithValue("@attempt_count", attemptCount);
        cmd.Parameters.AddWithValue("@lease_token", leaseToken?.ToString() ?? (object)DBNull.Value);
        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToBoolean(result);
    }

    public async Task<bool> SettleAsync(SettleRequest request, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        if (request.Outcome == SettleOutcome.Succeeded && request.DeleteOnSuccess)
        {
            await using var delete = new MySqlCommand(
                """
                DELETE FROM djk_jobs WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
                  AND (@lease_token IS NULL OR lease_token = @lease_token);
                """,
                connection);
            delete.Parameters.AddWithValue("@job_id", request.JobId.ToString());
            delete.Parameters.AddWithValue("@attempt_count", request.AttemptCount);
            delete.Parameters.AddWithValue("@lease_token", request.LeaseToken?.ToString() ?? (object)DBNull.Value);
            return await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
        }

        byte state;
        DateTime? eligibleAt;
        DateTime? completedAt;
        string? lastError;
        switch (request.Outcome)
        {
            case SettleOutcome.Succeeded:
                state = (byte)JobState.Succeeded;
                eligibleAt = null;
                completedAt = request.Now.UtcDateTime;
                lastError = null;
                break;
            case SettleOutcome.Retry:
                state = (byte)JobState.Ready;
                eligibleAt = (request.NextEligibleAt ?? request.Now).UtcDateTime;
                completedAt = null;
                lastError = request.LastError;
                break;
            case SettleOutcome.Dead:
                state = (byte)JobState.Dead;
                eligibleAt = null;
                completedAt = request.Now.UtcDateTime;
                lastError = request.LastError;
                break;
            case SettleOutcome.Cancelled:
                state = (byte)JobState.Cancelled;
                eligibleAt = null;
                completedAt = request.Now.UtcDateTime;
                lastError = request.LastError;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(request.Outcome));
        }

        await using var cmd = new MySqlCommand(
            """
            UPDATE djk_jobs SET state = @state, eligible_at = @eligible_at, completed_at = @completed_at, last_error = @last_error
            WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
              AND (@lease_token IS NULL OR lease_token = @lease_token);
            """,
            connection);
        cmd.Parameters.AddWithValue("@state", state);
        cmd.Parameters.AddWithValue("@eligible_at", (object?)eligibleAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@completed_at", (object?)completedAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@last_error", (object?)lastError ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@job_id", request.JobId.ToString());
        cmd.Parameters.AddWithValue("@attempt_count", request.AttemptCount);
        cmd.Parameters.AddWithValue("@lease_token", request.LeaseToken?.ToString() ?? (object)DBNull.Value);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<bool> CancelReadyAsync(Guid jobId, DateTimeOffset now, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new MySqlCommand(
            "UPDATE djk_jobs SET state = 4, eligible_at = NULL, completed_at = @now WHERE job_id = @job_id AND state = 0;",
            connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        cmd.Parameters.AddWithValue("@now", now.UtcDateTime);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<bool> RequestLeasedCancellationAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new MySqlCommand(
            "UPDATE djk_jobs SET cancellation_requested = 1 WHERE job_id = @job_id AND state = 1;",
            connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<JobRecord?> GetAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new MySqlCommand("SELECT * FROM djk_jobs WHERE job_id = @job_id LIMIT 1;", connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        return new JobRecord
        {
            JobId = ReadJobId(reader, "job_id"),
            Queue = reader.GetString("queue"),
            ContractName = reader.GetString("contract_name"),
            ContractVersion = reader.GetInt32("contract_version"),
            Payload = reader.GetString("payload"),
            State = (JobState)reader.GetByte("state"),
            EligibleAt = reader.IsDBNull(reader.GetOrdinal("eligible_at")) ? null : reader.GetDateTime("eligible_at"),
            AttemptCount = reader.GetInt32("attempt_count"),
            MaxAttempts = reader.GetInt32("max_attempts"),
            CancellationRequested = reader.GetBoolean("cancellation_requested"),
            CreatedAt = reader.GetDateTime("created_at"),
            CompletedAt = reader.IsDBNull(reader.GetOrdinal("completed_at")) ? null : reader.GetDateTime("completed_at"),
            LastError = reader.IsDBNull(reader.GetOrdinal("last_error")) ? null : reader.GetString("last_error"),
            IdempotencyKey = reader.IsDBNull(reader.GetOrdinal("idempotency_key")) ? null : reader.GetString("idempotency_key"),
            IdempotencyExpiresAt = reader.IsDBNull(reader.GetOrdinal("idempotency_expires_at"))
                ? null
                : new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime("idempotency_expires_at"), DateTimeKind.Utc)),
        };
    }

    public async Task<DateTimeOffset?> GetNextEligibleAtAsync(IReadOnlyList<string> queues, DateTimeOffset now, CancellationToken cancellationToken)
    {
        if (queues.Count == 0)
            return null;

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var queueList = string.Join(",", queues.Select((_, i) => $"@q{i}"));
        await using var cmd = new MySqlCommand(
            $"""
            SELECT MIN(eligible_at) FROM djk_jobs
            WHERE queue IN ({queueList}) AND state IN (0, 1) AND eligible_at IS NOT NULL;
            """,
            connection);
        for (var i = 0; i < queues.Count; i++)
            cmd.Parameters.AddWithValue($"@q{i}", queues[i]);

        var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value is DateTime dt ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc)) : null;
    }

    public async Task<int> DeleteTerminalBatchAsync(
        DateTimeOffset now,
        JobRetentionPurge retention,
        int batchSize,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new MySqlCommand(
            """
            DELETE FROM djk_jobs WHERE job_id IN (
                SELECT job_id FROM (
                    SELECT job_id FROM djk_jobs
                    WHERE completed_at IS NOT NULL AND (
                        (state = 2 AND completed_at <= @success_cutoff) OR
                        (state = 3 AND completed_at <= @failed_cutoff) OR
                        (state = 4 AND completed_at <= @cancel_cutoff))
                    LIMIT @batch
                ) x);
            """,
            connection);
        cmd.Parameters.AddWithValue("@success_cutoff", (now - retention.SucceededRetention).UtcDateTime);
        cmd.Parameters.AddWithValue("@failed_cutoff", (now - retention.FailedRetention).UtcDateTime);
        cmd.Parameters.AddWithValue("@cancel_cutoff", (now - retention.CancelledRetention).UtcDateTime);
        cmd.Parameters.AddWithValue("@batch", batchSize);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static Guid ReadJobId(MySqlDataReader reader, string column)
    {
        var ordinal = reader.GetOrdinal(column);
        var value = reader.GetValue(ordinal);
        return value switch
        {
            Guid g => g,
            string s => Guid.Parse(s),
            _ => Guid.Parse(reader.GetString(ordinal)),
        };
    }

    public void Dispose() => _dataSource.Dispose();
}
