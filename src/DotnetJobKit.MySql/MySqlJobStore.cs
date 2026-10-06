using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using Microsoft.Extensions.Options;
using MySqlConnector;

namespace DotnetJobKit.MySql;

public sealed class MySqlJobStore : IJobStore, IDisposable
{
    private readonly MySqlConnection _connection;
    private readonly MySqlJobStoreOptions _options;

    public MySqlJobStore(IOptions<MySqlJobStoreOptions> options)
    {
        _options = options.Value;
        _connection = new MySqlConnection(_options.ConnectionString);
        _connection.Open();
        EnsureSchema(_connection);
    }

    public static void EnsureSchema(MySqlConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = MySqlSchema.CreateTable;
        cmd.ExecuteNonQuery();
    }

    public async Task<Guid> SubmitAsync(JobSubmitRequest request, DateTimeOffset now, CancellationToken cancellationToken)
    {
        var jobId = Guid.NewGuid();
        await using var cmd = new MySqlCommand(
            """
            INSERT INTO djk_jobs (
                job_id, queue, contract_name, contract_version, payload, state, eligible_at,
                attempt_count, max_attempts, cancellation_requested, created_at)
            VALUES (@job_id, @queue, @contract_name, @contract_version, @payload, @state, @eligible_at,
                0, @max_attempts, 0, @created_at);
            """,
            _connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        cmd.Parameters.AddWithValue("@queue", request.Queue);
        cmd.Parameters.AddWithValue("@contract_name", request.ContractName);
        cmd.Parameters.AddWithValue("@contract_version", request.ContractVersion);
        cmd.Parameters.AddWithValue("@payload", request.Payload);
        cmd.Parameters.AddWithValue("@state", (byte)JobState.Ready);
        cmd.Parameters.AddWithValue("@eligible_at", (request.EligibleAt ?? now).UtcDateTime);
        cmd.Parameters.AddWithValue("@max_attempts", request.MaxAttempts ?? 3);
        cmd.Parameters.AddWithValue("@created_at", now.UtcDateTime);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        return jobId;
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

        var claimed = new List<ClaimedJob>(maxCount);
        var queueList = string.Join(",", queues.Select((_, i) => $"@q{i}"));
        await using var tx = await _connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

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
                _connection,
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
            {
                await using var dead = new MySqlCommand(
                    """
                    UPDATE djk_jobs SET state = 3, eligible_at = NULL, completed_at = @now,
                        last_error = COALESCE(last_error, 'Max attempts exhausted after lease expiration.')
                    WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count;
                    """,
                    _connection,
                    tx);
                dead.Parameters.AddWithValue("@job_id", jobId.ToString());
                dead.Parameters.AddWithValue("@attempt_count", attemptCount);
                dead.Parameters.AddWithValue("@now", now.UtcDateTime);
                await dead.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            var newAttempt = attemptCount + 1;
            var leaseExpiresAt = now + leaseDuration;
            var leaseToken = _options.OwnershipFencing == OwnershipFencingMode.LeaseToken ? Guid.NewGuid() : (Guid?)null;

            await using var update = new MySqlCommand(
                """
                UPDATE djk_jobs
                SET state = 1, attempt_count = @new_attempt, eligible_at = @lease_expires, lease_token = @lease_token
                WHERE job_id = @job_id AND state IN (0, 1) AND attempt_count = @attempt_count AND eligible_at <= @now;
                """,
                _connection,
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
        await using var cmd = new MySqlCommand(
            """
            UPDATE djk_jobs SET eligible_at = @lease_expires
            WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
              AND (@lease_token IS NULL OR lease_token = @lease_token);
            """,
            _connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        cmd.Parameters.AddWithValue("@attempt_count", attemptCount);
        cmd.Parameters.AddWithValue("@lease_expires", leaseExpiresAt.UtcDateTime);
        cmd.Parameters.AddWithValue("@lease_token", leaseToken?.ToString() ?? (object)DBNull.Value);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<bool> IsCancellationRequestedAsync(Guid jobId, int attemptCount, Guid? leaseToken, CancellationToken cancellationToken)
    {
        await using var cmd = new MySqlCommand(
            """
            SELECT cancellation_requested FROM djk_jobs
            WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
              AND (@lease_token IS NULL OR lease_token = @lease_token);
            """,
            _connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        cmd.Parameters.AddWithValue("@attempt_count", attemptCount);
        cmd.Parameters.AddWithValue("@lease_token", leaseToken?.ToString() ?? (object)DBNull.Value);
        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToBoolean(result);
    }

    public async Task<bool> SettleAsync(SettleRequest request, CancellationToken cancellationToken)
    {
        if (request.Outcome == SettleOutcome.Succeeded && request.DeleteOnSuccess)
        {
            await using var delete = new MySqlCommand(
                """
                DELETE FROM djk_jobs WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
                  AND (@lease_token IS NULL OR lease_token = @lease_token);
                """,
                _connection);
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
            _connection);
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
        await using var cmd = new MySqlCommand(
            "UPDATE djk_jobs SET state = 4, eligible_at = NULL, completed_at = @now WHERE job_id = @job_id AND state = 0;",
            _connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        cmd.Parameters.AddWithValue("@now", now.UtcDateTime);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<bool> RequestLeasedCancellationAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await using var cmd = new MySqlCommand(
            "UPDATE djk_jobs SET cancellation_requested = 1 WHERE job_id = @job_id AND state = 1;",
            _connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<JobRecord?> GetAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await using var cmd = new MySqlCommand("SELECT * FROM djk_jobs WHERE job_id = @job_id LIMIT 1;", _connection);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        return new JobRecord
        {
            JobId = Guid.Parse(reader.GetString("job_id")),
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
        };
    }

    public async Task<DateTimeOffset?> GetNextEligibleAtAsync(IReadOnlyList<string> queues, DateTimeOffset now, CancellationToken cancellationToken)
    {
        if (queues.Count == 0)
            return null;

        var queueList = string.Join(",", queues.Select((_, i) => $"@q{i}"));
        await using var cmd = new MySqlCommand(
            $"""
            SELECT MIN(eligible_at) FROM djk_jobs
            WHERE queue IN ({queueList}) AND state IN (0, 1) AND eligible_at IS NOT NULL;
            """,
            _connection);
        for (var i = 0; i < queues.Count; i++)
            cmd.Parameters.AddWithValue($"@q{i}", queues[i]);

        var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value is DateTime dt ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc)) : null;
    }

    public async Task<int> DeleteTerminalBatchAsync(DateTimeOffset now, int batchSize, CancellationToken cancellationToken)
    {
        await using var cmd = new MySqlCommand(
            """
            DELETE FROM djk_jobs WHERE job_id IN (
                SELECT job_id FROM (
                    SELECT job_id FROM djk_jobs WHERE state IN (2, 3, 4) AND completed_at IS NOT NULL LIMIT @batch
                ) x);
            """,
            _connection);
        cmd.Parameters.AddWithValue("@batch", batchSize);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public void Dispose() => _connection.Dispose();
}
