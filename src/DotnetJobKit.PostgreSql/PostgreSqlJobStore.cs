using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotnetJobKit.PostgreSql;

public sealed class PostgreSqlJobStore : IJobStore, IDisposable
{
    private readonly NpgsqlConnection _connection;
    private readonly PostgreSqlJobStoreOptions _options;

    public PostgreSqlJobStore(IOptions<PostgreSqlJobStoreOptions> options)
    {
        _options = options.Value;
        var csb = new NpgsqlConnectionStringBuilder(_options.ConnectionString) { CommandTimeout = 0 };
        _connection = new NpgsqlConnection(csb.ConnectionString);
        _connection.Open();
        EnsureSchema(_connection);
    }

    public NpgsqlConnection Connection => _connection;

    public static void EnsureSchema(NpgsqlConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = PostgreSqlSchema.CreateTable;
        cmd.ExecuteNonQuery();
        cmd.CommandText = PostgreSqlSchema.CreateDispatchIndex;
        cmd.ExecuteNonQuery();
    }

    public async Task<Guid> SubmitAsync(JobSubmitRequest request, DateTimeOffset now, CancellationToken cancellationToken)
    {
        var jobId = Guid.NewGuid();
        await using var cmd = new NpgsqlCommand(
            """
            INSERT INTO djk_jobs (
                job_id, queue, contract_name, contract_version, payload, state, eligible_at,
                attempt_count, max_attempts, cancellation_requested, created_at)
            VALUES (
                @job_id, @queue, @contract_name, @contract_version, @payload, @state, @eligible_at,
                0, @max_attempts, false, @created_at);
            """,
            _connection);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("queue", request.Queue);
        cmd.Parameters.AddWithValue("contract_name", request.ContractName);
        cmd.Parameters.AddWithValue("contract_version", request.ContractVersion);
        cmd.Parameters.AddWithValue("payload", request.Payload);
        cmd.Parameters.AddWithValue("state", (short)JobState.Ready);
        cmd.Parameters.AddWithValue("eligible_at", request.EligibleAt ?? now);
        cmd.Parameters.AddWithValue("max_attempts", request.MaxAttempts ?? 3);
        cmd.Parameters.AddWithValue("created_at", now);
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

        var leaseExpiresAt = now + leaseDuration;
        var useLeaseToken = _options.OwnershipFencing == OwnershipFencingMode.LeaseToken;
        var claimed = new List<ClaimedJob>(maxCount);
        await using var tx = await _connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        await using (var dead = new NpgsqlCommand(
            """
            UPDATE djk_jobs SET state = 3, eligible_at = NULL, completed_at = @now,
                last_error = COALESCE(last_error, 'Max attempts exhausted after lease expiration.')
            WHERE queue = ANY(@queues) AND state = 1 AND attempt_count >= max_attempts
              AND eligible_at IS NOT NULL AND eligible_at <= @now;
            """,
            _connection,
            tx))
        {
            dead.Parameters.AddWithValue("queues", queues.ToArray());
            dead.Parameters.AddWithValue("now", now);
            await dead.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using var claim = new NpgsqlCommand(
            """
            WITH picked AS (
                SELECT job_id, queue, contract_name, contract_version, payload, attempt_count, max_attempts, cancellation_requested
                FROM djk_jobs
                WHERE queue = ANY(@queues)
                  AND state IN (0, 1)
                  AND eligible_at IS NOT NULL
                  AND eligible_at <= @now
                  AND NOT (state = 1 AND attempt_count >= max_attempts)
                ORDER BY eligible_at, job_id
                FOR UPDATE SKIP LOCKED
                LIMIT @max_count
            )
            UPDATE djk_jobs AS j
            SET state = 1,
                attempt_count = picked.attempt_count + 1,
                eligible_at = @lease_expires,
                lease_token = CASE WHEN @use_token THEN gen_random_uuid() ELSE NULL END
            FROM picked
            WHERE j.job_id = picked.job_id
            RETURNING j.job_id, picked.queue, picked.contract_name, picked.contract_version, picked.payload,
                      j.attempt_count, picked.max_attempts, picked.cancellation_requested, j.lease_token;
            """,
            _connection,
            tx);
        claim.Parameters.AddWithValue("queues", queues.ToArray());
        claim.Parameters.AddWithValue("now", now);
        claim.Parameters.AddWithValue("max_count", maxCount);
        claim.Parameters.AddWithValue("lease_expires", leaseExpiresAt);
        claim.Parameters.AddWithValue("use_token", useLeaseToken);

        await using (var reader = await claim.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                claimed.Add(new ClaimedJob
                {
                    JobId = reader.GetGuid(0),
                    Queue = reader.GetString(1),
                    ContractName = reader.GetString(2),
                    ContractVersion = reader.GetInt32(3),
                    Payload = reader.GetString(4),
                    AttemptCount = reader.GetInt32(5),
                    MaxAttempts = reader.GetInt32(6),
                    CancellationRequested = reader.GetBoolean(7),
                    LeaseExpiresAt = leaseExpiresAt,
                    LeaseToken = reader.IsDBNull(8) ? null : reader.GetGuid(8),
                });
            }
        }

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        return claimed;
    }

    public async Task<bool> RenewAsync(Guid jobId, int attemptCount, Guid? leaseToken, DateTimeOffset leaseExpiresAt, CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand(
            """
            UPDATE djk_jobs SET eligible_at = @lease_expires
            WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
              AND (@lease_token IS NULL OR lease_token = @lease_token);
            """,
            _connection);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("attempt_count", attemptCount);
        cmd.Parameters.AddWithValue("lease_expires", leaseExpiresAt);
        cmd.Parameters.AddWithValue("lease_token", (object?)leaseToken ?? DBNull.Value);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<bool> IsCancellationRequestedAsync(Guid jobId, int attemptCount, Guid? leaseToken, CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand(
            """
            SELECT cancellation_requested FROM djk_jobs
            WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
              AND (@lease_token IS NULL OR lease_token = @lease_token);
            """,
            _connection);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("attempt_count", attemptCount);
        cmd.Parameters.AddWithValue("lease_token", (object?)leaseToken ?? DBNull.Value);
        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is true;
    }

    public async Task<bool> SettleAsync(SettleRequest request, CancellationToken cancellationToken)
    {
        if (request.Outcome == SettleOutcome.Succeeded && request.DeleteOnSuccess)
        {
            await using var delete = new NpgsqlCommand(
                """
                DELETE FROM djk_jobs WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
                  AND (@lease_token IS NULL OR lease_token = @lease_token);
                """,
                _connection);
            delete.Parameters.AddWithValue("job_id", request.JobId);
            delete.Parameters.AddWithValue("attempt_count", request.AttemptCount);
            delete.Parameters.AddWithValue("lease_token", (object?)request.LeaseToken ?? DBNull.Value);
            return await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
        }

        short state;
        DateTimeOffset? eligibleAt;
        DateTimeOffset? completedAt;
        string? lastError;
        switch (request.Outcome)
        {
            case SettleOutcome.Succeeded:
                state = (short)JobState.Succeeded;
                eligibleAt = null;
                completedAt = request.Now;
                lastError = null;
                break;
            case SettleOutcome.Retry:
                state = (short)JobState.Ready;
                eligibleAt = request.NextEligibleAt ?? request.Now;
                completedAt = null;
                lastError = request.LastError;
                break;
            case SettleOutcome.Dead:
                state = (short)JobState.Dead;
                eligibleAt = null;
                completedAt = request.Now;
                lastError = request.LastError;
                break;
            case SettleOutcome.Cancelled:
                state = (short)JobState.Cancelled;
                eligibleAt = null;
                completedAt = request.Now;
                lastError = request.LastError;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(request.Outcome));
        }

        await using var cmd = new NpgsqlCommand(
            """
            UPDATE djk_jobs SET state = @state, eligible_at = @eligible_at, completed_at = @completed_at, last_error = @last_error
            WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
              AND (@lease_token IS NULL OR lease_token = @lease_token);
            """,
            _connection);
        cmd.Parameters.AddWithValue("state", state);
        cmd.Parameters.AddWithValue("eligible_at", (object?)eligibleAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("completed_at", (object?)completedAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("last_error", (object?)lastError ?? DBNull.Value);
        cmd.Parameters.AddWithValue("job_id", request.JobId);
        cmd.Parameters.AddWithValue("attempt_count", request.AttemptCount);
        cmd.Parameters.AddWithValue("lease_token", (object?)request.LeaseToken ?? DBNull.Value);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<bool> CancelReadyAsync(Guid jobId, DateTimeOffset now, CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand(
            "UPDATE djk_jobs SET state = 4, eligible_at = NULL, completed_at = @now WHERE job_id = @job_id AND state = 0;",
            _connection);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("now", now);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<bool> RequestLeasedCancellationAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand(
            "UPDATE djk_jobs SET cancellation_requested = true WHERE job_id = @job_id AND state = 1;",
            _connection);
        cmd.Parameters.AddWithValue("job_id", jobId);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<JobRecord?> GetAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand("SELECT * FROM djk_jobs WHERE job_id = @job_id LIMIT 1;", _connection);
        cmd.Parameters.AddWithValue("job_id", jobId);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? ReadRecord(reader) : null;
    }

    public async Task<DateTimeOffset?> GetNextEligibleAtAsync(IReadOnlyList<string> queues, DateTimeOffset now, CancellationToken cancellationToken)
    {
        if (queues.Count == 0)
            return null;

        await using var cmd = new NpgsqlCommand(
            """
            SELECT MIN(eligible_at) FROM djk_jobs
            WHERE queue = ANY(@queues) AND state IN (0, 1) AND eligible_at IS NOT NULL;
            """,
            _connection);
        cmd.Parameters.AddWithValue("queues", queues.ToArray());
        var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value is DateTimeOffset dto ? dto : null;
    }

    public async Task<int> DeleteTerminalBatchAsync(DateTimeOffset now, int batchSize, CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand(
            """
            DELETE FROM djk_jobs WHERE job_id IN (
                SELECT job_id FROM djk_jobs WHERE state IN (2, 3, 4) AND completed_at IS NOT NULL LIMIT @batch);
            """,
            _connection);
        cmd.Parameters.AddWithValue("batch", batchSize);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static JobRecord ReadRecord(NpgsqlDataReader reader) => new()
    {
        JobId = reader.GetGuid(reader.GetOrdinal("job_id")),
        Queue = reader.GetString(reader.GetOrdinal("queue")),
        ContractName = reader.GetString(reader.GetOrdinal("contract_name")),
        ContractVersion = reader.GetInt32(reader.GetOrdinal("contract_version")),
        Payload = reader.GetString(reader.GetOrdinal("payload")),
        State = (JobState)reader.GetInt16(reader.GetOrdinal("state")),
        EligibleAt = reader.IsDBNull(reader.GetOrdinal("eligible_at")) ? null : reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("eligible_at")),
        AttemptCount = reader.GetInt32(reader.GetOrdinal("attempt_count")),
        MaxAttempts = reader.GetInt32(reader.GetOrdinal("max_attempts")),
        CancellationRequested = reader.GetBoolean(reader.GetOrdinal("cancellation_requested")),
        CreatedAt = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("created_at")),
        CompletedAt = reader.IsDBNull(reader.GetOrdinal("completed_at")) ? null : reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("completed_at")),
        LastError = reader.IsDBNull(reader.GetOrdinal("last_error")) ? null : reader.GetString(reader.GetOrdinal("last_error")),
    };

    public void Dispose() => _connection.Dispose();
}
