using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.Diagnostics;
using Npgsql;
using NpgsqlTypes;

namespace DotnetJobKit.PostgreSql;

public sealed partial class PostgreSqlJobStore
{
    public async Task<ClaimBatchResult> ClaimBatchAsync(
        IReadOnlyList<string> queues,
        int maxCount,
        DateTimeOffset now,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var jobs = await ClaimOnConnectionAsync(connection, queues, maxCount, now, leaseDuration, cancellationToken)
            .ConfigureAwait(false);
        var next = await ReadNextEligibleAtOnConnectionAsync(connection, queues, cancellationToken).ConfigureAwait(false);
        return new ClaimBatchResult(jobs, next);
    }

    internal async Task<IReadOnlyList<ClaimedJob>> ClaimOnConnectionAsync(
        NpgsqlConnection connection,
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
        await using var tx = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

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
                      j.attempt_count, picked.max_attempts, picked.cancellation_requested, j.lease_token,
                      j.recurrence_cron, j.continuation_queue, j.continuation_contract_name,
                      j.continuation_contract_version, j.continuation_payload;
            """,
            connection,
            tx);
        claim.Parameters.AddWithValue("queues", queues.ToArray());
        claim.Parameters.AddWithValue("now", now);
        claim.Parameters.AddWithValue("max_count", maxCount);
        claim.Parameters.AddWithValue("lease_expires", leaseExpiresAt);
        claim.Parameters.AddWithValue("use_token", useLeaseToken);
        JobStoreDiagnostics.RecordCommand();

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
                    RecurrenceCron = reader.IsDBNull(9) ? null : reader.GetString(9),
                    ContinuationQueue = reader.IsDBNull(10) ? null : reader.GetString(10),
                    ContinuationContractName = reader.IsDBNull(11) ? null : reader.GetString(11),
                    ContinuationContractVersion = reader.IsDBNull(12) ? null : reader.GetInt32(12),
                    ContinuationPayload = reader.IsDBNull(13) ? null : reader.GetString(13),
                });
            }
        }

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        return claimed;
    }

    private async Task<DateTimeOffset?> ReadNextEligibleAtOnConnectionAsync(
        NpgsqlConnection connection,
        IReadOnlyList<string> queues,
        CancellationToken cancellationToken)
    {
        if (queues.Count == 0)
            return null;

        await using var cmd = new NpgsqlCommand(
            """
            SELECT MIN(eligible_at) FROM djk_jobs
            WHERE queue = ANY(@queues) AND state IN (0, 1) AND eligible_at IS NOT NULL;
            """,
            connection);
        cmd.Parameters.AddWithValue("queues", queues.ToArray());
        JobStoreDiagnostics.RecordCommand();
        var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value is DateTimeOffset dto ? dto : null;
    }

    public async Task<IReadOnlyList<LeaseMaintenanceResult>> MaintainLeasesAsync(
        IReadOnlyList<LeaseMaintenanceRequest> requests,
        CancellationToken cancellationToken)
    {
        if (requests.Count == 0)
            return Array.Empty<LeaseMaintenanceResult>();

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var useLeaseToken = _options.OwnershipFencing == OwnershipFencingMode.LeaseToken;

        var jobIds = new Guid[requests.Count];
        var attemptCounts = new int[requests.Count];
        var newExpiries = new DateTimeOffset[requests.Count];
        var leaseTokens = new Guid?[requests.Count];
        for (var i = 0; i < requests.Count; i++)
        {
            jobIds[i] = requests[i].JobId;
            attemptCounts[i] = requests[i].AttemptCount;
            newExpiries[i] = requests[i].NewLeaseExpiry;
            leaseTokens[i] = requests[i].LeaseToken;
        }

        await using var cmd = new NpgsqlCommand(
            """
            WITH input AS (
                SELECT *
                FROM unnest(@job_ids, @attempt_counts, @new_expiries, @lease_tokens)
                    AS t(job_id, attempt_count, new_expiry, lease_token)
            ),
            updated AS (
                UPDATE djk_jobs AS j
                SET eligible_at = input.new_expiry
                FROM input
                WHERE j.job_id = input.job_id
                  AND j.state = 1
                  AND j.attempt_count = input.attempt_count
                  AND (
                      NOT @use_token
                      OR (j.lease_token IS NOT DISTINCT FROM input.lease_token)
                  )
                RETURNING j.job_id, j.attempt_count, j.cancellation_requested, j.eligible_at
            )
            SELECT u.job_id, u.attempt_count, TRUE AS still_owner, u.cancellation_requested, u.eligible_at
            FROM updated u
            UNION ALL
            SELECT i.job_id, i.attempt_count, FALSE AS still_owner,
                   COALESCE(j.cancellation_requested, FALSE), NULL::timestamptz
            FROM input i
            LEFT JOIN updated u ON u.job_id = i.job_id AND u.attempt_count = i.attempt_count
            LEFT JOIN djk_jobs j ON j.job_id = i.job_id AND j.state = 1 AND j.attempt_count = i.attempt_count
            WHERE u.job_id IS NULL;
            """,
            connection);
        cmd.Parameters.Add(new NpgsqlParameter("job_ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid) { Value = jobIds });
        cmd.Parameters.Add(new NpgsqlParameter("attempt_counts", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = attemptCounts });
        cmd.Parameters.Add(new NpgsqlParameter("new_expiries", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz) { Value = newExpiries });
        cmd.Parameters.Add(new NpgsqlParameter("lease_tokens", NpgsqlDbType.Array | NpgsqlDbType.Uuid) { Value = leaseTokens });
        cmd.Parameters.AddWithValue("use_token", useLeaseToken);

        var results = new List<LeaseMaintenanceResult>(requests.Count);
        JobStoreDiagnostics.RecordCommand();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            results.Add(new LeaseMaintenanceResult(
                reader.GetGuid(0),
                reader.GetInt32(1),
                reader.GetBoolean(2),
                reader.GetBoolean(3),
                reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTimeOffset>(4)));
        }

        return results;
    }

    public async Task<CommitOutcomeResult> CommitOutcomeAsync(
        CommitOutcomeRequest request,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var outcomeKey = OutcomeKey(request.JobId, request.AttemptCount);

        await using var tx = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var existing = await TryGetJobIdByIdempotencyKeyAsync(connection, outcomeKey, tx, cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new CommitOutcomeResult(CommitOutcomeStatus.AlreadyCommitted, existing);
        }

        Guid? successorId = null;
        if (request.Outcome.Continuation is { } cont)
        {
            successorId = Guid.NewGuid();
            await InsertSuccessorAsync(connection, cont, successorId.Value, outcomeKey, now, tx, cancellationToken)
                .ConfigureAwait(false);
        }

        var settled = await SettleOutcomeInTransactionAsync(connection, request, now, tx, cancellationToken).ConfigureAwait(false);
        if (!settled)
        {
            await tx.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new CommitOutcomeResult(CommitOutcomeStatus.StaleOwner, null);
        }

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new CommitOutcomeResult(CommitOutcomeStatus.Committed, successorId);
    }

    public async Task<int> RecoverExhaustedLeasesBatchAsync(
        IReadOnlyList<string> queues,
        int batchSize,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            """
            UPDATE djk_jobs SET state = 3, eligible_at = NULL, completed_at = @now,
                last_error = COALESCE(last_error, 'Max attempts exhausted after lease expiration.')
            WHERE job_id IN (
                SELECT job_id FROM djk_jobs
                WHERE queue = ANY(@queues) AND state = 1 AND attempt_count >= max_attempts
                  AND eligible_at IS NOT NULL AND eligible_at <= @now
                ORDER BY eligible_at, job_id
                LIMIT @batch);
            """,
            connection);
        cmd.Parameters.AddWithValue("queues", queues.ToArray());
        cmd.Parameters.AddWithValue("now", now);
        cmd.Parameters.AddWithValue("batch", batchSize);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static string OutcomeKey(Guid jobId, int attemptCount) => $"djk:outcome:{jobId:N}:{attemptCount}";

    private static async Task<Guid?> TryGetJobIdByIdempotencyKeyAsync(
        NpgsqlConnection connection,
        string key,
        NpgsqlTransaction? tx,
        CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT job_id FROM djk_jobs WHERE idempotency_key = @key LIMIT 1;",
            connection,
            tx);
        cmd.Parameters.AddWithValue("key", key);
        var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value is Guid id ? id : null;
    }

    private static async Task InsertSuccessorAsync(
        NpgsqlConnection connection,
        ContinuationSpec cont,
        Guid jobId,
        string outcomeKey,
        DateTimeOffset now,
        NpgsqlTransaction tx,
        CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand(
            """
            INSERT INTO djk_jobs (
                job_id, queue, contract_name, contract_version, payload, state, eligible_at,
                attempt_count, max_attempts, cancellation_requested, created_at, idempotency_key)
            VALUES (
                @job_id, @queue, @contract_name, @contract_version, @payload, 0, @eligible_at,
                0, 3, false, @created_at, @idempotency_key);
            """,
            connection,
            tx);
        cmd.Parameters.AddWithValue("job_id", jobId);
        cmd.Parameters.AddWithValue("queue", cont.Queue);
        cmd.Parameters.AddWithValue("contract_name", cont.ContractName);
        cmd.Parameters.AddWithValue("contract_version", cont.ContractVersion);
        cmd.Parameters.AddWithValue("payload", cont.Payload);
        cmd.Parameters.AddWithValue("eligible_at", now);
        cmd.Parameters.AddWithValue("created_at", now);
        cmd.Parameters.AddWithValue("idempotency_key", outcomeKey);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<bool> SettleOutcomeInTransactionAsync(
        NpgsqlConnection connection,
        CommitOutcomeRequest request,
        DateTimeOffset now,
        NpgsqlTransaction tx,
        CancellationToken cancellationToken)
    {
        if (request.Outcome.Kind == ExecutionOutcomeKind.Recurring)
        {
            await using var cmd = new NpgsqlCommand(
                """
                UPDATE djk_jobs SET state = 0, attempt_count = 0, eligible_at = @eligible_at,
                    completed_at = NULL, last_error = NULL, lease_token = NULL, cancellation_requested = false
                WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
                  AND (@lease_token IS NULL OR lease_token = @lease_token);
                """,
                connection,
                tx);
            cmd.Parameters.AddWithValue("eligible_at", request.Outcome.NextDueAt ?? now);
            cmd.Parameters.AddWithValue("job_id", request.JobId);
            cmd.Parameters.AddWithValue("attempt_count", request.AttemptCount);
            cmd.Parameters.AddWithValue("lease_token", (object?)request.LeaseToken ?? DBNull.Value);
            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
        }

        if (request.Outcome.Kind == ExecutionOutcomeKind.Succeeded && request.Outcome.DeleteOnSuccess)
        {
            await using var delete = new NpgsqlCommand(
                """
                DELETE FROM djk_jobs WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
                  AND (@lease_token IS NULL OR lease_token = @lease_token);
                """,
                connection,
                tx);
            delete.Parameters.AddWithValue("job_id", request.JobId);
            delete.Parameters.AddWithValue("attempt_count", request.AttemptCount);
            delete.Parameters.AddWithValue("lease_token", (object?)request.LeaseToken ?? DBNull.Value);
            return await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
        }

        var settle = request.Outcome.Kind switch
        {
            ExecutionOutcomeKind.Succeeded => new SettleRequest
            {
                JobId = request.JobId,
                AttemptCount = request.AttemptCount,
                LeaseToken = request.LeaseToken,
                Outcome = SettleOutcome.Succeeded,
                Now = now,
                DeleteOnSuccess = false,
            },
            ExecutionOutcomeKind.Retry => new SettleRequest
            {
                JobId = request.JobId,
                AttemptCount = request.AttemptCount,
                LeaseToken = request.LeaseToken,
                Outcome = SettleOutcome.Retry,
                Now = now,
                NextEligibleAt = request.Outcome.NextDueAt ?? now,
                LastError = request.Outcome.Error,
                DeleteOnSuccess = false,
            },
            ExecutionOutcomeKind.Dead => new SettleRequest
            {
                JobId = request.JobId,
                AttemptCount = request.AttemptCount,
                LeaseToken = request.LeaseToken,
                Outcome = SettleOutcome.Dead,
                Now = now,
                LastError = request.Outcome.Error,
                DeleteOnSuccess = false,
            },
            ExecutionOutcomeKind.Cancelled => new SettleRequest
            {
                JobId = request.JobId,
                AttemptCount = request.AttemptCount,
                LeaseToken = request.LeaseToken,
                Outcome = SettleOutcome.Cancelled,
                Now = now,
                LastError = request.Outcome.Error,
                DeleteOnSuccess = false,
            },
            _ => throw new InvalidOperationException($"Unsupported outcome {request.Outcome.Kind}"),
        };

        return await SettleInTransactionAsync(connection, settle, tx, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<bool> SettleInTransactionAsync(
        NpgsqlConnection connection,
        SettleRequest request,
        NpgsqlTransaction tx,
        CancellationToken cancellationToken)
    {
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
            connection,
            tx);
        cmd.Parameters.AddWithValue("state", state);
        cmd.Parameters.AddWithValue("eligible_at", (object?)eligibleAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("completed_at", (object?)completedAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("last_error", (object?)lastError ?? DBNull.Value);
        cmd.Parameters.AddWithValue("job_id", request.JobId);
        cmd.Parameters.AddWithValue("attempt_count", request.AttemptCount);
        cmd.Parameters.AddWithValue("lease_token", (object?)request.LeaseToken ?? DBNull.Value);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }
}
