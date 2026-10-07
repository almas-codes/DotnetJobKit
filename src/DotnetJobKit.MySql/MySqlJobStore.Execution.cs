using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using MySqlConnector;

namespace DotnetJobKit.MySql;

public sealed partial class MySqlJobStore
{
    public async Task<ClaimBatchResult> ClaimBatchAsync(
        IReadOnlyList<string> queues,
        int maxCount,
        DateTimeOffset now,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken)
    {
        var jobs = await ClaimAsync(queues, maxCount, now, leaseDuration, cancellationToken).ConfigureAwait(false);
        var next = await GetNextEligibleAtAsync(queues, now, cancellationToken).ConfigureAwait(false);
        return new ClaimBatchResult(jobs, next);
    }

    public async Task<IReadOnlyList<LeaseMaintenanceResult>> MaintainLeasesAsync(
        IReadOnlyList<LeaseMaintenanceRequest> requests,
        CancellationToken cancellationToken)
    {
        if (requests.Count == 0)
            return Array.Empty<LeaseMaintenanceResult>();

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var useLeaseToken = _options.OwnershipFencing == OwnershipFencingMode.LeaseToken;
        await using var tx = await connection.BeginTransactionAsync(
            System.Data.IsolationLevel.ReadCommitted,
            cancellationToken).ConfigureAwait(false);

        var unionParts = new List<string>(requests.Count);
        await using var update = new MySqlCommand { Connection = connection, Transaction = tx };
        for (var i = 0; i < requests.Count; i++)
        {
            unionParts.Add($"SELECT @jid{i} AS job_id, @att{i} AS attempt_count, @exp{i} AS new_expiry, @tok{i} AS lease_token");
            update.Parameters.AddWithValue($"@jid{i}", requests[i].JobId.ToString());
            update.Parameters.AddWithValue($"@att{i}", requests[i].AttemptCount);
            update.Parameters.AddWithValue($"@exp{i}", requests[i].NewLeaseExpiry.UtcDateTime);
            update.Parameters.AddWithValue($"@tok{i}", requests[i].LeaseToken?.ToString() ?? (object)DBNull.Value);
        }

        update.Parameters.AddWithValue("@use_token", useLeaseToken ? 1 : 0);
        update.CommandText = $"""
            UPDATE djk_jobs j
            INNER JOIN (
                {string.Join(" UNION ALL ", unionParts)}
            ) AS inp ON j.job_id = inp.job_id
            SET j.eligible_at = inp.new_expiry
            WHERE j.state = 1
              AND j.attempt_count = inp.attempt_count
              AND (@use_token = 0 OR j.lease_token <=> inp.lease_token);
            """;
        await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        await using var select = new MySqlCommand { Connection = connection, Transaction = tx };
        for (var i = 0; i < requests.Count; i++)
        {
            select.Parameters.AddWithValue($"@jid{i}", requests[i].JobId.ToString());
            select.Parameters.AddWithValue($"@att{i}", requests[i].AttemptCount);
            select.Parameters.AddWithValue($"@exp{i}", requests[i].NewLeaseExpiry.UtcDateTime);
        }

        select.CommandText = $"""
            SELECT inp.job_id, inp.attempt_count,
                   (j.job_id IS NOT NULL AND j.state = 1 AND j.attempt_count = inp.attempt_count AND j.eligible_at = inp.new_expiry) AS still_owner,
                   COALESCE(j.cancellation_requested, 0) AS cancellation_requested,
                   j.eligible_at
            FROM (
                {string.Join(" UNION ALL ", unionParts.Select((_, i) => $"SELECT @jid{i} AS job_id, @att{i} AS attempt_count, @exp{i} AS new_expiry"))}
            ) AS inp
            LEFT JOIN djk_jobs j ON j.job_id = inp.job_id;
            """;

        var results = new List<LeaseMaintenanceResult>(requests.Count);
        await using var reader = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var jobId = Guid.Parse(reader.GetString(0));
            var attempt = reader.GetInt32(1);
            var stillOwner = reader.GetBoolean(2);
            var cancel = reader.GetBoolean(3);
            DateTimeOffset? expiry = reader.IsDBNull(4)
                ? null
                : new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc));
            results.Add(new LeaseMaintenanceResult(
                jobId,
                attempt,
                stillOwner,
                cancel,
                stillOwner ? expiry ?? requests.First(r => r.JobId == jobId).NewLeaseExpiry : null));
        }

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
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

        var settled = await SettleOutcomeAsync(connection, request, now, tx, cancellationToken).ConfigureAwait(false);
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
        var queueList = string.Join(",", queues.Select((_, i) => $"@q{i}"));
        await using var cmd = new MySqlCommand(
            $"""
            UPDATE djk_jobs SET state = 3, eligible_at = NULL, completed_at = @now,
                last_error = COALESCE(last_error, 'Max attempts exhausted after lease expiration.')
            WHERE job_id IN (
                SELECT job_id FROM (
                    SELECT job_id FROM djk_jobs
                    WHERE queue IN ({queueList}) AND state = 1 AND attempt_count >= max_attempts
                      AND eligible_at IS NOT NULL AND eligible_at <= @now
                    ORDER BY eligible_at, job_id
                    LIMIT @batch) AS x);
            """,
            connection);
        for (var i = 0; i < queues.Count; i++)
            cmd.Parameters.AddWithValue($"@q{i}", queues[i]);
        cmd.Parameters.AddWithValue("@now", now.UtcDateTime);
        cmd.Parameters.AddWithValue("@batch", batchSize);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static string OutcomeKey(Guid jobId, int attemptCount) => $"djk:outcome:{jobId:N}:{attemptCount}";

    private static async Task<Guid?> TryGetJobIdByIdempotencyKeyAsync(
        MySqlConnection connection,
        string key,
        MySqlTransaction? tx,
        CancellationToken cancellationToken)
    {
        await using var cmd = new MySqlCommand("SELECT job_id FROM djk_jobs WHERE idempotency_key = @key LIMIT 1;", connection, tx);
        cmd.Parameters.AddWithValue("@key", key);
        var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value is null or DBNull ? null : value is Guid g ? g : Guid.Parse(value.ToString()!);
    }

    private static async Task InsertSuccessorAsync(
        MySqlConnection connection,
        ContinuationSpec cont,
        Guid jobId,
        string outcomeKey,
        DateTimeOffset now,
        MySqlTransaction tx,
        CancellationToken cancellationToken)
    {
        await using var cmd = new MySqlCommand(
            """
            INSERT INTO djk_jobs (
                job_id, queue, contract_name, contract_version, payload, state, eligible_at,
                attempt_count, max_attempts, cancellation_requested, created_at, idempotency_key)
            VALUES (
                @job_id, @queue, @contract_name, @contract_version, @payload, 0, @eligible_at,
                0, 3, 0, @created_at, @idempotency_key);
            """,
            connection,
            tx);
        cmd.Parameters.AddWithValue("@job_id", jobId.ToString());
        cmd.Parameters.AddWithValue("@queue", cont.Queue);
        cmd.Parameters.AddWithValue("@contract_name", cont.ContractName);
        cmd.Parameters.AddWithValue("@contract_version", cont.ContractVersion);
        cmd.Parameters.AddWithValue("@payload", cont.Payload);
        cmd.Parameters.AddWithValue("@eligible_at", now.UtcDateTime);
        cmd.Parameters.AddWithValue("@created_at", now.UtcDateTime);
        cmd.Parameters.AddWithValue("@idempotency_key", outcomeKey);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<bool> SettleOutcomeAsync(
        MySqlConnection connection,
        CommitOutcomeRequest request,
        DateTimeOffset now,
        MySqlTransaction tx,
        CancellationToken cancellationToken)
    {
        if (request.Outcome.Kind == ExecutionOutcomeKind.Recurring)
        {
            await using var cmd = new MySqlCommand(
                """
                UPDATE djk_jobs SET state = 0, attempt_count = 0, eligible_at = @eligible_at,
                    completed_at = NULL, last_error = NULL, lease_token = NULL, cancellation_requested = 0
                WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
                  AND (@lease_token IS NULL OR lease_token = @lease_token);
                """,
                connection,
                tx);
            cmd.Parameters.AddWithValue("@eligible_at", (request.Outcome.NextDueAt ?? now).UtcDateTime);
            cmd.Parameters.AddWithValue("@job_id", request.JobId.ToString());
            cmd.Parameters.AddWithValue("@attempt_count", request.AttemptCount);
            cmd.Parameters.AddWithValue("@lease_token", (object?)request.LeaseToken?.ToString() ?? DBNull.Value);
            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
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
                DeleteOnSuccess = request.Outcome.DeleteOnSuccess,
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
        MySqlConnection connection,
        SettleRequest request,
        MySqlTransaction tx,
        CancellationToken cancellationToken)
    {
        if (request.Outcome == SettleOutcome.Succeeded && request.DeleteOnSuccess)
        {
            await using var delete = new MySqlCommand(
                """
                DELETE FROM djk_jobs WHERE job_id = @job_id AND state = 1 AND attempt_count = @attempt_count
                  AND (@lease_token IS NULL OR lease_token = @lease_token);
                """,
                connection,
                tx);
            delete.Parameters.AddWithValue("@job_id", request.JobId.ToString());
            delete.Parameters.AddWithValue("@attempt_count", request.AttemptCount);
            delete.Parameters.AddWithValue("@lease_token", (object?)request.LeaseToken?.ToString() ?? DBNull.Value);
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
            connection,
            tx);
        cmd.Parameters.AddWithValue("@state", state);
        cmd.Parameters.AddWithValue("@eligible_at", (object?)eligibleAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@completed_at", (object?)completedAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@last_error", (object?)lastError ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@job_id", request.JobId.ToString());
        cmd.Parameters.AddWithValue("@attempt_count", request.AttemptCount);
        cmd.Parameters.AddWithValue("@lease_token", (object?)request.LeaseToken?.ToString() ?? DBNull.Value);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }
}
