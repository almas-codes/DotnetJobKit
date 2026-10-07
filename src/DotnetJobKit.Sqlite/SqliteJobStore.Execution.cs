using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.Diagnostics;
using Microsoft.Data.Sqlite;

namespace DotnetJobKit.Sqlite;

public sealed partial class SqliteJobStore
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

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var useLeaseToken = _options.OwnershipFencing == OwnershipFencingMode.LeaseToken;
            await using var tx = await SqliteImmediateTransaction.BeginAsync(_connection, _options.UseBeginImmediate, cancellationToken)
                .ConfigureAwait(false);

            await using (var create = _connection.CreateCommand())
            {
                create.Transaction = tx.Transaction;
                create.CommandText = """
                    CREATE TEMP TABLE IF NOT EXISTS _djk_maint (
                        JobId TEXT NOT NULL,
                        AttemptCount INTEGER NOT NULL,
                        NewExpiry TEXT NOT NULL,
                        LeaseToken TEXT NULL,
                        PRIMARY KEY (JobId, AttemptCount));
                    DELETE FROM _djk_maint;
                    """;
                JobStoreDiagnostics.RecordCommand();
                await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            if (requests.Count > 0)
            {
                var values = string.Join(",", requests.Select((_, i) =>
                    $"($jid{i}, $att{i}, $exp{i}, $tok{i})"));
                await using var bulk = _connection.CreateCommand();
                bulk.Transaction = tx.Transaction;
                bulk.CommandText = $"""
                    INSERT INTO _djk_maint (JobId, AttemptCount, NewExpiry, LeaseToken)
                    VALUES {values};
                    """;
                for (var i = 0; i < requests.Count; i++)
                {
                    bulk.Parameters.AddWithValue($"$jid{i}", requests[i].JobId.ToString());
                    bulk.Parameters.AddWithValue($"$att{i}", requests[i].AttemptCount);
                    bulk.Parameters.AddWithValue($"$exp{i}", requests[i].NewLeaseExpiry.ToString("O"));
                    bulk.Parameters.AddWithValue($"$tok{i}", requests[i].LeaseToken?.ToString() ?? (object)DBNull.Value);
                }

                JobStoreDiagnostics.RecordCommand();
                await bulk.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await using (var update = _connection.CreateCommand())
            {
                update.Transaction = tx.Transaction;
                update.CommandText = """
                    UPDATE Jobs
                    SET EligibleAt = (
                        SELECT m.NewExpiry FROM _djk_maint m
                        WHERE m.JobId = Jobs.JobId AND m.AttemptCount = Jobs.AttemptCount)
                    WHERE JobId IN (SELECT JobId FROM _djk_maint)
                      AND State = 1
                      AND AttemptCount IN (SELECT AttemptCount FROM _djk_maint m WHERE m.JobId = Jobs.JobId)
                      AND (
                          $useToken = 0
                          OR EXISTS (
                              SELECT 1 FROM _djk_maint m
                              WHERE m.JobId = Jobs.JobId AND m.AttemptCount = Jobs.AttemptCount
                                AND ((m.LeaseToken IS NULL AND Jobs.LeaseToken IS NULL) OR m.LeaseToken = Jobs.LeaseToken)));
                    """;
                update.Parameters.AddWithValue("$useToken", useLeaseToken ? 1 : 0);
                JobStoreDiagnostics.RecordCommand();
                await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            var results = new List<LeaseMaintenanceResult>(requests.Count);
            await using var select = _connection.CreateCommand();
            select.Transaction = tx.Transaction;
            select.CommandText = """
                SELECT m.JobId, m.AttemptCount,
                       CASE WHEN j.JobId IS NOT NULL AND j.State = 1 AND j.AttemptCount = m.AttemptCount
                            AND j.EligibleAt = m.NewExpiry THEN 1 ELSE 0 END,
                       COALESCE(j.CancellationRequested, 0),
                       j.EligibleAt
                FROM _djk_maint m
                LEFT JOIN Jobs j ON j.JobId = m.JobId;
                """;
            JobStoreDiagnostics.RecordCommand();
            await using var reader = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var jobId = Guid.Parse(reader.GetString(0));
                var attempt = reader.GetInt32(1);
                var stillOwner = reader.GetInt32(2) == 1;
                var cancel = reader.GetInt32(3) == 1;
                DateTimeOffset? expiry = stillOwner && !reader.IsDBNull(4)
                    ? DateTimeOffset.Parse(reader.GetString(4))
                    : null;
                results.Add(new LeaseMaintenanceResult(jobId, attempt, stillOwner, cancel, expiry));
            }

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return results;
        }
        finally
        {
            _gate.Release();
        }
    }

    public Task<CommitOutcomeResult> CommitOutcomeAsync(
        CommitOutcomeRequest request,
        DateTimeOffset now,
        CancellationToken cancellationToken) =>
        CommitOutcomeUnderGateAsync(request, now, cancellationToken);

    public async Task<int> RecoverExhaustedLeasesBatchAsync(
        IReadOnlyList<string> queues,
        int batchSize,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var placeholders = string.Join(",", queues.Select((_, i) => $"$q{i}"));
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"""
                UPDATE Jobs SET State = 3, EligibleAt = NULL, CompletedAt = $now,
                    LastError = COALESCE(LastError, 'Max attempts exhausted after lease expiration.')
                WHERE JobId IN (
                    SELECT JobId FROM Jobs
                    WHERE Queue IN ({placeholders}) AND State = 1 AND AttemptCount >= MaxAttempts
                      AND EligibleAt IS NOT NULL AND EligibleAt <= $now
                    ORDER BY EligibleAt, JobId
                    LIMIT $batch);
                """;
            for (var i = 0; i < queues.Count; i++)
                cmd.Parameters.AddWithValue($"$q{i}", queues[i]);
            cmd.Parameters.AddWithValue("$now", now.ToString("O"));
            cmd.Parameters.AddWithValue("$batch", batchSize);
            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<CommitOutcomeResult> CommitOutcomeUnderGateAsync(
        CommitOutcomeRequest request,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var outcomeKey = OutcomeKey(request.JobId, request.AttemptCount);
            var existing = await TryGetJobIdByIdempotencyKeyAsync(outcomeKey, cancellationToken).ConfigureAwait(false);
            if (existing is not null)
                return new CommitOutcomeResult(CommitOutcomeStatus.AlreadyCommitted, existing);

            await using var tx = await SqliteImmediateTransaction.BeginAsync(_connection, _options.UseBeginImmediate, cancellationToken)
                .ConfigureAwait(false);
            Guid? successorId = null;
            if (request.Outcome.Continuation is { } cont)
            {
                successorId = Guid.NewGuid();
                await InsertSuccessorAsync(cont, successorId.Value, outcomeKey, now, tx.Transaction, cancellationToken).ConfigureAwait(false);
            }

            var settled = await SettleOutcomeAsync(request, now, tx.Transaction, cancellationToken).ConfigureAwait(false);
            if (!settled)
                return new CommitOutcomeResult(CommitOutcomeStatus.StaleOwner, null);

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new CommitOutcomeResult(CommitOutcomeStatus.Committed, successorId);
        }
        finally
        {
            _gate.Release();
        }
    }

    private static string OutcomeKey(Guid jobId, int attemptCount) => $"djk:outcome:{jobId:N}:{attemptCount}";

    private async Task<Guid?> TryGetJobIdByIdempotencyKeyAsync(string key, CancellationToken cancellationToken)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT JobId FROM Jobs WHERE IdempotencyKey = $key LIMIT 1;";
        cmd.Parameters.AddWithValue("$key", key);
        var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value is string s && Guid.TryParse(s, out var id) ? id : null;
    }

    private async Task InsertSuccessorAsync(
        ContinuationSpec cont,
        Guid jobId,
        string outcomeKey,
        DateTimeOffset now,
        SqliteTransaction? tx,
        CancellationToken cancellationToken)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = """
            INSERT INTO Jobs (
                JobId, Queue, ContractName, ContractVersion, Payload, State, EligibleAt,
                AttemptCount, MaxAttempts, CancellationRequested, CreatedAt, IdempotencyKey)
            VALUES (
                $jobId, $queue, $contractName, $contractVersion, $payload, 0, $eligibleAt,
                0, 3, 0, $createdAt, $idempotencyKey);
            """;
        cmd.Parameters.AddWithValue("$jobId", jobId.ToString());
        cmd.Parameters.AddWithValue("$queue", cont.Queue);
        cmd.Parameters.AddWithValue("$contractName", cont.ContractName);
        cmd.Parameters.AddWithValue("$contractVersion", cont.ContractVersion);
        cmd.Parameters.AddWithValue("$payload", cont.Payload);
        cmd.Parameters.AddWithValue("$eligibleAt", now.ToString("O"));
        cmd.Parameters.AddWithValue("$createdAt", now.ToString("O"));
        cmd.Parameters.AddWithValue("$idempotencyKey", outcomeKey);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> SettleOutcomeAsync(
        CommitOutcomeRequest request,
        DateTimeOffset now,
        SqliteTransaction? tx,
        CancellationToken cancellationToken)
    {
        if (request.Outcome.Kind == ExecutionOutcomeKind.Recurring)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                UPDATE Jobs SET State = 0, AttemptCount = 0, EligibleAt = $eligibleAt,
                    CompletedAt = NULL, LastError = NULL, LeaseToken = NULL, CancellationRequested = 0
                WHERE JobId = $jobId AND State = 1 AND AttemptCount = $attemptCount
                  AND ($leaseToken IS NULL OR LeaseToken = $leaseToken);
                """;
            cmd.Parameters.AddWithValue("$eligibleAt", (request.Outcome.NextDueAt ?? now).ToString("O"));
            cmd.Parameters.AddWithValue("$jobId", request.JobId.ToString());
            cmd.Parameters.AddWithValue("$attemptCount", request.AttemptCount);
            cmd.Parameters.AddWithValue("$leaseToken", request.LeaseToken?.ToString() ?? (object)DBNull.Value);
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

        return await SettleInTransactionAsync(settle, tx, cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> SettleInTransactionAsync(SettleRequest request, SqliteTransaction? tx, CancellationToken cancellationToken)
    {
        if (request.Outcome == SettleOutcome.Succeeded && request.DeleteOnSuccess)
        {
            await using var delete = _connection.CreateCommand();
            delete.Transaction = tx;
            delete.CommandText = """
                DELETE FROM Jobs
                WHERE JobId = $jobId AND State = 1 AND AttemptCount = $attemptCount
                  AND ($leaseToken IS NULL OR LeaseToken = $leaseToken);
                """;
            delete.Parameters.AddWithValue("$jobId", request.JobId.ToString());
            delete.Parameters.AddWithValue("$attemptCount", request.AttemptCount);
            delete.Parameters.AddWithValue("$leaseToken", request.LeaseToken?.ToString() ?? (object)DBNull.Value);
            return await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
        }

        var (state, eligibleAt, completedAt, lastError) = request.Outcome switch
        {
            SettleOutcome.Succeeded => ((int)JobState.Succeeded, (string?)null, request.Now.ToString("O"), (string?)null),
            SettleOutcome.Retry => ((int)JobState.Ready, (request.NextEligibleAt ?? request.Now).ToString("O"), (string?)null, request.LastError),
            SettleOutcome.Dead => ((int)JobState.Dead, (string?)null, request.Now.ToString("O"), request.LastError),
            SettleOutcome.Cancelled => ((int)JobState.Cancelled, (string?)null, request.Now.ToString("O"), request.LastError),
            _ => throw new ArgumentOutOfRangeException(nameof(request), request.Outcome, "Unknown settle outcome."),
        };

        await using var cmd = _connection.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = """
            UPDATE Jobs
            SET State = $state,
                EligibleAt = $eligibleAt,
                CompletedAt = $completedAt,
                LastError = $lastError,
                CancellationRequested = CASE WHEN $state = 0 THEN 0 ELSE CancellationRequested END,
                LeaseToken = CASE WHEN $state = 0 THEN NULL ELSE LeaseToken END
            WHERE JobId = $jobId AND State = 1 AND AttemptCount = $attemptCount
              AND ($leaseToken IS NULL OR LeaseToken = $leaseToken);
            """;
        cmd.Parameters.AddWithValue("$jobId", request.JobId.ToString());
        cmd.Parameters.AddWithValue("$attemptCount", request.AttemptCount);
        cmd.Parameters.AddWithValue("$leaseToken", request.LeaseToken?.ToString() ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("$state", state);
        cmd.Parameters.AddWithValue("$eligibleAt", (object?)eligibleAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("$completedAt", (object?)completedAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("$lastError", (object?)lastError ?? DBNull.Value);
        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }
}
