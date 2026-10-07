using System.Data.Common;
using DotnetJobKit.Abstractions;
using DotnetJobKit.Configuration;
using DotnetJobKit.Diagnostics;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;

namespace DotnetJobKit.Sqlite;

public sealed partial class SqliteJobStore : IJobStore, IEnlistedJobStore, IAsyncDisposable, IDisposable
{
    private readonly SqliteConnection _connection;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly SqliteJobStoreOptions _options;

    public SqliteJobStore(IOptions<SqliteJobStoreOptions> options)
    {
        _options = options.Value;
        _connection = new SqliteConnection(_options.ConnectionString);
        _connection.Open();
        ApplyPragmas(_connection, _options);
        EnsureSchema(_connection);
    }

    public SqliteConnection Connection => _connection;

    public Task<Guid> SubmitAsync(JobSubmitRequest request, DateTimeOffset now, CancellationToken cancellationToken) =>
        SubmitInternalAsync(request, now, _connection, null, useGate: true, cancellationToken);

    public Task<Guid> SubmitAsync(
        JobSubmitRequest request,
        DateTimeOffset now,
        DbConnection connection,
        DbTransaction? transaction,
        CancellationToken cancellationToken) =>
        SubmitInternalAsync(request, now, connection, transaction, useGate: false, cancellationToken);

    public async Task<IReadOnlyList<ClaimedJob>> ClaimAsync(
        IReadOnlyList<string> queues,
        int maxCount,
        DateTimeOffset now,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken)
    {
        if (maxCount <= 0 || queues.Count == 0)
            return Array.Empty<ClaimedJob>();

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var tx = await SqliteImmediateTransaction.BeginAsync(_connection, _options.UseBeginImmediate, cancellationToken)
                .ConfigureAwait(false);

            var claimed = new List<ClaimedJob>(maxCount);
            var queueParams = string.Join(", ", queues.Select((_, i) => $"$q{i}"));

            for (var n = 0; n < maxCount; n++)
            {
                await using var select = _connection.CreateCommand();
                select.Transaction = tx.Transaction;
                select.CommandText = $"""
                    SELECT JobId, Queue, ContractName, ContractVersion, Payload, State, AttemptCount, MaxAttempts, CancellationRequested, LeaseToken
                    FROM Jobs
                    WHERE Queue IN ({queueParams})
                      AND State IN (0, 1)
                      AND EligibleAt IS NOT NULL
                      AND EligibleAt <= $now
                    ORDER BY EligibleAt, JobId
                    LIMIT 1;
                    """;
                select.Parameters.AddWithValue("$now", now.ToString("O"));
                for (var i = 0; i < queues.Count; i++)
                    select.Parameters.AddWithValue($"$q{i}", queues[i]);

                await using var reader = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    break;

                var jobId = Guid.Parse(reader.GetString(0));
                var state = (JobState)reader.GetInt32(5);
                var attemptCount = reader.GetInt32(6);
                var maxAttempts = reader.GetInt32(7);

                if (state == JobState.Leased && attemptCount >= maxAttempts)
                {
                    await using var dead = _connection.CreateCommand();
                    dead.Transaction = tx.Transaction;
                    dead.CommandText = """
                        UPDATE Jobs
                        SET State = 3, EligibleAt = NULL, CompletedAt = $now,
                            LastError = COALESCE(LastError, 'Max attempts exhausted after lease expiration.')
                        WHERE JobId = $jobId AND State = 1 AND AttemptCount = $attemptCount;
                        """;
                    dead.Parameters.AddWithValue("$jobId", jobId.ToString());
                    dead.Parameters.AddWithValue("$attemptCount", attemptCount);
                    dead.Parameters.AddWithValue("$now", now.ToString("O"));
                    await dead.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var newAttempt = attemptCount + 1;
                var leaseExpiresAt = now + leaseDuration;
                var newLeaseToken = _options.OwnershipFencing == OwnershipFencingMode.LeaseToken
                    ? Guid.NewGuid()
                    : (Guid?)null;

                await using var update = _connection.CreateCommand();
                update.Transaction = tx.Transaction;
                update.CommandText = """
                    UPDATE Jobs
                    SET State = 1,
                        AttemptCount = $newAttempt,
                        EligibleAt = $leaseExpiresAt,
                        LeaseToken = $leaseToken
                    WHERE JobId = $jobId
                      AND State IN (0, 1)
                      AND AttemptCount = $attemptCount
                      AND EligibleAt IS NOT NULL
                      AND EligibleAt <= $now;
                    """;
                update.Parameters.AddWithValue("$jobId", jobId.ToString());
                update.Parameters.AddWithValue("$attemptCount", attemptCount);
                update.Parameters.AddWithValue("$newAttempt", newAttempt);
                update.Parameters.AddWithValue("$leaseExpiresAt", leaseExpiresAt.ToString("O"));
                update.Parameters.AddWithValue("$now", now.ToString("O"));
                update.Parameters.AddWithValue("$leaseToken", newLeaseToken?.ToString() ?? (object)DBNull.Value);

                var rows = await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                if (rows == 0)
                    continue;

                claimed.Add(new ClaimedJob
                {
                    JobId = jobId,
                    Queue = reader.GetString(1),
                    ContractName = reader.GetString(2),
                    ContractVersion = reader.GetInt32(3),
                    Payload = reader.GetString(4),
                    AttemptCount = newAttempt,
                    MaxAttempts = maxAttempts,
                    CancellationRequested = reader.GetInt32(8) == 1,
                    LeaseExpiresAt = leaseExpiresAt,
                    LeaseToken = newLeaseToken,
                });
            }

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return claimed;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> RenewAsync(
        Guid jobId,
        int attemptCount,
        Guid? leaseToken,
        DateTimeOffset leaseExpiresAt,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = """
                UPDATE Jobs
                SET EligibleAt = $leaseExpiresAt
                WHERE JobId = $jobId AND State = 1 AND AttemptCount = $attemptCount
                  AND ($leaseToken IS NULL OR LeaseToken = $leaseToken);
                """;
            cmd.Parameters.AddWithValue("$jobId", jobId.ToString());
            cmd.Parameters.AddWithValue("$attemptCount", attemptCount);
            cmd.Parameters.AddWithValue("$leaseExpiresAt", leaseExpiresAt.ToString("O"));
            cmd.Parameters.AddWithValue("$leaseToken", leaseToken?.ToString() ?? (object)DBNull.Value);
            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> IsCancellationRequestedAsync(
        Guid jobId,
        int attemptCount,
        Guid? leaseToken,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = """
                SELECT CancellationRequested FROM Jobs
                WHERE JobId = $jobId AND State = 1 AND AttemptCount = $attemptCount
                  AND ($leaseToken IS NULL OR LeaseToken = $leaseToken)
                LIMIT 1;
                """;
            cmd.Parameters.AddWithValue("$jobId", jobId.ToString());
            cmd.Parameters.AddWithValue("$attemptCount", attemptCount);
            cmd.Parameters.AddWithValue("$leaseToken", leaseToken?.ToString() ?? (object)DBNull.Value);
            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return result is long l && l == 1;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> SettleAsync(SettleRequest request, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (request.Outcome == SettleOutcome.Succeeded && request.DeleteOnSuccess)
            {
                await using var delete = _connection.CreateCommand();
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
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> CancelReadyAsync(Guid jobId, DateTimeOffset now, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = """
                UPDATE Jobs SET State = 4, EligibleAt = NULL, CompletedAt = $now
                WHERE JobId = $jobId AND State = 0;
                """;
            cmd.Parameters.AddWithValue("$jobId", jobId.ToString());
            cmd.Parameters.AddWithValue("$now", now.ToString("O"));
            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> RequestLeasedCancellationAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = "UPDATE Jobs SET CancellationRequested = 1 WHERE JobId = $jobId AND State = 1;";
            cmd.Parameters.AddWithValue("$jobId", jobId.ToString());
            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<JobRecord?> GetAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = "SELECT * FROM Jobs WHERE JobId = $jobId LIMIT 1;";
            cmd.Parameters.AddWithValue("$jobId", jobId.ToString());
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
                ? ReadJobRecord(reader)
                : null;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<DateTimeOffset?> GetNextEligibleAtAsync(
        IReadOnlyList<string> queues,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        if (queues.Count == 0)
            return null;

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var queueParams = string.Join(", ", queues.Select((_, i) => $"$q{i}"));
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"""
                SELECT MIN(EligibleAt) FROM Jobs
                WHERE Queue IN ({queueParams}) AND State IN (0, 1) AND EligibleAt IS NOT NULL;
                """;
            for (var i = 0; i < queues.Count; i++)
                cmd.Parameters.AddWithValue($"$q{i}", queues[i]);

            var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (value is null or DBNull)
                return null;

            return DateTimeOffset.Parse((string)value, System.Globalization.CultureInfo.InvariantCulture);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<int> DeleteTerminalBatchAsync(
        DateTimeOffset now,
        JobRetentionPurge retention,
        int batchSize,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var successCutoff = (now - retention.SucceededRetention).ToString("O");
            var failedCutoff = (now - retention.FailedRetention).ToString("O");
            var cancelledCutoff = (now - retention.CancelledRetention).ToString("O");

            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = """
                DELETE FROM Jobs WHERE JobId IN (
                    SELECT JobId FROM Jobs
                    WHERE CompletedAt IS NOT NULL AND (
                        (State = 2 AND CompletedAt <= $successCutoff) OR
                        (State = 3 AND CompletedAt <= $failedCutoff) OR
                        (State = 4 AND CompletedAt <= $cancelCutoff))
                    LIMIT $batchSize);
                """;
            cmd.Parameters.AddWithValue("$successCutoff", successCutoff);
            cmd.Parameters.AddWithValue("$failedCutoff", failedCutoff);
            cmd.Parameters.AddWithValue("$cancelCutoff", cancelledCutoff);
            cmd.Parameters.AddWithValue("$batchSize", batchSize);
            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<Guid> SubmitInternalAsync(
        JobSubmitRequest request,
        DateTimeOffset now,
        DbConnection connection,
        DbTransaction? transaction,
        bool useGate,
        CancellationToken cancellationToken)
    {
        if (useGate)
            await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (!string.IsNullOrWhiteSpace(request.IdempotencyKey))
            {
                await using var tx = transaction is null
                    ? await SqliteImmediateTransaction.BeginAsync((SqliteConnection)connection, _options.UseBeginImmediate, cancellationToken).ConfigureAwait(false)
                    : null;
                var activeTx = transaction ?? tx?.Transaction;

                await ClearExpiredIdempotencyKeyAsync(connection, activeTx, request.IdempotencyKey, now, cancellationToken)
                    .ConfigureAwait(false);

                var jobId = Guid.NewGuid();
                await using var insert = connection.CreateCommand();
                insert.Transaction = activeTx;
                insert.CommandText = """
                    INSERT INTO Jobs (
                        JobId, Queue, ContractName, ContractVersion, Payload, State, EligibleAt,
                        AttemptCount, MaxAttempts, CancellationRequested, CreatedAt, CompletedAt, LastError,
                        IdempotencyKey, IdempotencyExpiresAt, LeaseToken,
                        RecurrenceCron, ContinuationQueue, ContinuationContractName, ContinuationContractVersion, ContinuationPayload)
                    VALUES (
                        $jobId, $queue, $contractName, $contractVersion, $payload, $state, $eligibleAt,
                        $attemptCount, $maxAttempts, $cancellationRequested, $createdAt, NULL, NULL,
                        $idempotencyKey, $idempotencyExpiresAt, NULL,
                        $recurrenceCron, $continuationQueue, $continuationContractName, $continuationContractVersion, $continuationPayload)
                    ON CONFLICT(IdempotencyKey) WHERE IdempotencyKey IS NOT NULL AND State IN (0, 1)
                    DO NOTHING
                    RETURNING JobId;
                    """;
                BindSubmit(insert, jobId, request, request.EligibleAt ?? now, request.MaxAttempts ?? 3, now,
                    request.IdempotencyTtl is { } ttl ? now + ttl : null);
                JobStoreDiagnostics.RecordCommand();
                var inserted = await insert.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (inserted is string created && Guid.TryParse(created, out var createdId))
                {
                    if (tx is not null)
                        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
                    return createdId;
                }

                var existing = await ReadActiveIdempotentJobIdAsync(connection, activeTx, request.IdempotencyKey, now, cancellationToken)
                    .ConfigureAwait(false);
                if (tx is not null)
                    await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
                if (existing is null)
                    throw new InvalidOperationException("Idempotent submit failed to resolve an active job id.");
                return existing.Value;
            }

            var newJobId = Guid.NewGuid();
            var eligibleAt = request.EligibleAt ?? now;
            var maxAttempts = request.MaxAttempts ?? 3;
            var expiresAt = request.IdempotencyTtl is { } ttl2 ? now + ttl2 : (DateTimeOffset?)null;

            await using var cmd = connection.CreateCommand();
            if (transaction is not null)
                cmd.Transaction = transaction;

            cmd.CommandText = """
                INSERT INTO Jobs (
                    JobId, Queue, ContractName, ContractVersion, Payload, State, EligibleAt,
                    AttemptCount, MaxAttempts, CancellationRequested, CreatedAt, CompletedAt, LastError,
                    IdempotencyKey, IdempotencyExpiresAt, LeaseToken,
                    RecurrenceCron, ContinuationQueue, ContinuationContractName, ContinuationContractVersion, ContinuationPayload)
                VALUES (
                    $jobId, $queue, $contractName, $contractVersion, $payload, $state, $eligibleAt,
                    $attemptCount, $maxAttempts, $cancellationRequested, $createdAt, NULL, NULL,
                    $idempotencyKey, $idempotencyExpiresAt, NULL,
                    $recurrenceCron, $continuationQueue, $continuationContractName, $continuationContractVersion, $continuationPayload);
                """;
            BindSubmit(cmd, newJobId, request, eligibleAt, maxAttempts, now, expiresAt);
            JobStoreDiagnostics.RecordCommand();
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            return newJobId;
        }
        finally
        {
            if (useGate)
                _gate.Release();
        }
    }

    public static void ApplyPragmas(SqliteConnection connection, SqliteJobStoreOptions options)
    {
        using var pragma = connection.CreateCommand();
        if (options.EnableWal)
        {
            pragma.CommandText = "PRAGMA journal_mode=WAL;";
            pragma.ExecuteNonQuery();
        }

        pragma.CommandText = $"PRAGMA busy_timeout={options.BusyTimeoutMs};";
        pragma.ExecuteNonQuery();
        pragma.CommandText = $"PRAGMA synchronous={options.Synchronous};";
        pragma.ExecuteNonQuery();
        pragma.CommandText = $"PRAGMA cache_size={options.CacheSizeKiB};";
        pragma.ExecuteNonQuery();
        pragma.CommandText = $"PRAGMA mmap_size={options.MmapSizeBytes};";
        pragma.ExecuteNonQuery();
    }

    public static void EnsureSchema(SqliteConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = SqliteSchema.CreateJobsTable;
        cmd.ExecuteNonQuery();
        cmd.CommandText = SqliteSchema.CreateDispatchIndex;
        cmd.ExecuteNonQuery();
        cmd.CommandText = SqliteSchema.CreateIdempotencyIndex;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT COUNT(*) FROM pragma_table_info('Jobs') WHERE name = 'LeaseToken';";
        var hasLeaseToken = Convert.ToInt64(cmd.ExecuteScalar()) > 0;
        if (!hasLeaseToken)
        {
            cmd.CommandText = "ALTER TABLE Jobs ADD COLUMN LeaseToken TEXT NULL;";
            cmd.ExecuteNonQuery();
        }

        EnsureOptionalColumn(connection, "RecurrenceCron", "TEXT NULL");
        EnsureOptionalColumn(connection, "ContinuationQueue", "TEXT NULL");
        EnsureOptionalColumn(connection, "ContinuationContractName", "TEXT NULL");
        EnsureOptionalColumn(connection, "ContinuationContractVersion", "INTEGER NULL");
        EnsureOptionalColumn(connection, "ContinuationPayload", "TEXT NULL");
    }

    private static void EnsureOptionalColumn(SqliteConnection connection, string name, string definition)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM pragma_table_info('Jobs') WHERE name = '{name}';";
        if (Convert.ToInt64(cmd.ExecuteScalar()) > 0)
            return;

        cmd.CommandText = $"ALTER TABLE Jobs ADD COLUMN {name} {definition};";
        cmd.ExecuteNonQuery();
    }

    private static void BindSubmit(
        DbCommand cmd,
        Guid jobId,
        JobSubmitRequest request,
        DateTimeOffset eligibleAt,
        int maxAttempts,
        DateTimeOffset now,
        DateTimeOffset? idempotencyExpiresAt)
    {
        AddParam(cmd, "$jobId", jobId.ToString());
        AddParam(cmd, "$queue", request.Queue);
        AddParam(cmd, "$contractName", request.ContractName);
        AddParam(cmd, "$contractVersion", request.ContractVersion);
        AddParam(cmd, "$payload", request.Payload);
        AddParam(cmd, "$state", (int)JobState.Ready);
        AddParam(cmd, "$eligibleAt", eligibleAt.ToString("O"));
        AddParam(cmd, "$attemptCount", 0);
        AddParam(cmd, "$maxAttempts", maxAttempts);
        AddParam(cmd, "$cancellationRequested", 0);
        AddParam(cmd, "$createdAt", now.ToString("O"));
        AddParam(cmd, "$idempotencyKey", (object?)request.IdempotencyKey ?? DBNull.Value);
        AddParam(
            cmd,
            "$idempotencyExpiresAt",
            idempotencyExpiresAt is null ? DBNull.Value : idempotencyExpiresAt.Value.ToString("O"));
        AddParam(cmd, "$recurrenceCron", (object?)request.RecurrenceCron ?? DBNull.Value);
        AddParam(cmd, "$continuationQueue", (object?)request.ContinuationQueue ?? DBNull.Value);
        AddParam(cmd, "$continuationContractName", (object?)request.ContinuationContractName ?? DBNull.Value);
        AddParam(cmd, "$continuationContractVersion", (object?)request.ContinuationContractVersion ?? DBNull.Value);
        AddParam(cmd, "$continuationPayload", (object?)request.ContinuationPayload ?? DBNull.Value);
    }

    private static void AddParam(DbCommand cmd, string name, object value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }

    private static Task<Guid?> ReadActiveIdempotentJobIdAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string key,
        DateTimeOffset now,
        CancellationToken cancellationToken) =>
        ReadIdempotentJobIdAsync(connection, transaction, key, now, cancellationToken);

    private static async Task ClearExpiredIdempotencyKeyAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string key,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await using var cmd = connection.CreateCommand();
        if (transaction is not null)
            cmd.Transaction = transaction;
        cmd.CommandText = """
            UPDATE Jobs SET IdempotencyKey = NULL, IdempotencyExpiresAt = NULL
            WHERE IdempotencyKey = $key AND State IN (0, 1)
              AND IdempotencyExpiresAt IS NOT NULL AND IdempotencyExpiresAt <= $now;
            """;
        AddParam(cmd, "$key", key);
        AddParam(cmd, "$now", now.ToString("O"));
        JobStoreDiagnostics.RecordCommand();
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<Guid?> ReadIdempotentJobIdAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string key,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await using var cmd = connection.CreateCommand();
        if (transaction is not null)
            cmd.Transaction = transaction;

        cmd.CommandText = """
            SELECT JobId, IdempotencyExpiresAt, State FROM Jobs WHERE IdempotencyKey = $key LIMIT 1;
            """;
        AddParam(cmd, "$key", key);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        var expiresRaw = reader.IsDBNull(1) ? null : reader.GetString(1);
        var state = (JobState)reader.GetInt32(2);
        if (state is not (JobState.Ready or JobState.Leased))
            return null;
        if (expiresRaw is not null && DateTimeOffset.Parse(expiresRaw) <= now)
            return null;

        return Guid.Parse(reader.GetString(0));
    }

    private static JobRecord ReadJobRecord(SqliteDataReader reader) => new()
    {
        JobId = Guid.Parse(reader.GetString(reader.GetOrdinal("JobId"))),
        Queue = reader.GetString(reader.GetOrdinal("Queue")),
        ContractName = reader.GetString(reader.GetOrdinal("ContractName")),
        ContractVersion = reader.GetInt32(reader.GetOrdinal("ContractVersion")),
        Payload = reader.GetString(reader.GetOrdinal("Payload")),
        State = (JobState)reader.GetInt32(reader.GetOrdinal("State")),
        EligibleAt = reader.IsDBNull(reader.GetOrdinal("EligibleAt"))
            ? null
            : DateTimeOffset.Parse(reader.GetString(reader.GetOrdinal("EligibleAt"))),
        AttemptCount = reader.GetInt32(reader.GetOrdinal("AttemptCount")),
        MaxAttempts = reader.GetInt32(reader.GetOrdinal("MaxAttempts")),
        CancellationRequested = reader.GetInt32(reader.GetOrdinal("CancellationRequested")) == 1,
        CreatedAt = DateTimeOffset.Parse(reader.GetString(reader.GetOrdinal("CreatedAt"))),
        CompletedAt = reader.IsDBNull(reader.GetOrdinal("CompletedAt"))
            ? null
            : DateTimeOffset.Parse(reader.GetString(reader.GetOrdinal("CompletedAt"))),
        LastError = reader.IsDBNull(reader.GetOrdinal("LastError"))
            ? null
            : reader.GetString(reader.GetOrdinal("LastError")),
        IdempotencyKey = reader.IsDBNull(reader.GetOrdinal("IdempotencyKey"))
            ? null
            : reader.GetString(reader.GetOrdinal("IdempotencyKey")),
        IdempotencyExpiresAt = reader.IsDBNull(reader.GetOrdinal("IdempotencyExpiresAt"))
            ? null
            : DateTimeOffset.Parse(reader.GetString(reader.GetOrdinal("IdempotencyExpiresAt"))),
        RecurrenceCron = TryGetString(reader, "RecurrenceCron"),
        ContinuationQueue = TryGetString(reader, "ContinuationQueue"),
        ContinuationContractName = TryGetString(reader, "ContinuationContractName"),
        ContinuationContractVersion = TryGetInt(reader, "ContinuationContractVersion"),
        ContinuationPayload = TryGetString(reader, "ContinuationPayload"),
    };

    private static string? TryGetString(SqliteDataReader reader, string column)
    {
        var ordinal = reader.GetOrdinal(column);
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    private static int? TryGetInt(SqliteDataReader reader, string column)
    {
        var ordinal = reader.GetOrdinal(column);
        return reader.IsDBNull(ordinal) ? null : reader.GetInt32(ordinal);
    }

    public ValueTask DisposeAsync()
    {
        _connection.Dispose();
        _gate.Dispose();
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        _connection.Dispose();
        _gate.Dispose();
    }
}
